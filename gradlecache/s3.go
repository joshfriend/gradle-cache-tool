package gradlecache

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/errgroup"
)

type awsCreds struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type s3Client struct {
	region      string
	creds       awsCreds
	http        *http.Client
	chunkSize   int64
	dlWorkers   int
	testBaseURL string
}

const (
	defaultDownloadChunkSize = 32 << 20
	defaultDownloadWorkers   = 8
)

func newS3Client(region string) (*s3Client, error) {
	creds, err := resolveAWSCredentials(region)
	if err != nil {
		return nil, errors.Wrap(err, "resolve AWS credentials")
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: defaultDownloadWorkers,
		WriteBufferSize:     128 << 10,
		ReadBufferSize:      128 << 10,
	}
	return &s3Client{
		region:    region,
		creds:     creds,
		http:      &http.Client{Transport: transport},
		chunkSize: defaultDownloadChunkSize,
		dlWorkers: defaultDownloadWorkers,
	}, nil
}

type s3ObjInfo struct {
	Size int64
	ETag string
}

func (c *s3Client) stat(ctx context.Context, bucket, key string) (s3ObjInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(bucket, key), nil)
	if err != nil {
		return s3ObjInfo{}, err
	}
	c.sign(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return s3ObjInfo{}, err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
	resp.Body.Close()              //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return s3ObjInfo{}, errors.Errorf("status %d", resp.StatusCode)
	}
	return s3ObjInfo{
		Size: resp.ContentLength,
		ETag: resp.Header.Get("ETag"),
	}, nil
}

func (c *s3Client) get(ctx context.Context, bucket, key string, info s3ObjInfo) (io.ReadCloser, error) {
	if info.Size <= c.chunkSize {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(bucket, key), nil)
		if err != nil {
			return nil, err
		}
		c.sign(req)
		resp, err := c.http.Do(req) //nolint:gosec
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close() //nolint:errcheck,gosec
			return nil, errors.Errorf("s3 GET: status %d: %s", resp.StatusCode, body)
		}
		return resp.Body, nil
	}

	dlCtx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	go func() {
		err := c.parallelGet(dlCtx, bucket, key, info, pw)
		cancel()
		pw.CloseWithError(err)
	}()
	return &cancelReadCloser{ReadCloser: pr, cancel: cancel}, nil
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}

func (c *s3Client) parallelGet(ctx context.Context, bucket, key string, info s3ObjInfo, w io.Writer) error {
	numChunks := int((info.Size + c.chunkSize - 1) / c.chunkSize)
	numWorkers := min(c.dlWorkers, numChunks)

	type chunkResult struct {
		data []byte
		err  error
	}

	results := make([]chan chunkResult, numChunks)
	for i := range results {
		results[i] = make(chan chunkResult, 1)
	}

	work := make(chan int, numChunks)
	for i := range numChunks {
		work <- i
	}
	close(work)

	eg, egCtx := errgroup.WithContext(ctx)
	for range numWorkers {
		eg.Go(func() error {
			for seq := range work {
				if egCtx.Err() != nil {
					results[seq] <- chunkResult{err: egCtx.Err()}
					continue
				}
				start := int64(seq) * c.chunkSize
				end := min(start+c.chunkSize-1, info.Size-1)

				req, err := http.NewRequestWithContext(egCtx, http.MethodGet, c.objectURL(bucket, key), nil)
				if err != nil {
					results[seq] <- chunkResult{err: err}
					continue
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
				if info.ETag != "" {
					req.Header.Set("If-Match", info.ETag)
				}
				c.sign(req)

				resp, err := c.http.Do(req) //nolint:gosec
				if err != nil {
					results[seq] <- chunkResult{err: err}
					continue
				}
				if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
					msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
					resp.Body.Close() //nolint:errcheck,gosec
					results[seq] <- chunkResult{err: errors.Errorf("s3 GET range %d-%d: status %d: %s", start, end, resp.StatusCode, msg)}
					continue
				}
				data, readErr := io.ReadAll(resp.Body)
				resp.Body.Close() //nolint:errcheck,gosec
				results[seq] <- chunkResult{data: data, err: readErr}
			}
			return nil
		})
	}

	var writeErr error
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for _, ch := range results {
			r := <-ch
			if writeErr != nil {
				continue
			}
			if r.err != nil {
				writeErr = r.err
				continue
			}
			if _, err := w.Write(r.data); err != nil {
				writeErr = err
			}
		}
	}()

	egErr := eg.Wait()
	<-writeDone

	if writeErr != nil {
		return writeErr
	}
	return egErr
}

func (c *s3Client) objectURL(bucket, key string) string {
	var sb strings.Builder
	if c.testBaseURL != "" {
		sb.WriteString(strings.TrimRight(c.testBaseURL, "/"))
		sb.WriteByte('/')
		sb.WriteString(bucket)
	} else {
		sb.WriteString("https://")
		sb.WriteString(bucket)
		sb.WriteString(".s3.")
		sb.WriteString(c.region)
		sb.WriteString(".amazonaws.com")
	}
	for _, seg := range strings.Split(key, "/") {
		sb.WriteByte('/')
		sb.WriteString(s3PathEscape(seg))
	}
	return sb.String()
}

func s3PathEscape(s string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			sb.WriteByte(c)
		} else {
			fmt.Fprintf(&sb, "%%%02X", c)
		}
	}
	return sb.String()
}

func canonQueryString(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	params, _ := url.ParseQuery(rawQuery)
	pairs := make([]string, 0, len(params))
	for k, vs := range params {
		ek := awsQueryEscape(k)
		for _, v := range vs {
			pairs = append(pairs, ek+"="+awsQueryEscape(v))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func awsQueryEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func (c *s3Client) sign(req *http.Request) {
	if c.testBaseURL != "" {
		return
	}
	now := time.Now().UTC()
	date := now.Format("20060102")
	datetime := now.Format("20060102T150405Z")

	req.Header.Set("X-Amz-Date", datetime)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	if c.creds.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", c.creds.SessionToken)
	}

	type kv struct{ k, v string }
	hdrs := []kv{{"host", req.URL.Host}}
	for k, vs := range req.Header {
		hdrs = append(hdrs, kv{strings.ToLower(k), strings.TrimSpace(strings.Join(vs, ","))})
	}
	sort.Slice(hdrs, func(i, j int) bool { return hdrs[i].k < hdrs[j].k })

	var canonHdrs, signedNames strings.Builder
	for i, h := range hdrs {
		canonHdrs.WriteString(h.k)
		canonHdrs.WriteByte(':')
		canonHdrs.WriteString(h.v)
		canonHdrs.WriteByte('\n')
		if i > 0 {
			signedNames.WriteByte(';')
		}
		signedNames.WriteString(h.k)
	}

	canonReq := req.Method + "\n" +
		req.URL.EscapedPath() + "\n" +
		canonQueryString(req.URL.RawQuery) + "\n" +
		canonHdrs.String() + "\n" +
		signedNames.String() + "\n" +
		"UNSIGNED-PAYLOAD"

	credScope := date + "/" + c.region + "/s3/aws4_request"
	h := sha256.Sum256([]byte(canonReq))
	stringToSign := "AWS4-HMAC-SHA256\n" + datetime + "\n" + credScope + "\n" + hex.EncodeToString(h[:])

	sigKey := awsSigningKey(c.creds.SecretAccessKey, date, c.region, "s3")
	sig := hex.EncodeToString(s4HMAC(sigKey, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.creds.AccessKeyID, credScope, signedNames.String(), sig,
	))
}

func awsSigningKey(secret, date, region, service string) []byte {
	return s4HMAC(
		s4HMAC(
			s4HMAC(
				s4HMAC([]byte("AWS4"+secret), []byte(date)),
				[]byte(region),
			),
			[]byte(service),
		),
		[]byte("aws4_request"),
	)
}

func s4HMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func resolveAWSCredentials(region string) (awsCreds, error) {
	if tokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"); tokenFile != "" {
		roleARN := os.Getenv("AWS_ROLE_ARN")
		if roleARN == "" {
			return awsCreds{}, errors.New("AWS_WEB_IDENTITY_TOKEN_FILE set but AWS_ROLE_ARN is missing")
		}
		slog.Info("resolving AWS credentials via web identity", "token_file", tokenFile, "role_arn", roleARN)
		return assumeRoleWithWebIdentity(tokenFile, roleARN, region)
	}
	if id := os.Getenv("AWS_ACCESS_KEY_ID"); id != "" {
		slog.Info("resolving AWS credentials via environment variables")
		return awsCreds{
			AccessKeyID:     id,
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		}, nil
	}
	if creds, err := credentialsFromFile(); err == nil {
		slog.Info("resolving AWS credentials via credentials file")
		return creds, nil
	}
	if creds, err := credentialsFromIMDS(); err == nil {
		slog.Info("resolving AWS credentials via IMDS")
		return creds, nil
	}
	return awsCreds{}, errors.New("no AWS credentials found (checked env, ~/.aws/credentials, IMDS)")
}

func credentialsFromFile() (awsCreds, error) {
	credsPath := os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	if credsPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return awsCreds{}, err
		}
		credsPath = filepath.Join(home, ".aws", "credentials")
	}
	f, err := os.Open(credsPath)
	if err != nil {
		return awsCreds{}, err
	}
	defer f.Close() //nolint:errcheck

	profile := os.Getenv("AWS_PROFILE")
	if profile == "" {
		profile = os.Getenv("AWS_DEFAULT_PROFILE")
	}
	if profile == "" {
		profile = "default"
	}

	var inSection bool
	var creds awsCreds
	var credProcess string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '[' {
			inSection = strings.Trim(line, "[]") == profile
			continue
		}
		if !inSection {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "aws_access_key_id":
			creds.AccessKeyID = strings.TrimSpace(v)
		case "aws_secret_access_key":
			creds.SecretAccessKey = strings.TrimSpace(v)
		case "aws_session_token":
			creds.SessionToken = strings.TrimSpace(v)
		case "credential_process":
			credProcess = strings.TrimSpace(v)
		}
	}
	if err := scanner.Err(); err != nil {
		return awsCreds{}, err
	}
	if creds.AccessKeyID != "" {
		return creds, nil
	}
	if credProcess != "" {
		return credentialsFromProcess(credProcess)
	}
	return awsCreds{}, errors.Errorf("profile %q not found in %s", profile, credsPath)
}

func credentialsFromProcess(command string) (awsCreds, error) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return awsCreds{}, errors.New("credential_process is empty")
	}
	//nolint:gosec
	out, err := exec.Command(parts[0], parts[1:]...).Output()
	if err != nil {
		return awsCreds{}, errors.Errorf("credential_process %q: %w", command, err)
	}
	var result struct {
		AccessKeyID     string `json:"AccessKeyId"`
		SecretAccessKey string `json:"SecretAccessKey"`
		SessionToken    string `json:"SessionToken"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		return awsCreds{}, errors.Errorf("credential_process output: %w", err)
	}
	return awsCreds{
		AccessKeyID:     result.AccessKeyID,
		SecretAccessKey: result.SecretAccessKey,
		SessionToken:    result.SessionToken,
	}, nil
}

func credentialsFromIMDS() (awsCreds, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	tokenReq, _ := http.NewRequest(http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	tokenReq.Header.Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
	tokenResp, err := client.Do(tokenReq)
	if err != nil {
		return awsCreds{}, errors.Wrap(err, "IMDS token")
	}
	imdsToken, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body.Close() //nolint:errcheck

	get := func(path string) (string, error) {
		req, _ := http.NewRequest(http.MethodGet, "http://169.254.169.254"+path, nil)
		req.Header.Set("X-Aws-Ec2-Metadata-Token", string(imdsToken))
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close() //nolint:errcheck
		return string(body), nil
	}

	roleStr, err := get("/latest/meta-data/iam/security-credentials/")
	if err != nil {
		return awsCreds{}, errors.Wrap(err, "IMDS role name")
	}
	roleName := strings.TrimSpace(strings.SplitN(roleStr, "\n", 2)[0])

	credsStr, err := get("/latest/meta-data/iam/security-credentials/" + roleName)
	if err != nil {
		return awsCreds{}, errors.Wrap(err, "IMDS credentials")
	}

	var result struct {
		AccessKeyID     string `json:"AccessKeyId"`
		SecretAccessKey string `json:"SecretAccessKey"`
		SessionToken    string `json:"Token"`
	}
	if err := json.Unmarshal([]byte(credsStr), &result); err != nil {
		return awsCreds{}, errors.Wrap(err, "parse IMDS credentials")
	}
	return awsCreds{
		AccessKeyID:     result.AccessKeyID,
		SecretAccessKey: result.SecretAccessKey,
		SessionToken:    result.SessionToken,
	}, nil
}

func assumeRoleWithWebIdentity(tokenFile, roleARN, region string) (awsCreds, error) {
	token, err := os.ReadFile(tokenFile) //nolint:gosec
	if err != nil {
		return awsCreds{}, errors.Wrap(err, "read web identity token")
	}
	params := url.Values{
		"Action":           {"AssumeRoleWithWebIdentity"},
		"Version":          {"2011-06-15"},
		"RoleArn":          {roleARN},
		"WebIdentityToken": {string(token)},
		"RoleSessionName":  {"gradle-cache"},
	}
	resp, err := http.PostForm("https://sts."+region+".amazonaws.com/", params)
	if err != nil {
		return awsCreds{}, errors.Wrap(err, "STS AssumeRoleWithWebIdentity")
	}
	defer resp.Body.Close() //nolint:errcheck
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return awsCreds{}, errors.Errorf("STS returned status %d: %s", resp.StatusCode, body)
	}
	s := string(body)
	creds := awsCreds{
		AccessKeyID:     xmlTagText(s, "AccessKeyId"),
		SecretAccessKey: xmlTagText(s, "SecretAccessKey"),
		SessionToken:    xmlTagText(s, "SessionToken"),
	}
	if creds.AccessKeyID == "" {
		return awsCreds{}, errors.New("STS response missing AccessKeyId")
	}
	return creds, nil
}

func xmlTagText(s, tag string) string {
	open, close := "<"+tag+">", "</"+tag+">"
	i := strings.Index(s, open)
	if i < 0 {
		return ""
	}
	i += len(open)
	j := strings.Index(s[i:], close)
	if j < 0 {
		return ""
	}
	return s[i : i+j]
}
