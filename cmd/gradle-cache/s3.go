package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/errors"
)

// awsCreds holds AWS credentials (permanent or temporary session credentials).
type awsCreds struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string // empty for non-temporary credentials
}

// s3Client is a minimal AWS S3 client supporting HeadObject, GetObject, and PutObject.
type s3Client struct {
	region string
	creds  awsCreds
	http   *http.Client
}

func newS3Client(region string) (*s3Client, error) {
	creds, err := resolveAWSCredentials(region)
	if err != nil {
		return nil, errors.Wrap(err, "resolve AWS credentials")
	}
	return &s3Client{
		region: region,
		creds:  creds,
		http:   &http.Client{},
	}, nil
}

// stat returns nil if the object exists, or an error otherwise.
func (c *s3Client) stat(ctx context.Context, bucket, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(bucket, key), nil)
	if err != nil {
		return err
	}
	c.sign(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
	resp.Body.Close()              //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("s3 HEAD %s/%s: status %d", bucket, key, resp.StatusCode)
	}
	return nil
}

// get downloads an object and returns its body as a ReadCloser plus the
// Content-Length (-1 if unknown).
func (c *s3Client) get(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(bucket, key), nil)
	if err != nil {
		return nil, 0, err
	}
	c.sign(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close() //nolint:errcheck,gosec
		return nil, 0, errors.Errorf("s3 GET %s/%s: status %d: %s", bucket, key, resp.StatusCode, body)
	}
	return resp.Body, resp.ContentLength, nil
}

// put uploads r to S3 with a known content length.
func (c *s3Client) put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(bucket, key), r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	c.sign(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("s3 PUT %s/%s: status %d: %s", bucket, key, resp.StatusCode, body)
	}
	return nil
}

// objectURL returns the virtual-hosted S3 URL for the given bucket and key.
// Each path segment of the key is percent-encoded per RFC 3986.
func (c *s3Client) objectURL(bucket, key string) string {
	var sb strings.Builder
	sb.WriteString("https://")
	sb.WriteString(bucket)
	sb.WriteString(".s3.")
	sb.WriteString(c.region)
	sb.WriteString(".amazonaws.com")
	for _, seg := range strings.Split(key, "/") {
		sb.WriteByte('/')
		sb.WriteString(url.PathEscape(seg))
	}
	return sb.String()
}

// sign adds AWS Signature Version 4 headers to req using UNSIGNED-PAYLOAD,
// which is permitted for all requests over HTTPS.
func (c *s3Client) sign(req *http.Request) {
	now := time.Now().UTC()
	date := now.Format("20060102")
	datetime := now.Format("20060102T150405Z")

	req.Header.Set("X-Amz-Date", datetime)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	if c.creds.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", c.creds.SessionToken)
	}

	// Build canonical headers: host plus everything in req.Header, sorted by lowercase name.
	type kv struct{ k, v string }
	hdrs := []kv{{"host", req.URL.Host}} // host is not in req.Header; handle it explicitly
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
		req.URL.RawQuery + "\n" +
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

// resolveAWSCredentials tries each credential source in order:
// IRSA web identity → environment variables → credentials file → IMDSv2.
func resolveAWSCredentials(region string) (awsCreds, error) {
	if tokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"); tokenFile != "" {
		roleARN := os.Getenv("AWS_ROLE_ARN")
		if roleARN == "" {
			return awsCreds{}, errors.New("AWS_WEB_IDENTITY_TOKEN_FILE set but AWS_ROLE_ARN is missing")
		}
		return assumeRoleWithWebIdentity(tokenFile, roleARN, region)
	}
	if id := os.Getenv("AWS_ACCESS_KEY_ID"); id != "" {
		return awsCreds{
			AccessKeyID:     id,
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		}, nil
	}
	if creds, err := credentialsFromFile(); err == nil {
		return creds, nil
	}
	if creds, err := credentialsFromIMDS(); err == nil {
		return creds, nil
	}
	return awsCreds{}, errors.New("no AWS credentials found (checked env, ~/.aws/credentials, IMDS)")
}

// credentialsFromFile reads credentials from the AWS credentials file.
// The path defaults to ~/.aws/credentials but can be overridden with
// AWS_SHARED_CREDENTIALS_FILE. The profile is selected by AWS_PROFILE /
// AWS_DEFAULT_PROFILE (default: "default").
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
	// Fall back to credential_process if no static credentials were found.
	if credProcess != "" {
		return credentialsFromProcess(credProcess)
	}
	return awsCreds{}, errors.Errorf("profile %q not found in %s", profile, credsPath)
}

// credentialsFromProcess executes a credential_process command and parses the
// JSON output into awsCreds. The output format is the standard AWS SDK
// credential process protocol (Version 1).
func credentialsFromProcess(command string) (awsCreds, error) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return awsCreds{}, errors.New("credential_process is empty")
	}
	//nolint:gosec // command comes from the user's own credentials file
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

// credentialsFromIMDS fetches temporary credentials from the EC2 Instance
// Metadata Service using the IMDSv2 token-based protocol.
func credentialsFromIMDS() (awsCreds, error) {
	client := &http.Client{Timeout: 2 * time.Second}

	// Acquire an IMDSv2 session token.
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

// assumeRoleWithWebIdentity exchanges a Kubernetes IRSA web identity token for
// temporary AWS credentials via STS AssumeRoleWithWebIdentity.
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

// xmlTagText extracts the text content of the first matching XML element.
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
