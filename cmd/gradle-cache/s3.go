package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
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
	"sync"
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
	region      string
	creds       awsCreds
	http        *http.Client
	chunkSize   int64
	dlWorkers   int
	testBaseURL string // non-empty in tests: overrides the virtual-hosted URL prefix and skips signing
}

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

	slog.Debug("s3 client config", "workers", defaultDownloadWorkers, "chunk_mb", defaultDownloadChunkSize>>20)

	return &s3Client{
		region:    region,
		creds:     creds,
		http:      &http.Client{Transport: transport},
		chunkSize: defaultDownloadChunkSize,
		dlWorkers: defaultDownloadWorkers,
	}, nil
}

// s3ObjInfo holds metadata returned by a HEAD request.
type s3ObjInfo struct {
	Size int64
	ETag string // used to pin parallel range-GET requests to one revision
}

// stat returns object metadata if the object exists, or an error otherwise.
// The returned info is passed to get() so it can plan chunk ranges and pin
// all range requests to the same object revision via If-Match.
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
		return s3ObjInfo{}, errors.Errorf("s3 HEAD %s/%s: status %d", bucket, key, resp.StatusCode)
	}
	return s3ObjInfo{
		Size: resp.ContentLength,
		ETag: resp.Header.Get("ETag"),
	}, nil
}

const (
	defaultDownloadChunkSize = 32 << 20
	// defaultDownloadWorkers is the number of concurrent S3 Range GET
	// requests. Benchmarking (see BENCHMARKING.md, phase 4) showed no
	// throughput difference from 4 to 128 workers — extraction IOPS is the
	// bottleneck, not download bandwidth. 8 workers keeps the connection
	// count low on shared CI nodes while staying well above the knee.
	defaultDownloadWorkers = 8
)

// get downloads an object and returns its body as a streaming ReadCloser.
// info must come from a prior stat() call — this avoids an extra HEAD round
// trip. For objects larger than one chunk, parallel Range requests are issued
// and reassembled in order through an io.Pipe so the caller's pipeline
// (pzstd → extractor) runs concurrently with the download. All range requests
// are pinned to the ETag from stat() to prevent reading mixed revisions if
// the key is overwritten during a large download.
// The caller must close the returned reader.
func (c *s3Client) get(ctx context.Context, bucket, key string, info s3ObjInfo) (io.ReadCloser, error) {
	// Small object or unknown size: single-stream GET.
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
			return nil, errors.Errorf("s3 GET %s/%s: status %d: %s", bucket, key, resp.StatusCode, body)
		}
		return resp.Body, nil
	}

	// Large object: parallel range requests, reassembled in order and piped to
	// the caller so download and extraction run concurrently.
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(c.parallelGet(ctx, bucket, key, info, pw))
	}()
	return pr, nil
}

// parallelGet downloads the object in parallel chunks and writes them in order
// to w. Each worker drains its chunk fully into memory so its TCP connection
// stays active at full speed. All workers run concurrently, saturating the
// available S3 bandwidth. Peak memory is numWorkers × downloadChunkSize.
// All range requests are pinned to the given ETag to ensure consistency.
func (c *s3Client) parallelGet(ctx context.Context, bucket, key string, info s3ObjInfo, w io.Writer) error {
	numChunks := int((info.Size + c.chunkSize - 1) / c.chunkSize)
	numWorkers := c.dlWorkers

	type chunkResult struct {
		data []byte
		err  error
	}

	// Pre-allocate a buffered result channel per chunk (capacity 1 so the
	// worker never blocks after draining its body).
	results := make([]chan chunkResult, numChunks)
	for i := range results {
		results[i] = make(chan chunkResult, 1)
	}

	// Work queue: chunk indices.
	work := make(chan int, numChunks)
	for i := range numChunks {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	for range min(numWorkers, numChunks) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range work {
				start := int64(seq) * c.chunkSize
				end := min(start+c.chunkSize-1, info.Size-1)

				req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(bucket, key), nil)
				if err != nil {
					results[seq] <- chunkResult{err: err}
					continue
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
				// Pin to the object revision from stat() to prevent reading a
				// mix of old and new data if the key is overwritten mid-download.
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
				// Drain the body immediately so the TCP connection stays at
				// full speed. All workers do this concurrently, saturating
				// the available bandwidth across all parallel connections.
				data, readErr := io.ReadAll(resp.Body)
				resp.Body.Close() //nolint:errcheck,gosec
				results[seq] <- chunkResult{data: data, err: readErr}
			}
		}()
	}

	// Write chunks to w in order. Each receive blocks until that chunk's
	// worker has drained its body, while other workers continue concurrently.
	var writeErr error
	for _, ch := range results {
		r := <-ch
		if writeErr != nil {
			continue // drain remaining channels so goroutines can exit
		}
		if r.err != nil {
			writeErr = r.err
			continue
		}
		if _, err := w.Write(r.data); err != nil {
			writeErr = err
		}
	}

	wg.Wait()
	return writeErr
}

const (
	// uploadPartSize is the size of each multipart upload part.
	// 64 MiB = at most 160 parts for a 10 GB object, well within the S3
	// 10 000 part limit. Minimum S3 part size is 5 MiB (except last part).
	uploadPartSize = 64 << 20
	// uploadWorkers is the number of concurrent part uploads.
	uploadWorkers = 8
)

// put uploads a seekable file to S3. For objects larger than one part it uses
// parallel multipart upload; smaller objects use a single-part PUT.
// r must implement io.ReadSeeker (e.g. *os.File).
func (c *s3Client) put(ctx context.Context, bucket, key string, r io.ReadSeeker, size int64, contentType string) error {
	if size <= uploadPartSize {
		return c.putSingle(ctx, bucket, key, r, size, contentType)
	}
	return c.putMultipart(ctx, bucket, key, r, size, contentType)
}

// putSingle uploads r as a single PUT request.
func (c *s3Client) putSingle(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(bucket, key), r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	c.sign(req)
	resp, err := c.http.Do(req) //nolint:gosec
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

// putMultipart uploads a large file using parallel S3 multipart upload.
// Parts are uploaded concurrently; the final CompleteMultipartUpload is issued
// only after all parts succeed. The upload is aborted on any error.
func (c *s3Client) putMultipart(ctx context.Context, bucket, key string, r io.ReadSeeker, size int64, contentType string) error {
	uploadID, err := c.createMultipartUpload(ctx, bucket, key, contentType)
	if err != nil {
		return err
	}

	numParts := int((size + uploadPartSize - 1) / uploadPartSize)

	type partResult struct {
		num  int
		etag string
		err  error
	}

	results := make(chan partResult, numParts)
	work := make(chan int, numParts)
	for i := range numParts {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	for range min(uploadWorkers, numParts) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range work {
				partNum := seq + 1 // S3 part numbers are 1-based
				offset := int64(seq) * uploadPartSize
				partSize := min(uploadPartSize, size-offset)

				// Read the part slice from the file at the correct offset.
				// Each goroutine reads a non-overlapping region; io.NewSectionReader
				// is safe for concurrent use on the same *os.File.
				sr := io.NewSectionReader(r.(io.ReaderAt), offset, partSize)
				etag, err := c.uploadPart(ctx, bucket, key, uploadID, partNum, sr, partSize)
				results <- partResult{num: partNum, etag: etag, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	type completedPart struct {
		XMLName    xml.Name `xml:"Part"`
		PartNumber int      `xml:"PartNumber"`
		ETag       string   `xml:"ETag"`
	}
	parts := make([]completedPart, numParts)
	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		if r.err == nil {
			parts[r.num-1] = completedPart{PartNumber: r.num, ETag: r.etag}
		}
	}

	if firstErr != nil {
		c.abortMultipartUpload(ctx, bucket, key, uploadID) //nolint:errcheck
		return firstErr
	}

	return c.completeMultipartUpload(ctx, bucket, key, uploadID, parts)
}

// createMultipartUpload initiates an S3 multipart upload and returns the UploadId.
func (c *s3Client) createMultipartUpload(ctx context.Context, bucket, key, contentType string) (string, error) {
	u := c.objectURL(bucket, key) + "?uploads"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	c.sign(req)
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", errors.Errorf("s3 CreateMultipartUpload %s/%s: status %d: %s", bucket, key, resp.StatusCode, body)
	}
	var result struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", errors.Wrap(err, "decode CreateMultipartUpload response")
	}
	return result.UploadID, nil
}

// uploadPart uploads one part and returns the ETag from the response.
func (c *s3Client) uploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, r io.Reader, size int64) (string, error) {
	u := fmt.Sprintf("%s?partNumber=%d&uploadId=%s", c.objectURL(bucket, key), partNum, url.QueryEscape(uploadID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, r)
	if err != nil {
		return "", err
	}
	req.ContentLength = size
	c.sign(req)
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return "", err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("s3 UploadPart %s/%s part %d: status %d: %s", bucket, key, partNum, resp.StatusCode, body)
	}
	return resp.Header.Get("ETag"), nil
}

// completeMultipartUpload finalises the upload by listing all completed parts.
func (c *s3Client) completeMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts any) error {
	type completeReq struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   any      `xml:"Part"`
	}
	xmlBody, err := xml.Marshal(completeReq{Parts: parts})
	if err != nil {
		return err
	}
	u := fmt.Sprintf("%s?uploadId=%s", c.objectURL(bucket, key), url.QueryEscape(uploadID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(string(xmlBody)))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(xmlBody))
	req.Header.Set("Content-Type", "application/xml")
	c.sign(req)
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("s3 CompleteMultipartUpload %s/%s: status %d: %s", bucket, key, resp.StatusCode, body)
	}
	return nil
}

// abortMultipartUpload cancels an in-progress multipart upload.
func (c *s3Client) abortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	u := fmt.Sprintf("%s?uploadId=%s", c.objectURL(bucket, key), url.QueryEscape(uploadID))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	c.sign(req)
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
	resp.Body.Close()              //nolint:errcheck,gosec
	return nil
}

// putStreamingMultipart uploads data from an io.Reader of unknown size using
// S3 multipart upload. The input is split into uploadPartSize chunks which are
// uploaded in parallel as they become available. This allows the archive
// (tar+zstd) to run concurrently with the upload, overlapping I/O and
// compression with network transfer.
// Returns the total number of bytes uploaded.
func (c *s3Client) putStreamingMultipart(ctx context.Context, bucket, key string, r io.Reader, contentType string) (int64, error) {
	uploadID, err := c.createMultipartUpload(ctx, bucket, key, contentType)
	if err != nil {
		return 0, err
	}

	type partJob struct {
		num  int
		data []byte
	}
	type partResult struct {
		num  int
		size int
		etag string
		err  error
	}

	jobs := make(chan partJob, uploadWorkers)
	results := make(chan partResult, uploadWorkers)

	// Workers upload parts as they arrive.
	var wg sync.WaitGroup
	for range uploadWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				etag, err := c.uploadPart(ctx, bucket, key, uploadID, job.num,
					bytes.NewReader(job.data), int64(len(job.data)))
				results <- partResult{num: job.num, size: len(job.data), etag: etag, err: err}
			}
		}()
	}

	// Reader goroutine: split input into uploadPartSize chunks and dispatch.
	// The buffered reader decouples the upstream pipe (zero-buffer io.Pipe
	// from createTarZstd) from the chunking loop. Without it, when the reader
	// blocks on a full jobs channel the archive goroutine stalls because
	// nobody is consuming the pipe. The 8 MiB buffer absorbs ongoing
	// archive output during those brief stalls.
	br := bufio.NewReaderSize(r, 8<<20)
	go func() {
		partNum := 1
		for {
			buf := make([]byte, uploadPartSize)
			n, err := io.ReadFull(br, buf)
			if n > 0 {
				jobs <- partJob{num: partNum, data: buf[:n]}
				partNum++
			}
			if err != nil { // io.EOF or io.ErrUnexpectedEOF (last partial chunk)
				break
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	type completedPart struct {
		XMLName    xml.Name `xml:"Part"`
		PartNumber int      `xml:"PartNumber"`
		ETag       string   `xml:"ETag"`
	}
	var parts []completedPart
	var totalSize int64
	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		if r.err == nil {
			parts = append(parts, completedPart{PartNumber: r.num, ETag: r.etag})
			totalSize += int64(r.size)
		}
	}

	if firstErr != nil {
		c.abortMultipartUpload(ctx, bucket, key, uploadID) //nolint:errcheck
		return 0, firstErr
	}

	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	if err := c.completeMultipartUpload(ctx, bucket, key, uploadID, parts); err != nil {
		return 0, err
	}

	return totalSize, nil
}

// objectURL returns the virtual-hosted S3 URL for the given bucket and key.
// Each path segment is percent-encoded using the AWS SigV4 URI encoding rules
// (only A-Za-z0-9 - . _ ~ are left unencoded).
// In tests, testBaseURL overrides the scheme+host to point at a local server.
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

// s3PathEscape percent-encodes a single path segment using the AWS SigV4
// URI encoding rules: unreserved characters (A-Za-z0-9 - . _ ~) are left
// as-is; everything else (including colons) is percent-encoded.
// This is stricter than url.PathEscape which also leaves sub-delimiters
// like : ; @ ! unencoded.
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

// canonQueryString builds the AWS SigV4 canonical query string from rawQuery.
// Unlike req.URL.RawQuery (which may omit the "=" for valueless params like
// "?uploads"), this function always emits "key=" for empty values, percent-
// encodes with %XX (not "+"), and sorts parameters alphabetically.
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

// awsQueryEscape percent-encodes s per the AWS SigV4 spec: all characters
// except unreserved (A-Z a-z 0-9 - _ . ~) become %XX with uppercase hex.
// Unlike url.QueryEscape it never uses "+".
func awsQueryEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// sign adds AWS Signature Version 4 headers to req using UNSIGNED-PAYLOAD,
// which is permitted for all requests over HTTPS.
// When testBaseURL is set the request targets a local test server that does not
// verify signatures, so signing is skipped.
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
