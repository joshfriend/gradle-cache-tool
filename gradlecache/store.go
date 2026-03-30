package gradlecache

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/alecthomas/errors"
	crc64nvme "github.com/minio/crc64nvme"
)

// bundleStatInfo holds opaque metadata returned by bundleStore.stat().
type bundleStatInfo struct {
	Size int64
	etag string
}

// bundleStore abstracts over S3 and cachew as storage backends.
type bundleStore interface {
	stat(ctx context.Context, commit, cacheKey string) (bundleStatInfo, error)
	get(ctx context.Context, commit, cacheKey string, info bundleStatInfo) (io.ReadCloser, error)
	put(ctx context.Context, commit, cacheKey string, r io.ReadSeeker, size int64) error
	putStream(ctx context.Context, commit, cacheKey string, r io.Reader) (int64, error)
}

func newStore(bucket, region, cachewURL string) (bundleStore, error) {
	if cachewURL != "" {
		return newCachewClient(cachewURL), nil
	}
	// Inside GitHub Actions, use the Actions Cache Service when no
	// S3 bucket or cachew URL is configured.
	if bucket == "" {
		return newGHACacheStore()
	}
	client, err := newS3Client(region)
	if err != nil {
		return nil, err
	}
	return &s3BundleStore{client: client, bucket: bucket}, nil
}

// ── S3 bundle store ─────────────────────────────────────────────────────────

type s3BundleStore struct {
	client *s3Client
	bucket string
}

func (s *s3BundleStore) stat(ctx context.Context, commit, cacheKey string) (bundleStatInfo, error) {
	obj, err := s.client.stat(ctx, s.bucket, s3Key(commit, cacheKey, bundleFilename(cacheKey)))
	if err != nil {
		return bundleStatInfo{}, err
	}
	return bundleStatInfo{Size: obj.Size, etag: obj.ETag}, nil
}

func (s *s3BundleStore) get(ctx context.Context, commit, cacheKey string, info bundleStatInfo) (io.ReadCloser, error) {
	return s.client.get(ctx, s.bucket, s3Key(commit, cacheKey, bundleFilename(cacheKey)), s3ObjInfo{Size: info.Size, ETag: info.etag})
}

func (s *s3BundleStore) put(ctx context.Context, commit, cacheKey string, r io.ReadSeeker, size int64) error {
	return s.client.put(ctx, s.bucket, s3Key(commit, cacheKey, bundleFilename(cacheKey)), r, size, "application/zstd")
}

func (s *s3BundleStore) putStream(ctx context.Context, commit, cacheKey string, r io.Reader) (int64, error) {
	return s.client.putStreamingMultipart(ctx, s.bucket, s3Key(commit, cacheKey, bundleFilename(cacheKey)), r, "application/zstd")
}

func bundleFilename(cacheKey string) string {
	return strings.ReplaceAll(cacheKey, ":", "-") + ".tar.zst"
}

func s3Key(commit, cacheKey, bundleFile string) string {
	return commit + "/" + cacheKey + "/" + bundleFile
}

// ── Cachew client ───────────────────────────────────────────────────────────

type cachewClient struct {
	baseURL string
	http    *http.Client
}

func newCachewClient(baseURL string) *cachewClient {
	return &cachewClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{},
	}
}

func (c *cachewClient) objectURL(commit, cacheKey string) string {
	return fmt.Sprintf("%s/api/v1/object/%s/%s",
		c.baseURL,
		url.PathEscape(cacheKey),
		url.PathEscape(commit),
	)
}

func (c *cachewClient) stat(ctx context.Context, commit, cacheKey string) (bundleStatInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(commit, cacheKey), nil)
	if err != nil {
		return bundleStatInfo{}, err
	}
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return bundleStatInfo{}, err
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
	resp.Body.Close()              //nolint:errcheck,gosec
	if resp.StatusCode == http.StatusNotFound {
		return bundleStatInfo{}, errors.Errorf("cachew: not found for %.8s", commit)
	}
	if resp.StatusCode != http.StatusOK {
		return bundleStatInfo{}, errors.Errorf("cachew HEAD %.8s: status %d", commit, resp.StatusCode)
	}
	return bundleStatInfo{}, nil
}

func (c *cachewClient) get(ctx context.Context, commit, cacheKey string, _ bundleStatInfo) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(commit, cacheKey), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close() //nolint:errcheck,gosec
		return nil, errors.Errorf("cachew GET %.8s: status %d: %s", commit, resp.StatusCode, msg)
	}
	return resp.Body, nil
}

func (c *cachewClient) put(ctx context.Context, commit, cacheKey string, r io.ReadSeeker, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.objectURL(commit, cacheKey), r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/zstd")
	req.Header.Set("Time-To-Live", "168h")
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return err
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("cachew POST %.8s: status %d: %s", commit, resp.StatusCode, msg)
	}
	return nil
}

func (c *cachewClient) putStream(ctx context.Context, commit, cacheKey string, r io.Reader) (int64, error) {
	cr := &putCountingReader{r: r}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.objectURL(commit, cacheKey), cr)
	if err != nil {
		return 0, err
	}
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/zstd")
	req.Header.Set("Time-To-Live", "168h")
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return cr.n, err
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return cr.n, errors.Errorf("cachew POST %.8s: status %d: %s", commit, resp.StatusCode, msg)
	}
	return cr.n, nil
}

type putCountingReader struct {
	r io.Reader
	n int64
}

func (c *putCountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// ── S3 upload helpers ───────────────────────────────────────────────────────

const (
	uploadPartSize = 64 << 20
	uploadWorkers  = 8
)

func (c *s3Client) put(ctx context.Context, bucket, key string, r io.ReadSeeker, size int64, contentType string) error {
	if size <= uploadPartSize {
		return c.putSingle(ctx, bucket, key, r, size, contentType)
	}
	return c.putMultipart(ctx, bucket, key, r, size, contentType)
}

// crc64Of computes a CRC64-NVME checksum of the data from r, then seeks back.
func crc64Of(r io.ReadSeeker) (string, error) {
	h := crc64nvme.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return crc64Base64(h), nil
}

// crc64Base64 returns the base64-encoded 8-byte big-endian CRC64-NVME value.
func crc64Base64(h hash.Hash64) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h.Sum64())
	return base64.StdEncoding.EncodeToString(buf[:])
}

func (c *s3Client) putSingle(ctx context.Context, bucket, key string, r io.ReadSeeker, size int64, contentType string) error {
	checksum, err := crc64Of(r)
	if err != nil {
		return errors.Wrap(err, "compute CRC64-NVME checksum")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(bucket, key), r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("X-Amz-Checksum-Crc64nvme", checksum)
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC64NVME")
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

func (c *s3Client) putMultipart(ctx context.Context, bucket, key string, r io.ReadSeeker, size int64, contentType string) error {
	uploadID, err := c.createMultipartUpload(ctx, bucket, key, contentType)
	if err != nil {
		return err
	}

	numParts := int((size + uploadPartSize - 1) / uploadPartSize)

	type partResultMsg struct {
		num      int
		etag     string
		checksum string
		err      error
	}

	results := make(chan partResultMsg, numParts)
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
				partNum := seq + 1
				offset := int64(seq) * uploadPartSize
				partSize := min(uploadPartSize, size-offset)
				sr := io.NewSectionReader(r.(io.ReaderAt), offset, partSize)
				res, err := c.uploadPart(ctx, bucket, key, uploadID, partNum, sr, partSize)
				results <- partResultMsg{num: partNum, etag: res.etag, checksum: res.checksum, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	type completedPart struct {
		XMLName           xml.Name `xml:"Part"`
		PartNumber        int      `xml:"PartNumber"`
		ETag              string   `xml:"ETag"`
		ChecksumCRC64NVME string   `xml:"ChecksumCRC64NVME,omitempty"`
	}
	parts := make([]completedPart, numParts)
	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		if r.err == nil {
			parts[r.num-1] = completedPart{PartNumber: r.num, ETag: r.etag, ChecksumCRC64NVME: r.checksum}
		}
	}

	if firstErr != nil {
		c.abortMultipartUpload(ctx, bucket, key, uploadID) //nolint:errcheck
		return firstErr
	}

	return c.completeMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func (c *s3Client) createMultipartUpload(ctx context.Context, bucket, key, contentType string) (string, error) {
	u := c.objectURL(bucket, key) + "?uploads"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("X-Amz-Checksum-Algorithm", "CRC64NVME")
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

// uploadPartResult holds both the ETag and CRC64 checksum for a completed part.
type uploadPartResult struct {
	etag     string
	checksum string // base64-encoded CRC64-NVME
}

func (c *s3Client) uploadPart(ctx context.Context, bucket, key, uploadID string, partNum int, r io.ReadSeeker, size int64) (uploadPartResult, error) {
	h := crc64nvme.New()
	if _, err := io.Copy(h, r); err != nil {
		return uploadPartResult{}, err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return uploadPartResult{}, err
	}
	checksum := crc64Base64(h)

	u := fmt.Sprintf("%s?partNumber=%d&uploadId=%s", c.objectURL(bucket, key), partNum, url.QueryEscape(uploadID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, r)
	if err != nil {
		return uploadPartResult{}, err
	}
	req.ContentLength = size
	req.Header.Set("X-Amz-Checksum-Crc64nvme", checksum)
	c.sign(req)
	resp, err := c.http.Do(req) //nolint:gosec
	if err != nil {
		return uploadPartResult{}, err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec
	if resp.StatusCode != http.StatusOK {
		return uploadPartResult{}, errors.Errorf("s3 UploadPart %s/%s part %d: status %d: %s", bucket, key, partNum, resp.StatusCode, body)
	}
	return uploadPartResult{etag: resp.Header.Get("ETag"), checksum: checksum}, nil
}

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

func (c *s3Client) putStreamingMultipart(ctx context.Context, bucket, key string, r io.Reader, contentType string) (int64, error) {
	uploadID, err := c.createMultipartUpload(ctx, bucket, key, contentType)
	if err != nil {
		return 0, err
	}

	type partJob struct {
		num  int
		data []byte
	}
	type streamPartResult struct {
		num      int
		size     int
		etag     string
		checksum string
		err      error
	}

	jobs := make(chan partJob, uploadWorkers)
	results := make(chan streamPartResult, uploadWorkers)

	var wg sync.WaitGroup
	for range uploadWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				res, err := c.uploadPart(ctx, bucket, key, uploadID, job.num,
					bytes.NewReader(job.data), int64(len(job.data)))
				results <- streamPartResult{num: job.num, size: len(job.data), etag: res.etag, checksum: res.checksum, err: err}
			}
		}()
	}

	br := io.Reader(r)
	go func() {
		partNum := 1
		for {
			buf := make([]byte, uploadPartSize)
			n, err := io.ReadFull(br, buf)
			if n > 0 {
				jobs <- partJob{num: partNum, data: buf[:n]}
				partNum++
			}
			if err != nil {
				break
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	type completedPart struct {
		XMLName           xml.Name `xml:"Part"`
		PartNumber        int      `xml:"PartNumber"`
		ETag              string   `xml:"ETag"`
		ChecksumCRC64NVME string   `xml:"ChecksumCRC64NVME,omitempty"`
	}
	var parts []completedPart
	var totalSize int64
	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		if r.err == nil {
			parts = append(parts, completedPart{PartNumber: r.num, ETag: r.etag, ChecksumCRC64NVME: r.checksum})
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
