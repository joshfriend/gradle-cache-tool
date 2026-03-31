package gradlecache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/alecthomas/errors"
)

// ghaCacheStore implements bundleStore on top of the GitHub Actions Cache
// Service v2 Twirp API (available inside every Actions runner via the
// ACTIONS_RESULTS_URL and ACTIONS_RUNTIME_TOKEN environment variables).
//
// The v2 API uses Twirp (JSON over HTTP POST) with three RPCs:
//
//	POST twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL
//	POST twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry
//	POST twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload
//
// Upload/download use signed Azure Blob Storage URLs returned by the API.
// Large uploads are split into parallel Azure Block Blob blocks for throughput.
type ghaCacheStore struct {
	baseURL string // ACTIONS_RESULTS_URL (includes trailing slash)
	token   string // ACTIONS_RUNTIME_TOKEN
	http    *http.Client
}

const (
	// ghaBlockSize is the size of each Azure Block Blob block.
	// 32 MiB × 50 000 blocks = 1.5 TiB max, well above any cache bundle.
	ghaBlockSize = 32 << 20
	// ghaUploadWorkers is the number of concurrent block uploads.
	ghaUploadWorkers = 8
)

func newGHACacheStore() (*ghaCacheStore, error) {
	baseURL := os.Getenv("ACTIONS_RESULTS_URL")
	if baseURL == "" {
		baseURL = os.Getenv("ACTIONS_CACHE_URL")
	}
	token := os.Getenv("ACTIONS_RUNTIME_TOKEN")
	if baseURL == "" || token == "" {
		return nil, errors.New("ACTIONS_RESULTS_URL and ACTIONS_RUNTIME_TOKEN must be set (are you running inside GitHub Actions?)")
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	return &ghaCacheStore{
		baseURL: baseURL,
		token:   token,
		http: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: ghaUploadWorkers,
				WriteBufferSize:     128 << 10,
				ReadBufferSize:      128 << 10,
			},
		},
	}, nil
}

// cacheKey builds the Actions cache key from a commit and bundle cache key.
func ghaCacheKey(commit, cacheKey string) string {
	return fmt.Sprintf("gradle-cache-%s-%s", cacheKey, commit)
}

// cacheVersion is a hash that disambiguates keys. We use a fixed hash of the
// cache key so different cache-key values don't collide even if they share a
// commit.
func ghaCacheVersion(cacheKey string) string {
	h := sha256.Sum256([]byte("bundle-cache:" + cacheKey))
	return fmt.Sprintf("%x", h)
}

const twirpBase = "twirp/github.actions.results.api.v1.CacheService/"

func (g *ghaCacheStore) twirpURL(method string) string {
	return g.baseURL + twirpBase + method
}

func (g *ghaCacheStore) twirpCall(ctx context.Context, method string, reqBody, respBody any) error {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return errors.Wrap(err, "marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.twirpURL(method), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+g.token)

	resp, err := g.http.Do(req)
	if err != nil {
		return errors.Wrap(err, method)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		resp.Body.Close()              //nolint:errcheck,gosec
	}()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return errors.Errorf("%s: status %d: %s", method, resp.StatusCode, msg)
	}

	return json.NewDecoder(resp.Body).Decode(respBody)
}

// ─── Twirp request/response types ───────────────────────────────────────────

type ghaCacheMetadata struct {
	RepositoryID int64           `json:"repository_id,omitempty"`
	Scope        []ghaCacheScope `json:"scope,omitempty"`
}

type ghaCacheScope struct {
	Scope      string `json:"scope"`
	Permission int64  `json:"permission"`
}

type ghaGetDownloadURLReq struct {
	Metadata    ghaCacheMetadata `json:"metadata"`
	Key         string           `json:"key"`
	RestoreKeys []string         `json:"restore_keys,omitempty"`
	Version     string           `json:"version"`
}

type ghaGetDownloadURLResp struct {
	OK                bool   `json:"ok"`
	SignedDownloadURL string `json:"signed_download_url"`
	MatchedKey        string `json:"matched_key"`
}

type ghaCreateEntryReq struct {
	Metadata ghaCacheMetadata `json:"metadata"`
	Key      string           `json:"key"`
	Version  string           `json:"version"`
}

type ghaCreateEntryResp struct {
	OK              bool   `json:"ok"`
	SignedUploadURL string `json:"signed_upload_url"`
}

type ghaFinalizeReq struct {
	Metadata  ghaCacheMetadata `json:"metadata"`
	Key       string           `json:"key"`
	SizeBytes int64            `json:"size_bytes"`
	Version   string           `json:"version"`
}

type ghaFinalizeResp struct {
	OK      bool            `json:"ok"`
	EntryID json.RawMessage `json:"entry_id"`
}

// ─── bundleStore implementation ─────────────────────────────────────────────

func (g *ghaCacheStore) metadata() ghaCacheMetadata {
	// The runner provides scope info, but for simplicity we send an empty
	// metadata block — the server infers scope from the token.
	return ghaCacheMetadata{}
}

// stat checks whether a cache entry exists.
func (g *ghaCacheStore) stat(ctx context.Context, commit, cacheKey string) (bundleStatInfo, error) {
	key := ghaCacheKey(commit, cacheKey)
	version := ghaCacheVersion(cacheKey)

	var resp ghaGetDownloadURLResp
	err := g.twirpCall(ctx, "GetCacheEntryDownloadURL", ghaGetDownloadURLReq{
		Metadata: g.metadata(),
		Key:      key,
		Version:  version,
	}, &resp)
	if err != nil {
		return bundleStatInfo{}, errors.Errorf("gha cache: not found for %.8s: %w", commit, err)
	}
	if !resp.OK || resp.SignedDownloadURL == "" {
		return bundleStatInfo{}, errors.Errorf("gha cache: not found for %.8s", commit)
	}
	return bundleStatInfo{}, nil
}

// get downloads the cache entry.
func (g *ghaCacheStore) get(ctx context.Context, commit, cacheKey string, _ bundleStatInfo) (io.ReadCloser, error) {
	key := ghaCacheKey(commit, cacheKey)
	version := ghaCacheVersion(cacheKey)

	var resp ghaGetDownloadURLResp
	err := g.twirpCall(ctx, "GetCacheEntryDownloadURL", ghaGetDownloadURLReq{
		Metadata: g.metadata(),
		Key:      key,
		Version:  version,
	}, &resp)
	if err != nil {
		return nil, errors.Wrap(err, "gha cache lookup")
	}
	if !resp.OK || resp.SignedDownloadURL == "" {
		return nil, errors.Errorf("gha cache: no download URL for %.8s", commit)
	}

	dlReq, err := http.NewRequestWithContext(ctx, http.MethodGet, resp.SignedDownloadURL, nil)
	if err != nil {
		return nil, err
	}
	dlResp, err := g.http.Do(dlReq)
	if err != nil {
		return nil, errors.Wrap(err, "download from signed URL")
	}
	if dlResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(dlResp.Body, 4096))
		dlResp.Body.Close() //nolint:errcheck,gosec
		return nil, errors.Errorf("gha cache download: status %d: %s", dlResp.StatusCode, body)
	}
	return dlResp.Body, nil
}

// createAndFinalize handles the Twirp API calls to create an entry, run the
// upload function, and finalize.
func (g *ghaCacheStore) createAndFinalize(ctx context.Context, commit, cacheKey string, size int64, uploadFn func(signedURL string) error) error {
	key := ghaCacheKey(commit, cacheKey)
	version := ghaCacheVersion(cacheKey)

	// 1. Create cache entry → get signed upload URL
	var createResp ghaCreateEntryResp
	if err := g.twirpCall(ctx, "CreateCacheEntry", ghaCreateEntryReq{
		Metadata: g.metadata(),
		Key:      key,
		Version:  version,
	}, &createResp); err != nil {
		return errors.Wrap(err, "gha cache create")
	}
	if !createResp.OK || createResp.SignedUploadURL == "" {
		return errors.Errorf("gha cache: create entry returned ok=%v", createResp.OK)
	}

	// 2. Upload
	if err := uploadFn(createResp.SignedUploadURL); err != nil {
		return err
	}

	// 3. Finalize
	var finalResp ghaFinalizeResp
	if err := g.twirpCall(ctx, "FinalizeCacheEntryUpload", ghaFinalizeReq{
		Metadata:  g.metadata(),
		Key:       key,
		SizeBytes: size,
		Version:   version,
	}, &finalResp); err != nil {
		return errors.Wrap(err, "gha cache finalize")
	}
	if !finalResp.OK {
		return errors.Errorf("gha cache: finalize returned ok=false")
	}

	slog.Debug("cache entry finalized", "entryId", string(finalResp.EntryID))
	return nil
}

// put uploads a cache entry from a ReadSeeker of known size.
// For small bundles (≤ 1 block), uses a single PUT. For larger bundles,
// uses parallel Azure Block Blob upload (Put Block + Put Block List).
func (g *ghaCacheStore) put(ctx context.Context, commit, cacheKey string, r io.ReadSeeker, size int64) error {
	return g.createAndFinalize(ctx, commit, cacheKey, size, func(signedURL string) error {
		if size <= ghaBlockSize {
			return g.azurePutSingle(ctx, signedURL, r, size)
		}
		return g.azurePutParallel(ctx, signedURL, r.(io.ReaderAt), size)
	})
}

// putStream uploads from an io.Reader of unknown size, streaming blocks in
// parallel as data arrives (like S3 putStreamingMultipart). The archive
// pipeline runs concurrently with the upload.
func (g *ghaCacheStore) putStream(ctx context.Context, commit, cacheKey string, r io.Reader) (int64, error) {
	// We need the signed URL before we can start uploading blocks, so first
	// create the cache entry.
	key := ghaCacheKey(commit, cacheKey)
	version := ghaCacheVersion(cacheKey)

	var createResp ghaCreateEntryResp
	if err := g.twirpCall(ctx, "CreateCacheEntry", ghaCreateEntryReq{
		Metadata: g.metadata(),
		Key:      key,
		Version:  version,
	}, &createResp); err != nil {
		return 0, errors.Wrap(err, "gha cache create")
	}
	if !createResp.OK || createResp.SignedUploadURL == "" {
		return 0, errors.Errorf("gha cache: create entry returned ok=%v", createResp.OK)
	}

	// Stream blocks in parallel.
	totalSize, blockIDs, err := g.azurePutBlocksStreaming(ctx, createResp.SignedUploadURL, r)
	if err != nil {
		return totalSize, err
	}

	// Commit the block list.
	if err := g.azureCommitBlockList(ctx, createResp.SignedUploadURL, blockIDs); err != nil {
		return totalSize, err
	}

	// Finalize with the cache service.
	var finalResp ghaFinalizeResp
	if err := g.twirpCall(ctx, "FinalizeCacheEntryUpload", ghaFinalizeReq{
		Metadata:  g.metadata(),
		Key:       key,
		SizeBytes: totalSize,
		Version:   version,
	}, &finalResp); err != nil {
		return totalSize, errors.Wrap(err, "gha cache finalize")
	}
	if !finalResp.OK {
		return totalSize, errors.Errorf("gha cache: finalize returned ok=false")
	}

	slog.Debug("cache entry finalized", "entryId", string(finalResp.EntryID))
	return totalSize, nil
}

// ─── Azure Blob Storage helpers ─────────────────────────────────────────────

// azureBlockID returns a base64-encoded block ID for the given sequence number.
// Azure requires all block IDs in a blob to be the same length, so we zero-pad.
func azureBlockID(seq int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("block%06d", seq)))
}

// azurePutSingle uploads the entire body as a single BlockBlob PUT.
func (g *ghaCacheStore) azurePutSingle(ctx context.Context, signedURL string, r io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-ms-blob-type", "BlockBlob")

	resp, err := g.http.Do(req)
	if err != nil {
		return errors.Wrap(err, "azure put single")
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
	resp.Body.Close()              //nolint:errcheck,gosec

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.Errorf("azure put single: status %d", resp.StatusCode)
	}
	return nil
}

// azurePutParallel uploads a seekable file in parallel blocks, then commits.
func (g *ghaCacheStore) azurePutParallel(ctx context.Context, signedURL string, r io.ReaderAt, size int64) error {
	numBlocks := int((size + ghaBlockSize - 1) / ghaBlockSize)
	blockIDs := make([]string, numBlocks)

	type blockResult struct {
		seq int
		err error
	}

	results := make(chan blockResult, numBlocks)
	work := make(chan int, numBlocks)
	for i := range numBlocks {
		work <- i
		blockIDs[i] = azureBlockID(i)
	}
	close(work)

	var wg sync.WaitGroup
	for range min(ghaUploadWorkers, numBlocks) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range work {
				offset := int64(seq) * ghaBlockSize
				blockSize := min(ghaBlockSize, size-offset)
				sr := io.NewSectionReader(r, offset, blockSize)
				err := g.azurePutBlock(ctx, signedURL, blockIDs[seq], sr, blockSize)
				results <- blockResult{seq: seq, err: err}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
	}
	if firstErr != nil {
		return firstErr
	}

	return g.azureCommitBlockList(ctx, signedURL, blockIDs)
}

// azurePutBlocksStreaming reads from r in ghaBlockSize chunks and uploads
// blocks in parallel as they arrive. Returns total bytes and the ordered
// block IDs for the commit call.
func (g *ghaCacheStore) azurePutBlocksStreaming(ctx context.Context, signedURL string, r io.Reader) (int64, []string, error) {
	type blockJob struct {
		seq  int
		id   string
		data []byte
	}
	type blockResult struct {
		seq  int
		size int
		err  error
	}

	jobs := make(chan blockJob, ghaUploadWorkers)
	results := make(chan blockResult, ghaUploadWorkers)

	var wg sync.WaitGroup
	for range ghaUploadWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := g.azurePutBlock(ctx, signedURL, job.id, bytes.NewReader(job.data), int64(len(job.data)))
				results <- blockResult{seq: job.seq, size: len(job.data), err: err}
			}
		}()
	}

	// Reader goroutine: chunk the input and dispatch.
	go func() {
		seq := 0
		for {
			buf := make([]byte, ghaBlockSize)
			n, err := io.ReadFull(r, buf)
			if n > 0 {
				jobs <- blockJob{seq: seq, id: azureBlockID(seq), data: buf[:n]}
				seq++
			}
			if err != nil { // io.EOF or io.ErrUnexpectedEOF (last partial chunk)
				break
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var blockIDs []string
	var totalSize int64
	var firstErr error
	type idEntry struct {
		seq int
		id  string
	}
	var ids []idEntry

	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		if r.err == nil {
			totalSize += int64(r.size)
			ids = append(ids, idEntry{seq: r.seq, id: azureBlockID(r.seq)})
		}
	}
	if firstErr != nil {
		return totalSize, nil, firstErr
	}

	// Sort by sequence to maintain block order.
	sort.Slice(ids, func(i, j int) bool { return ids[i].seq < ids[j].seq })
	for _, e := range ids {
		blockIDs = append(blockIDs, e.id)
	}

	return totalSize, blockIDs, nil
}

// azurePutBlock uploads a single block to Azure Blob Storage.
func (g *ghaCacheStore) azurePutBlock(ctx context.Context, signedURL, blockID string, r io.Reader, size int64) error {
	// Append comp=block&blockid=<id> to the signed URL.
	sep := "&"
	if !strings.Contains(signedURL, "?") {
		sep = "?"
	}
	blockURL := signedURL + sep + "comp=block&blockid=" + blockID

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, blockURL, r)
	if err != nil {
		return err
	}
	req.ContentLength = size

	resp, err := g.http.Do(req)
	if err != nil {
		return errors.Wrap(err, "azure put block")
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
	resp.Body.Close()              //nolint:errcheck,gosec

	if resp.StatusCode != http.StatusCreated {
		return errors.Errorf("azure put block: status %d", resp.StatusCode)
	}
	return nil
}

// azureCommitBlockList commits the uploaded blocks into the final blob.
func (g *ghaCacheStore) azureCommitBlockList(ctx context.Context, signedURL string, blockIDs []string) error {
	type blockEntry struct {
		XMLName xml.Name `xml:"Latest"`
		ID      string   `xml:",chardata"`
	}
	type blockListXML struct {
		XMLName xml.Name     `xml:"BlockList"`
		Blocks  []blockEntry `xml:"Latest"`
	}

	bl := blockListXML{}
	for _, id := range blockIDs {
		bl.Blocks = append(bl.Blocks, blockEntry{ID: id})
	}
	xmlBody, err := xml.Marshal(bl)
	if err != nil {
		return errors.Wrap(err, "marshal block list")
	}

	sep := "&"
	if !strings.Contains(signedURL, "?") {
		sep = "?"
	}
	commitURL := signedURL + sep + "comp=blocklist"

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, commitURL, bytes.NewReader(xmlBody))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(xmlBody))
	req.Header.Set("Content-Type", "application/xml")

	resp, err := g.http.Do(req)
	if err != nil {
		return errors.Wrap(err, "azure commit block list")
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close() //nolint:errcheck,gosec

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return errors.Errorf("azure commit block list: status %d: %s", resp.StatusCode, body)
	}
	return nil
}
