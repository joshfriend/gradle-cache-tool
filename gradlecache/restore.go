// Package gradlecache provides a Go library for restoring Gradle build cache
// bundles from S3 or cachew. It wraps the same logic used by the gradle-cache
// CLI in an importable API.
package gradlecache

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"github.com/klauspost/compress/zstd"
)

const (
	zstdFrameMagic = uint32(0xFD2FB528) // standard single-frame zstd
	pzstdMagicMin  = uint32(0x184D2A50) // pzstd skippable-frame magic range
	pzstdMagicMax  = uint32(0x184D2A5F)
)

func peekMagic(br *bufio.Reader) (uint32, error) {
	b, err := br.Peek(4)
	if err != nil {
		return 0, errors.Wrap(err, "peek stream magic")
	}
	return binary.LittleEndian.Uint32(b), nil
}

// skipPzstdSkippableFrame reads and discards the pzstd skippable header frame.
// On entry r is positioned at the 4-byte magic (not yet consumed).
func skipPzstdSkippableFrame(r io.Reader) error {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return errors.Wrap(err, "read pzstd skippable frame header")
	}
	size := binary.LittleEndian.Uint32(hdr[4:8])
	_, err := io.CopyN(io.Discard, r, int64(size))
	return errors.Wrap(err, "skip pzstd skippable frame content")
}

// readZstdFrame reads exactly one complete zstd frame from r into buf and returns
// buf.Bytes(). buf must be Reset before the call; the caller owns the bytes until
// it resets or reuses buf. Returns io.EOF if r is at end-of-stream before the
// frame magic (clean termination).
func readZstdFrame(r io.Reader, buf *bytes.Buffer) ([]byte, error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, err // io.EOF = clean end; io.ErrUnexpectedEOF = truncated
	}
	if binary.LittleEndian.Uint32(magic[:]) != zstdFrameMagic {
		return nil, fmt.Errorf("expected zstd frame magic, got %#010x", binary.LittleEndian.Uint32(magic[:]))
	}
	buf.Write(magic[:])

	var fhd [1]byte
	if _, err := io.ReadFull(r, fhd[:]); err != nil {
		return nil, errors.Wrap(err, "read FHD")
	}
	buf.Write(fhd[:])
	fcsFlag := (fhd[0] >> 6) & 0x3
	singleSeg := (fhd[0] >> 5) & 0x1
	contentChecksum := (fhd[0] >> 2) & 0x1
	dictIDFlag := fhd[0] & 0x3

	// Window descriptor absent when Single_Segment_Flag=1.
	if singleSeg == 0 {
		var wd [1]byte
		if _, err := io.ReadFull(r, wd[:]); err != nil {
			return nil, errors.Wrap(err, "read window descriptor")
		}
		buf.Write(wd[:])
	}

	// Dictionary ID: 0, 1, 2, or 4 bytes.
	var dictIDSize int
	switch dictIDFlag {
	case 1:
		dictIDSize = 1
	case 2:
		dictIDSize = 2
	case 3:
		dictIDSize = 4
	}
	if err := bufReadN(r, buf, dictIDSize); err != nil {
		return nil, errors.Wrap(err, "read dict ID")
	}

	// Frame Content Size: 0/1/2/4/8 bytes depending on flags.
	var fcsSize int
	if singleSeg == 1 && fcsFlag == 0 {
		fcsSize = 1
	} else {
		switch fcsFlag {
		case 1:
			fcsSize = 2
		case 2:
			fcsSize = 4
		case 3:
			fcsSize = 8
		}
	}
	if err := bufReadN(r, buf, fcsSize); err != nil {
		return nil, errors.Wrap(err, "read frame content size")
	}

	// Blocks: read until Last_Block flag.
	for {
		var blockHdr [3]byte
		if _, err := io.ReadFull(r, blockHdr[:]); err != nil {
			return nil, errors.Wrap(err, "read block header")
		}
		buf.Write(blockHdr[:])
		lastBlock := blockHdr[0] & 0x1
		blockType := (blockHdr[0] >> 1) & 0x3
		blockSize := (uint32(blockHdr[2]) << 13) | (uint32(blockHdr[1]) << 5) | (uint32(blockHdr[0]) >> 3)

		// Wire bytes: Raw=blockSize, RLE=1 byte, Compressed=blockSize.
		var contentSize int
		switch blockType {
		case 0:
			contentSize = int(blockSize)
		case 1:
			contentSize = 1
		case 2:
			contentSize = int(blockSize)
		default:
			return nil, fmt.Errorf("reserved zstd block type %d", blockType)
		}
		if err := bufReadN(r, buf, contentSize); err != nil {
			return nil, errors.Wrap(err, "read block content")
		}
		if lastBlock == 1 {
			break
		}
	}

	// Optional 4-byte content checksum.
	if contentChecksum == 1 {
		var chk [4]byte
		if _, err := io.ReadFull(r, chk[:]); err != nil {
			return nil, errors.Wrap(err, "read content checksum")
		}
		buf.Write(chk[:])
	}

	return buf.Bytes(), nil
}

// bufReadN reads exactly n bytes from r directly into buf, reusing buf's internal
// storage. It is a no-op when n == 0.
func bufReadN(r io.Reader, buf *bytes.Buffer, n int) error {
	if n == 0 {
		return nil
	}
	buf.Grow(n)
	_, err := io.ReadFull(r, buf.AvailableBuffer()[:n])
	if err != nil {
		return err
	}
	buf.Write(buf.AvailableBuffer()[:n])
	return nil
}

// frameBufPool holds reusable bytes.Buffers for reading compressed frame data.
// Each buffer is Reset before use and returned after DecodeAll completes.
// Average compressed frame size: ~4–5 MB.
var frameBufPool = sync.Pool{
	New: func() any { return bytes.NewBuffer(make([]byte, 0, 5<<20)) },
}

// outputBufPool holds reusable byte slices for DecodeAll decompression output.
// Each slice is returned to the pool after the consumer writes it to the pipe.
// Average decompressed frame size: ~8–10 MB.
var outputBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 10<<20)
		return &b
	},
}

// zstdDecoderPool holds reusable single-threaded decoders for parallel DecodeAll calls.
var zstdDecoderPool = sync.Pool{
	New: func() any {
		d, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		return d
	},
}

// RestoreConfig holds the parameters for a cache restore operation.
type RestoreConfig struct {
	// Bucket is the S3 bucket name. Mutually exclusive with CachewURL.
	Bucket string
	// Region is the AWS region. Falls back to AWS_REGION / AWS_DEFAULT_REGION env vars, then "us-west-2".
	Region string
	// CachewURL is the cachew server URL. Mutually exclusive with Bucket.
	CachewURL string
	// CacheKey is the bundle identifier (e.g. "my-project:assembleRelease").
	CacheKey string
	// GitDir is the path to the git repository for history walking. Defaults to ".".
	GitDir string
	// Ref is the git ref to start the history walk from. Defaults to "HEAD".
	Ref string
	// Commit is a specific commit SHA to try directly, skipping history walk.
	Commit string
	// MaxBlocks is the number of distinct-author commit blocks to search. Defaults to 20.
	MaxBlocks int
	// GradleUserHome is the path to GRADLE_USER_HOME. Defaults to ~/.gradle.
	GradleUserHome string
	// IncludedBuilds lists included build directories whose build/ output to
	// restore (relative to project root). Defaults to ["buildSrc"].
	IncludedBuilds []string
	// Branch is an optional branch name to also apply a delta bundle for.
	Branch string
	// Metrics is an optional metrics client. If nil, a no-op client is used.
	Metrics MetricsClient
	// Logger is an optional structured logger. If nil, slog.Default() is used.
	Logger *slog.Logger
}

// defaultRegion returns the AWS region from environment variables, falling back to us-west-2.
func defaultRegion() string {
	if r := os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
		return r
	}
	return "us-west-2"
}

func (c *RestoreConfig) defaults() {
	if c.Region == "" {
		c.Region = defaultRegion()
	}
	if c.GitDir == "" {
		c.GitDir = "."
	}
	if c.Ref == "" {
		c.Ref = "HEAD"
	}
	if c.MaxBlocks == 0 {
		c.MaxBlocks = 20
	}
	if c.GradleUserHome == "" {
		home, _ := os.UserHomeDir()
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if len(c.IncludedBuilds) == 0 {
		c.IncludedBuilds = []string{"buildSrc"}
	}
	if c.Metrics == nil {
		c.Metrics = NoopMetrics{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

func (c *RestoreConfig) validate() error {
	if c.CacheKey == "" {
		return errors.New("CacheKey is required")
	}
	if c.Bucket != "" && c.CachewURL != "" {
		return errors.New("Bucket and CachewURL are mutually exclusive")
	}
	return nil
}

// Restore downloads and extracts a Gradle cache bundle, routing tar entries
// directly to their final destinations. It walks git history to find the most
// recent cached bundle, downloads it with parallel range requests, and streams
// it through zstd decompression into the filesystem.
//
// If Branch is set, a delta bundle is downloaded concurrently and applied after
// the base extraction.
func Restore(ctx context.Context, cfg RestoreConfig) error {
	// Ensure GOMAXPROCS is high enough for the I/O-bound goroutine pools.
	if runtime.GOMAXPROCS(0) < 16 {
		runtime.GOMAXPROCS(16)
	}

	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	log := cfg.Logger

	store, err := newStore(cfg.Bucket, cfg.Region, cfg.CachewURL)
	if err != nil {
		return err
	}

	// ── Find phase ───────────────────────────────────────────────────────
	findStart := time.Now()

	var commits []string
	if cfg.Commit != "" {
		commits = []string{cfg.Commit}
	} else {
		commits, err = historyCommits(ctx, cfg.GitDir, cfg.Ref, cfg.MaxBlocks)
		if err != nil {
			return errors.Wrap(err, "walk git history")
		}
	}

	var hitCommit string
	var hitInfo bundleStatInfo
	var lastErr error
	for _, sha := range commits {
		info, err := store.stat(ctx, sha, cfg.CacheKey)
		if err == nil {
			hitCommit = sha
			hitInfo = info
			break
		}
		lastErr = err
		log.Debug("cache miss", "sha", sha[:min(8, len(sha))], "err", err)
	}
	log.Debug("find complete", "duration", time.Since(findStart), "commits_checked", len(commits))

	if hitCommit == "" {
		log.Info("no cache bundle found in history", "last_err", lastErr, "commits_checked", len(commits))
		return nil
	}
	log.Info("cache hit", "commit", hitCommit, "cache-key", cfg.CacheKey)

	// ── Delta pre-fetch (concurrent with base extraction) ────────────────
	type deltaResult struct {
		tmpFile *os.File
		dlStart time.Time
		n       int64
		eofAt   time.Time
		err     error
	}
	var deltaCh chan deltaResult
	if cfg.Branch != "" {
		deltaCh = make(chan deltaResult, 1)
		go func() {
			dc := deltaCommit(cfg.Branch)
			deltaInfo, statErr := store.stat(ctx, dc, cfg.CacheKey)
			if statErr != nil {
				log.Info("no delta bundle found for branch", "branch", cfg.Branch)
				deltaCh <- deltaResult{}
				return
			}
			log.Info("found delta bundle, downloading in background", "branch", cfg.Branch)
			dlStart := time.Now()
			body, err := store.get(ctx, dc, cfg.CacheKey, deltaInfo)
			if err != nil {
				deltaCh <- deltaResult{err: errors.Wrap(err, "get delta bundle")}
				return
			}
			defer body.Close() //nolint:errcheck,gosec
			tmp, err := os.CreateTemp("", "gradle-cache-delta-dl-*")
			if err != nil {
				deltaCh <- deltaResult{err: errors.Wrap(err, "create delta temp file")}
				return
			}
			cb := &countingBody{r: body, dlStart: dlStart}
			if _, err := io.Copy(tmp, cb); err != nil {
				tmp.Close()           //nolint:errcheck,gosec
				os.Remove(tmp.Name()) //nolint:errcheck,gosec
				deltaCh <- deltaResult{err: errors.Wrap(err, "buffer delta bundle")}
				return
			}
			if _, err := tmp.Seek(0, io.SeekStart); err != nil {
				tmp.Close()           //nolint:errcheck,gosec
				os.Remove(tmp.Name()) //nolint:errcheck,gosec
				deltaCh <- deltaResult{err: errors.Wrap(err, "rewind delta temp file")}
				return
			}
			deltaCh <- deltaResult{tmpFile: tmp, dlStart: dlStart, n: cb.n, eofAt: cb.eofAt}
		}()
	}

	// ── Download + extract phase (pipelined) ─────────────────────────────
	dlStart := time.Now()
	log.Info("downloading bundle", "commit", hitCommit[:min(8, len(hitCommit))])

	if err := os.MkdirAll(cfg.GradleUserHome, 0o750); err != nil {
		return errors.Wrap(err, "create gradle user home dir")
	}
	entries, _ := os.ReadDir(cfg.GradleUserHome)
	gradleUserHomeEmpty := len(entries) == 0

	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}

	rules := []extractRule{
		{prefix: "caches/", baseDir: cfg.GradleUserHome},
		{prefix: "wrapper/", baseDir: cfg.GradleUserHome},
		{prefix: "configuration-cache/", baseDir: filepath.Join(projectDir, ".gradle")},
	}

	body, err := store.get(ctx, hitCommit, cfg.CacheKey, hitInfo)
	if err != nil {
		return errors.Wrap(err, "get bundle")
	}
	defer body.Close() //nolint:errcheck,gosec

	cb := &countingBody{r: body, dlStart: dlStart}
	netTiming := &timingReader{r: cb}
	ps, err := extractBundleZstd(ctx, netTiming, rules, projectDir, !gradleUserHomeEmpty)
	if err != nil {
		return errors.Wrap(err, "extract bundle")
	}

	totalElapsed := time.Since(dlStart)

	if !cb.eofAt.IsZero() {
		dlElapsed := cb.eofAt.Sub(dlStart)
		activeMbps := float64(cb.n) / netTiming.blocked.Seconds() / 1e6
		log.Info("download complete", "duration", dlElapsed.Round(time.Millisecond),
			"size_mb", fmt.Sprintf("%.1f", float64(cb.n)/1e6),
			"speed_mbps", fmt.Sprintf("%.1f", float64(cb.n)/dlElapsed.Seconds()/1e6),
			"s3_mbps", fmt.Sprintf("%.1f", activeMbps))
	}

	decompressBusy := ps.extractWait - ps.decompressWait
	diskBusy := ps.wallTime - ps.extractWait
	attrs := []any{
		"total_duration", totalElapsed.Round(time.Millisecond),
		"bottleneck", ps.bottleneck(),
		"uncompressed_mb", fmt.Sprintf("%.1f", float64(ps.uncompressedBytes)/1e6),
	}
	if ps.decompressWait > 0 {
		attrs = append(attrs, "download_mbps", fmt.Sprintf("%.1f", float64(ps.compressedBytes)/ps.decompressWait.Seconds()/1e6))
	}
	if decompressBusy > 0 {
		attrs = append(attrs, "decompress_mbps", fmt.Sprintf("%.1f", float64(ps.uncompressedBytes)/decompressBusy.Seconds()/1e6))
	}
	if diskBusy > 0 {
		attrs = append(attrs, "disk_mbps", fmt.Sprintf("%.1f", float64(ps.uncompressedBytes)/diskBusy.Seconds()/1e6))
	}
	log.Info("restore pipeline complete", attrs...)
	cfg.Metrics.Distribution("gradle_cache.restore.duration_ms", float64(totalElapsed.Milliseconds()), "cache_key:"+cfg.CacheKey)
	cfg.Metrics.Distribution("gradle_cache.restore.size_bytes", float64(cb.n), "cache_key:"+cfg.CacheKey)
	if !cb.eofAt.IsZero() {
		dlElapsed := cb.eofAt.Sub(dlStart)
		mbps := float64(cb.n) / dlElapsed.Seconds() / 1e6
		cfg.Metrics.Distribution("gradle_cache.restore.speed_mbps", mbps, "cache_key:"+cfg.CacheKey)
	}

	if err := touchMarkerFile(filepath.Join(cfg.GradleUserHome, ".cache-restore-marker")); err != nil {
		log.Warn("could not write restore marker", "err", err)
	}

	// ── Apply delta bundle (if Branch was given) ─────────────────────────
	if deltaCh != nil {
		dr := <-deltaCh
		if dr.err != nil {
			return dr.err
		}
		if dr.tmpFile != nil {
			defer func() {
				dr.tmpFile.Close()           //nolint:errcheck,gosec
				os.Remove(dr.tmpFile.Name()) //nolint:errcheck,gosec
			}()
			if !dr.eofAt.IsZero() {
				dlElapsed := dr.eofAt.Sub(dr.dlStart)
				log.Info("delta download complete", "branch", cfg.Branch,
					"duration", dlElapsed.Round(time.Millisecond),
					"size_mb", fmt.Sprintf("%.1f", float64(dr.n)/1e6),
					"speed_mbps", fmt.Sprintf("%.1f", float64(dr.n)/dlElapsed.Seconds()/1e6))
			}
			applyStart := time.Now()
			if err := extractTarZstd(ctx, dr.tmpFile, cfg.GradleUserHome); err != nil {
				return errors.Wrap(err, "extract delta bundle")
			}
			log.Info("applied delta bundle", "branch", cfg.Branch,
				"duration", time.Since(applyStart).Round(time.Millisecond))
		}
	}

	log.Debug("restore complete")
	return nil
}

// extractRule maps a tar entry path prefix to a destination base directory.
type extractRule struct {
	prefix  string
	baseDir string
}

// pipelineStats captures timing for each stage of the download→decompress→extract pipeline.
type pipelineStats struct {
	wallTime          time.Duration // total pipeline wall time
	decompressWait    time.Duration // time decompressor blocked reading from download
	extractWait       time.Duration // time tar extractor blocked reading from decompressor
	compressedBytes   int64         // bytes read from network (compressed)
	uncompressedBytes int64         // bytes read from decompressor (uncompressed)
}

// bottleneck returns a human-readable label for the slowest pipeline stage.
// Each stage's busy time = wall time − its wait time. The busiest stage is
// the bottleneck because the other stages spent the remaining time idle.
func (ps pipelineStats) bottleneck() string {
	if ps.wallTime == 0 {
		return "balanced"
	}
	// Download has no upstream, so its busy time ≈ wallTime − 0 isn't useful.
	// Instead we infer it: decompressor's wait on download = download's busy
	// time from the decompressor's perspective.
	downloadBusy := ps.decompressWait                    // time spent fetching bytes
	decompressBusy := ps.extractWait - ps.decompressWait // time decompressing (not waiting on download)
	diskBusy := ps.wallTime - ps.extractWait             // time writing to disk (not waiting on decompress)

	// Clamp negative values (can happen due to buffering overlap).
	if decompressBusy < 0 {
		decompressBusy = 0
	}
	if diskBusy < 0 {
		diskBusy = 0
	}

	max := downloadBusy
	label := "download"
	if decompressBusy > max {
		max = decompressBusy
		label = "decompress"
	}
	if diskBusy > max {
		label = "disk"
	}
	return label
}

// extractBundleZstd decompresses and extracts a base bundle. It auto-detects
// pzstd multi-frame format and dispatches to a parallel decompressor; otherwise
// falls back to the single-frame klauspost streaming decoder.
func extractBundleZstd(ctx context.Context, r io.Reader, rules []extractRule, defaultDir string, skipExisting bool) (pipelineStats, error) {
	pipeStart := time.Now()
	dlTiming := &timingReader{r: r}
	br := bufio.NewReaderSize(dlTiming, 8<<20)

	magic, err := peekMagic(br)
	if err != nil {
		return pipelineStats{}, err
	}
	if magic >= pzstdMagicMin && magic <= pzstdMagicMax {
		slog.Debug("using multi-frame parallel decoder")
		return extractBundleZstdMultiFrame(ctx, br, dlTiming, pipeStart, rules, defaultDir, skipExisting)
	}
	slog.Debug("using klauspost single-frame decoder")
	return extractBundleZstdSingleFrame(br, dlTiming, pipeStart, rules, defaultDir, skipExisting)
}

func extractBundleZstdSingleFrame(br *bufio.Reader, dlTiming *timingReader, pipeStart time.Time, rules []extractRule, defaultDir string, skipExisting bool) (pipelineStats, error) {
	dec, err := zstd.NewReader(br, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return pipelineStats{}, errors.Wrap(err, "create zstd decoder")
	}
	defer dec.Close()

	decTiming := &timingReader{r: dec}
	targetFn := func(name string) string {
		for _, rule := range rules {
			if strings.HasPrefix(name, rule.prefix) {
				return filepath.Join(rule.baseDir, name)
			}
		}
		return filepath.Join(defaultDir, name)
	}

	if err := extractTarPlatformRouted(decTiming, targetFn, skipExisting); err != nil {
		return pipelineStats{}, err
	}
	if err := drainCompressedReader(br); err != nil {
		return pipelineStats{}, errors.Wrap(err, "drain compressed reader")
	}
	return pipelineStats{
		wallTime:          time.Since(pipeStart),
		decompressWait:    dlTiming.blocked,
		extractWait:       decTiming.blocked,
		compressedBytes:   dlTiming.bytes,
		uncompressedBytes: decTiming.bytes,
	}, nil
}

// extractBundleZstdMultiFrame decompresses a pzstd multi-frame archive.
// Frames are dispatched to worker goroutines for parallel DecodeAll and
// reassembled in order into the tar extraction pipeline.
func extractBundleZstdMultiFrame(ctx context.Context, br *bufio.Reader, dlTiming *timingReader, pipeStart time.Time, rules []extractRule, defaultDir string, skipExisting bool) (pipelineStats, error) {
	if err := skipPzstdSkippableFrame(br); err != nil {
		return pipelineStats{}, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	const maxInFlight = 8

	type frameResult struct {
		data   []byte
		outBuf *[]byte // non-nil → return to outputBufPool after the consumer writes data
		err    error
	}
	type frameJob struct {
		resultCh chan frameResult
	}

	jobQueue := make(chan frameJob, maxInFlight)
	pr, pw := io.Pipe()

	// Consumer: reads jobs in order, writes decompressed data to the pipe.
	consumerDone := make(chan error, 1)
	go func() {
		var firstErr error
		for job := range jobQueue {
			result := <-job.resultCh
			if firstErr != nil {
				// Drain: return pooled buffer even on error path.
				if result.outBuf != nil {
					outputBufPool.Put(result.outBuf)
				}
				continue
			}
			if result.err != nil {
				firstErr = result.err
				if result.outBuf != nil {
					outputBufPool.Put(result.outBuf)
				}
				continue
			}
			_, writeErr := pw.Write(result.data)
			// Return the output buffer to the pool immediately after Write —
			// the data has been copied into the pipe at this point.
			if result.outBuf != nil {
				*result.outBuf = result.data[:0]
				outputBufPool.Put(result.outBuf)
			}
			if writeErr != nil {
				firstErr = writeErr
			}
		}
		if firstErr != nil {
			pw.CloseWithError(firstErr)
		} else {
			_ = pw.Close()
		}
		consumerDone <- firstErr
	}()

	// Dispatcher: reads frames sequentially, decompresses in parallel.
	dispatchErrCh := make(chan error, 1)
	sem := make(chan struct{}, maxInFlight)
	go func() {
		var dispatchErr error
		var frameCount int
		defer func() {
			slog.Debug("multi-frame dispatch complete", "frames", frameCount)
			close(jobQueue)
			dispatchErrCh <- dispatchErr
		}()
		for {
			// Peek at magic to skip any interleaved skippable frames
			// (pzstd may insert them between data frames or at the end).
			magic, err := peekMagic(br)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
					dispatchErr = errors.Wrap(err, "peek frame magic")
				}
				return
			}
			if magic >= pzstdMagicMin && magic <= pzstdMagicMax {
				if err := skipPzstdSkippableFrame(br); err != nil {
					dispatchErr = errors.Wrap(err, "skip interleaved skippable frame")
				}
				continue
			}

			inBuf := frameBufPool.Get().(*bytes.Buffer)
			inBuf.Reset()
			frameData, err := readZstdFrame(br, inBuf)
			if err != nil {
				frameBufPool.Put(inBuf)
				if !errors.Is(err, io.EOF) {
					dispatchErr = errors.Errorf("read zstd frame %d: %w", frameCount+1, err)
				}
				return
			}
			frameCount++

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				frameBufPool.Put(inBuf)
				return
			}

			job := frameJob{resultCh: make(chan frameResult, 1)}
			select {
			case jobQueue <- job:
			case <-ctx.Done():
				frameBufPool.Put(inBuf)
				<-sem
				return
			}

			go func(j frameJob, data []byte, fb *bytes.Buffer) {
				defer func() { <-sem }()
				dec := zstdDecoderPool.Get().(*zstd.Decoder)
				outBuf := outputBufPool.Get().(*[]byte)
				out, decErr := dec.DecodeAll(data, (*outBuf)[:0])
				// Compressed data fully consumed; return frame buffer to pool.
				frameBufPool.Put(fb)
				zstdDecoderPool.Put(dec)
				*outBuf = out
				select {
				case j.resultCh <- frameResult{data: out, outBuf: outBuf, err: decErr}:
				case <-ctx.Done():
					*outBuf = out[:0]
					outputBufPool.Put(outBuf)
				}
			}(job, frameData, inBuf)
		}
	}()

	// Extract tar from the ordered decompressed stream.
	decTiming := &timingReader{r: pr}
	targetFn := func(name string) string {
		for _, rule := range rules {
			if strings.HasPrefix(name, rule.prefix) {
				return filepath.Join(rule.baseDir, name)
			}
		}
		return filepath.Join(defaultDir, name)
	}
	extractErr := extractTarPlatformRouted(decTiming, targetFn, skipExisting)
	_ = pr.Close()
	cancel()

	consumerErr := <-consumerDone
	dispatchErr := <-dispatchErrCh

	if extractErr != nil {
		return pipelineStats{}, extractErr
	}
	if dispatchErr != nil {
		return pipelineStats{}, dispatchErr
	}
	// If extraction succeeded, ignore consumer pipe errors — closing the
	// read end after tar finishes is expected and may race with a pending write.
	if consumerErr != nil && !errors.Is(consumerErr, io.ErrClosedPipe) {
		return pipelineStats{}, errors.Wrap(consumerErr, "assemble decompressed frames")
	}

	return pipelineStats{
		wallTime:          time.Since(pipeStart),
		decompressWait:    dlTiming.blocked,
		extractWait:       decTiming.blocked,
		compressedBytes:   dlTiming.bytes,
		uncompressedBytes: decTiming.bytes,
	}, nil
}

func extractTarZstd(_ context.Context, r io.Reader, dir string) error {
	br := bufio.NewReaderSize(r, 8<<20)
	dec, err := zstd.NewReader(br, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return errors.Wrap(err, "create zstd decoder")
	}
	defer dec.Close()
	if err := extractTarPlatform(dec, dir); err != nil {
		return err
	}
	if err := drainCompressedReader(br); err != nil {
		return errors.Wrap(err, "drain compressed reader")
	}
	return nil
}

func drainCompressedReader(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

// countingBody wraps an io.Reader, counts bytes consumed, and records the time
// the underlying reader returns io.EOF.
type countingBody struct {
	r       io.Reader
	n       int64
	dlStart time.Time
	eofAt   time.Time
}

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if err == io.EOF && c.eofAt.IsZero() {
		c.eofAt = time.Now()
	}
	return n, err
}

// timingReader wraps an io.Reader and accumulates the wall-clock time spent
// blocked inside Read calls and bytes transferred. This lets us measure how
// long a downstream consumer waits for its upstream producer at each pipeline
// boundary and compute per-stage throughput.
type timingReader struct {
	r       io.Reader
	blocked time.Duration
	bytes   int64
}

func (t *timingReader) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := t.r.Read(p)
	t.blocked += time.Since(start)
	t.bytes += int64(n)
	return n, err
}

func touchMarkerFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return errors.Wrap(err, "create marker parent dir")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}
