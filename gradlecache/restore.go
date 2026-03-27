// Package gradlecache provides a Go library for restoring Gradle build cache
// bundles from S3 or cachew. It wraps the same logic used by the gradle-cache
// CLI in an importable API.
package gradlecache

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/DataDog/zstd"
	"github.com/alecthomas/errors"
)

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
	for _, sha := range commits {
		if info, err := store.stat(ctx, sha, cfg.CacheKey); err == nil {
			hitCommit = sha
			hitInfo = info
			break
		}
		log.Debug("cache miss", "sha", sha[:min(8, len(sha))])
	}
	log.Debug("find complete", "duration", time.Since(findStart), "commits_checked", len(commits))

	if hitCommit == "" {
		log.Info("no cache bundle found in history")
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
	if err := extractBundleZstd(ctx, cb, rules, projectDir, !gradleUserHomeEmpty); err != nil {
		return errors.Wrap(err, "extract bundle")
	}

	totalElapsed := time.Since(dlStart)

	if !cb.eofAt.IsZero() {
		dlElapsed := cb.eofAt.Sub(dlStart)
		log.Info("download complete", "duration", dlElapsed.Round(time.Millisecond),
			"size_mb", fmt.Sprintf("%.1f", float64(cb.n)/1e6),
			"speed_mbps", fmt.Sprintf("%.1f", float64(cb.n)/dlElapsed.Seconds()/1e6))
	}

	log.Info("restore pipeline complete",
		"total_duration", totalElapsed.Round(time.Millisecond))
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

// extractBundleZstd decompresses and extracts a base bundle, routing tar
// entries to their final destinations based on rules.
func extractBundleZstd(_ context.Context, r io.Reader, rules []extractRule, defaultDir string, skipExisting bool) error {
	br := bufio.NewReaderSize(r, 8<<20)
	dec := zstd.NewReader(br)
	defer dec.Close() //nolint:errcheck

	targetFn := func(name string) string {
		for _, rule := range rules {
			if strings.HasPrefix(name, rule.prefix) {
				return filepath.Join(rule.baseDir, name)
			}
		}
		return filepath.Join(defaultDir, name)
	}

	if err := extractTarPlatformRouted(dec, targetFn, skipExisting); err != nil {
		return err
	}
	if err := drainCompressedReader(br); err != nil {
		return errors.Wrap(err, "drain compressed reader")
	}
	return nil
}

func extractTarZstd(_ context.Context, r io.Reader, dir string) error {
	br := bufio.NewReaderSize(r, 8<<20)
	dec := zstd.NewReader(br)
	defer dec.Close() //nolint:errcheck
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
