// gradle-cache restores and saves Gradle build cache bundles from S3 or cachew.
//
// Base bundles are stored at s3://{bucket}/{commit}/{cache-key}/{bundle-file},
// where bundle-file is the cache key with colons replaced by dashes + ".tar.zst".
// This format is compatible with the bundled-cache-manager Ruby script.
//
// On restore, the tool walks the local git history (counting distinct-author
// "blocks") to find the most recent S3 hit, downloads it, and extracts it
// directly into the final destination directories (no staging dir, no symlinks).
// A restore marker file is written immediately after extraction; save-delta uses
// its mtime as the baseline to identify files created during the build.
//
// PR branch delta workflow (two invocations per phase):
//
//	Restore phase: restore (base) → restore-delta --branch $BRANCH
//	Save phase:    save-delta --branch $BRANCH
//
// Delta bundles are keyed by branch name at branches/{slug}/{cache-key}/{bundle-file},
// so they survive rebases and force-pushes. Each delta is a cumulative snapshot of
// all files added on top of the base bundle since the branch diverged.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/kong"
)

type CLI struct {
	LogLevel     string          `help:"Log level." default:"info" enum:"debug,info,warn,error"`
	Restore      RestoreCmd      `cmd:"" help:"Find the newest cached bundle in history and restore it to GRADLE_USER_HOME."`
	RestoreDelta RestoreDeltaCmd `cmd:"" help:"Apply a branch delta bundle on top of an already-restored base cache."`
	Save         SaveCmd         `cmd:"" help:"Bundle GRADLE_USER_HOME/caches and upload to S3 tagged with a commit SHA."`
	SaveDelta    SaveDeltaCmd    `cmd:"" help:"Pack files added since the last restore and upload as a branch delta bundle."`
}

type backendFlags struct {
	Bucket    string `help:"S3 bucket name."`
	Region    string `help:"AWS region." default:"us-west-2"`
	CachewURL string `help:"Cachew server URL (e.g. http://localhost:8080). Mutually exclusive with --bucket." name:"cachew-url"`
}

// bundleStore abstracts over S3 and cachew as storage backends for Gradle cache bundles.
type bundleStore interface {
	stat(ctx context.Context, commit, cacheKey string) (int64, error)
	get(ctx context.Context, commit, cacheKey string, size int64) (io.ReadCloser, error)
	put(ctx context.Context, commit, cacheKey string, r io.ReadSeeker, size int64) error
}

// s3BundleStore adapts the low-level s3Client to the bundleStore interface,
// computing the canonical S3 key from commit and cacheKey.
type s3BundleStore struct {
	client *s3Client
	bucket string
}

func (s *s3BundleStore) stat(ctx context.Context, commit, cacheKey string) (int64, error) {
	return s.client.stat(ctx, s.bucket, s3Key(commit, cacheKey, bundleFilename(cacheKey)))
}

func (s *s3BundleStore) get(ctx context.Context, commit, cacheKey string, size int64) (io.ReadCloser, error) {
	return s.client.get(ctx, s.bucket, s3Key(commit, cacheKey, bundleFilename(cacheKey)), size)
}

func (s *s3BundleStore) put(ctx context.Context, commit, cacheKey string, r io.ReadSeeker, size int64) error {
	return s.client.put(ctx, s.bucket, s3Key(commit, cacheKey, bundleFilename(cacheKey)), r, size, "application/zstd")
}

func (f *backendFlags) newStore() (bundleStore, error) {
	if f.CachewURL != "" {
		return newCachewClient(f.CachewURL), nil
	}
	client, err := newS3Client(f.Region)
	if err != nil {
		return nil, err
	}
	return &s3BundleStore{client: client, bucket: f.Bucket}, nil
}

// RestoreCmd downloads and extracts a Gradle cache bundle, then symlinks
// $GRADLE_USER_HOME/caches to the extracted directory.
// Also restores configuration-cache and included build output dirs if present in the bundle.
//
// If --branch is given the branch delta bundle is downloaded concurrently with
// the base extraction and applied immediately after the symlink is set up,
// collapsing the two-step restore/restore-delta workflow into a single invocation.
type RestoreCmd struct {
	backendFlags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	GitDir         string   `help:"Path to the git repository used for history walking." default:"." type:"path"`
	Ref            string   `help:"Git ref to start the history walk from." default:"HEAD"`
	Commit         string   `help:"Specific commit SHA to try directly, skipping history walk."`
	MaxBlocks      int      `help:"Number of distinct-author commit blocks to search." default:"20"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to restore (relative to project root). Use 'dir/*' to restore build/ for all subdirectories. May be repeated." name:"included-build"`
	Branch         string   `help:"Branch name to also apply a delta bundle for (typically $$BRANCH_NAME). The delta download runs concurrently with base extraction." optional:""`
}

func (c *RestoreCmd) AfterApply() error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if len(c.IncludedBuilds) == 0 {
		c.IncludedBuilds = []string{"buildSrc"}
	}
	if c.Bucket == "" && c.CachewURL == "" {
		return errors.New("one of --bucket or --cachew-url is required")
	}
	if c.Bucket != "" && c.CachewURL != "" {
		return errors.New("--bucket and --cachew-url are mutually exclusive")
	}
	return nil
}

func (c *RestoreCmd) Run(ctx context.Context) error {
	totalStart := time.Now()

	store, err := c.newStore()
	if err != nil {
		return err
	}

	// ── Find phase ────────────────────────────────────────────────────────────
	findStart := time.Now()

	var commits []string
	if c.Commit != "" {
		commits = []string{c.Commit}
	} else {
		commits, err = historyCommits(ctx, c.GitDir, c.Ref, c.MaxBlocks)
		if err != nil {
			return errors.Wrap(err, "walk git history")
		}
	}

	var hitCommit string
	var hitSize int64
	for _, sha := range commits {
		if size, err := store.stat(ctx, sha, c.CacheKey); err == nil {
			hitCommit = sha
			hitSize = size
			break
		}
		slog.Debug("cache miss", "sha", sha[:min(8, len(sha))])
	}
	slog.Debug("find complete", "duration", time.Since(findStart), "commits_checked", len(commits))

	if hitCommit == "" {
		slog.Info("no cache bundle found in history")
		return nil
	}
	slog.Info("cache hit", "commit", hitCommit[:min(8, len(hitCommit))], "cache-key", c.CacheKey)

	// ── Delta pre-fetch (concurrent with base extraction) ─────────────────────
	// If --branch is set, kick off a goroutine that stats + downloads the delta
	// bundle to a temp file while the base bundle is streaming in. The delta is
	// typically tens of MB; the base is hundreds to thousands of MB — so the
	// delta is almost always fully buffered before the base extraction finishes,
	// eliminating the delta's network latency from the critical path.
	type deltaResult struct {
		tmpFile *os.File
		dlStart time.Time
		n       int64
		eofAt   time.Time
		err     error
	}
	var deltaCh chan deltaResult
	if c.Branch != "" {
		deltaCh = make(chan deltaResult, 1)
		go func() {
			dc := deltaCommit(c.Branch)
			_, statErr := store.stat(ctx, dc, c.CacheKey)
			if statErr != nil {
				slog.Info("no delta bundle found for branch", "branch", c.Branch)
				deltaCh <- deltaResult{} // empty result; nil tmpFile signals "no delta"
				return
			}
			slog.Info("found delta bundle, downloading in background", "branch", c.Branch)
			dlStart := time.Now()
			body, err := store.get(ctx, dc, c.CacheKey, 0)
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

	// ── Download + extract phase (pipelined) ─────────────────────────────────
	// The bundle streams directly into pzstd → extractor with no temp file.
	// Download and extraction run concurrently: pzstd decompresses as bytes
	// arrive, and the extractor writes files as blocks are decompressed.
	// This keeps total time close to max(download_time, extract_time) rather
	// than their sum.
	dlStart := time.Now()
	slog.Info("downloading bundle", "commit", hitCommit[:min(8, len(hitCommit))])

	// Ensure GRADLE_USER_HOME exists before extracting into it.
	if err := os.MkdirAll(c.GradleUserHome, 0o750); err != nil {
		return errors.Wrap(err, "create gradle user home dir")
	}

	// Resolve the project directory upfront; bundle entries are routed here for
	// configuration-cache and convention build dirs.
	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}

	// Route tar entries to their final destinations directly:
	//   ./caches/...               → GRADLE_USER_HOME/caches/...
	//   ./configuration-cache/...  → <project>/.gradle/configuration-cache/...
	//   everything else            → <project>/...  (buildSrc/build, plugins/*/build, …)
	// Existing files are left untouched (skipExisting=true) so a partial
	// pre-existing cache is merged rather than overwritten.
	rules := []extractRule{
		{prefix: "caches/", baseDir: c.GradleUserHome},
		{prefix: "configuration-cache/", baseDir: filepath.Join(projectDir, ".gradle")},
	}

	body, err := store.get(ctx, hitCommit, c.CacheKey, hitSize)
	if err != nil {
		return errors.Wrap(err, "get bundle")
	}
	defer body.Close() //nolint:errcheck,gosec

	// countingBody records bytes consumed and timestamps when the S3 body is
	// exhausted so we can log download speed independently of extraction.
	cb := &countingBody{r: body, dlStart: dlStart}
	if err := extractBundleZstd(ctx, cb, rules, projectDir); err != nil {
		return errors.Wrap(err, "extract bundle")
	}

	totalElapsed := time.Since(dlStart)

	// Log download phase: time from start until the last S3 byte was consumed
	// by the pzstd pipeline. Because download and extraction run concurrently,
	// this is normally the dominant term.
	if !cb.eofAt.IsZero() {
		dlElapsed := cb.eofAt.Sub(dlStart)
		slog.Info("download complete", "duration", dlElapsed.Round(time.Millisecond),
			"size_mb", fmt.Sprintf("%.1f", float64(cb.n)/1e6),
			"speed_mbps", fmt.Sprintf("%.1f", float64(cb.n)/dlElapsed.Seconds()/1e6))
	}

	// Log total restore time. Download and extraction are pipelined so
	// total ≈ download time + a small flush of buffered pipeline stages.
	slog.Info("restore pipeline complete",
		"total_duration", totalElapsed.Round(time.Millisecond))

	// Write a marker recording when the base restore finished.
	// save-delta compares file mtimes against this to identify files created
	// during the build. Our Go extractor never calls chtimes, so all restored
	// files have mtime ≈ extraction time — any file with mtime > marker was
	// created (or overlaid by restore-delta) after this point.
	if err := touchMarkerFile(filepath.Join(c.GradleUserHome, ".cache-restore-marker")); err != nil {
		slog.Warn("could not write restore marker", "err", err)
	}

	// ── Apply delta bundle (if --branch was given) ────────────────────────────
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
				slog.Info("delta download complete", "branch", c.Branch,
					"duration", dlElapsed.Round(time.Millisecond),
					"size_mb", fmt.Sprintf("%.1f", float64(dr.n)/1e6),
					"speed_mbps", fmt.Sprintf("%.1f", float64(dr.n)/dlElapsed.Seconds()/1e6))
			}
			applyStart := time.Now()
			if err := extractTarZstd(ctx, dr.tmpFile, c.GradleUserHome); err != nil {
				return errors.Wrap(err, "extract delta bundle")
			}
			slog.Info("applied delta bundle", "branch", c.Branch,
				"duration", time.Since(applyStart).Round(time.Millisecond))
		}
	}

	slog.Debug("restore complete", "total_duration", time.Since(totalStart))
	return nil
}

// extractRule maps a tar entry path prefix to a destination base directory.
// For an entry "prefix/rest/of/path", the file is placed at
// filepath.Join(baseDir, "prefix/rest/of/path").
type extractRule struct {
	prefix  string // without leading "./"
	baseDir string
}

// extractBundleZstd decompresses and extracts a base bundle, routing tar
// entries to their final destinations based on rules. Any entry whose path does
// not match a rule is placed under defaultDir. Existing files are not
// overwritten (skipExisting semantics), so a partial pre-existing cache is
// merged rather than replaced.
func extractBundleZstd(ctx context.Context, r io.Reader, rules []extractRule, defaultDir string) error {
	zstdCmd := zstdDecompressCmd(ctx)
	zstdCmd.Stdin = r

	var zstdStderr bytes.Buffer
	zstdCmd.Stderr = &zstdStderr

	zstdOut, err := zstdCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "zstd stdout pipe")
	}

	if err := zstdCmd.Start(); err != nil {
		return errors.Wrap(err, "start zstd")
	}

	targetFn := func(name string) string {
		for _, rule := range rules {
			if strings.HasPrefix(name, rule.prefix) {
				return filepath.Join(rule.baseDir, name)
			}
		}
		return filepath.Join(defaultDir, name)
	}

	extractErr := extractTarPlatformRouted(zstdOut, targetFn, true)
	zstdErr := zstdCmd.Wait()

	var errs []error
	if extractErr != nil {
		errs = append(errs, extractErr)
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

// RestoreDeltaCmd downloads and applies a branch-specific delta bundle on top of
// an already-restored base cache. It must be called after restore, which sets up
// the caches symlink and writes the restore marker.
//
// Delta bundles are keyed by branch name rather than commit SHA, so they survive
// force-pushes and rebases that rewrite the branch tip.
type RestoreDeltaCmd struct {
	backendFlags
	CacheKey       string `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Branch         string `help:"Branch name to look up a delta for (typically $BRANCH_NAME / $GIT_BRANCH)." required:""`
	GradleUserHome string `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
}

func (c *RestoreDeltaCmd) AfterApply() error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if c.Bucket == "" && c.CachewURL == "" {
		return errors.New("one of --bucket or --cachew-url is required")
	}
	if c.Bucket != "" && c.CachewURL != "" {
		return errors.New("--bucket and --cachew-url are mutually exclusive")
	}
	return nil
}

func (c *RestoreDeltaCmd) Run(ctx context.Context) error {
	// Require the base restore to have run first so the caches symlink exists.
	cachesDir := filepath.Join(c.GradleUserHome, "caches")
	if _, err := os.Stat(cachesDir); err != nil {
		return errors.Errorf("caches directory not found at %s — run restore first: %w", cachesDir, err)
	}

	store, err := c.newStore()
	if err != nil {
		return err
	}

	dc := deltaCommit(c.Branch)
	size, err := store.stat(ctx, dc, c.CacheKey)
	if err != nil {
		slog.Info("no delta bundle found for branch", "branch", c.Branch, "cache-key", c.CacheKey)
		return nil
	}
	slog.Info("found delta bundle", "branch", c.Branch, "cache-key", c.CacheKey)

	dlStart := time.Now()
	body, err := store.get(ctx, dc, c.CacheKey, size)
	if err != nil {
		return errors.Wrap(err, "get delta bundle")
	}
	defer body.Close() //nolint:errcheck,gosec

	cb := &countingBody{r: body, dlStart: dlStart}
	// Extract into GradleUserHome: delta entries are ./caches/... paths, so they
	// land under the caches symlink and overlay the base bundle's files.
	if err := extractTarZstd(ctx, cb, c.GradleUserHome); err != nil {
		return errors.Wrap(err, "extract delta bundle")
	}

	if !cb.eofAt.IsZero() {
		dlElapsed := cb.eofAt.Sub(dlStart)
		slog.Info("delta download complete", "duration", dlElapsed.Round(time.Millisecond),
			"size_mb", fmt.Sprintf("%.1f", float64(cb.n)/1e6),
			"speed_mbps", fmt.Sprintf("%.1f", float64(cb.n)/dlElapsed.Seconds()/1e6))
	}
	slog.Info("applied delta bundle", "branch", c.Branch, "cache-key", c.CacheKey,
		"total_duration", time.Since(dlStart).Round(time.Millisecond))
	return nil
}

// SaveCmd archives $GRADLE_USER_HOME/caches and uploads it to the configured backend.
// Also includes configuration-cache and included build output dirs if they exist.
type SaveCmd struct {
	backendFlags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Commit         string   `help:"Commit SHA to tag this bundle with. Defaults to HEAD of --git-dir."`
	GitDir         string   `help:"Path to the git repository (used to resolve HEAD when --commit is not set)." default:"." type:"path"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to archive (relative to project root). Use 'dir/*' to archive build/ for all subdirectories. May be repeated." name:"included-build"`
}

func (c *SaveCmd) AfterApply(ctx context.Context) error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if len(c.IncludedBuilds) == 0 {
		c.IncludedBuilds = []string{"buildSrc"}
	}
	if c.Commit == "" {
		sha, err := gitHead(ctx, c.GitDir)
		if err != nil {
			return errors.Wrap(err, "resolve HEAD commit (pass --commit to override)")
		}
		c.Commit = sha
	}
	if c.Bucket == "" && c.CachewURL == "" {
		return errors.New("one of --bucket or --cachew-url is required")
	}
	if c.Bucket != "" && c.CachewURL != "" {
		return errors.New("--bucket and --cachew-url are mutually exclusive")
	}
	return nil
}

func (c *SaveCmd) Run(ctx context.Context) error {
	cachesDir := filepath.Join(c.GradleUserHome, "caches")
	if _, err := os.Stat(cachesDir); err != nil {
		return errors.Errorf("caches directory not found at %s: %w", cachesDir, err)
	}

	store, err := c.newStore()
	if err != nil {
		return err
	}

	// Skip upload if bundle already exists.
	if _, err := store.stat(ctx, c.Commit, c.CacheKey); err == nil {
		slog.Info("bundle already exists", "commit", c.Commit[:min(8, len(c.Commit))], "cache-key", c.CacheKey)
		return nil
	}

	// Build the list of tar sources: always include caches, plus any
	// configuration-cache and convention build dirs in the current directory.
	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}
	sources := []tarSource{{BaseDir: c.GradleUserHome, Path: "./caches"}}
	sources = append(sources, projectDirSources(projectDir, c.IncludedBuilds)...)

	// Buffer the bundle to a temp file so we have a known Content-Length for upload.
	tmp, err := os.CreateTemp("", "gradle-cache-bundle-*")
	if err != nil {
		return errors.Wrap(err, "create temp file")
	}
	defer func() {
		tmp.Close()           //nolint:errcheck,gosec
		os.Remove(tmp.Name()) //nolint:errcheck,gosec
	}()

	slog.Info("saving bundle", "commit", c.Commit[:min(8, len(c.Commit))], "cache-key", c.CacheKey)
	saveStart := time.Now()

	if err := createTarZstd(ctx, tmp, sources); err != nil {
		return errors.Wrap(err, "create bundle archive")
	}

	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "seek bundle")
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "rewind bundle")
	}

	if err := store.put(ctx, c.Commit, c.CacheKey, tmp, size); err != nil {
		return errors.Wrap(err, "upload bundle")
	}

	elapsed := time.Since(saveStart)
	mbps := float64(size) / elapsed.Seconds() / 1e6
	slog.Info("archive+upload complete", "duration", elapsed,
		"size_mb", fmt.Sprintf("%.1f", float64(size)/1e6),
		"speed_mbps", fmt.Sprintf("%.1f", mbps))
	slog.Info("saved bundle", "commit", c.Commit[:min(8, len(c.Commit))], "cache-key", c.CacheKey)
	return nil
}

// SaveDeltaCmd packs all cache files created since the last restore and uploads
// them as a branch delta bundle. The delta key is based on the branch name rather
// than a commit SHA, so it remains valid after a rebase or force-push.
//
// Because our Go extractor never preserves tar-stored timestamps, all restored
// files get mtime ≈ extraction time. Files created after that — by restore-delta
// or by the build — have strictly newer mtimes, so comparing against the restore
// marker cleanly separates "what was restored" from "what is new". Each save-delta
// is therefore a cumulative snapshot of all files added since the base restore.
type SaveDeltaCmd struct {
	backendFlags
	CacheKey       string `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Branch         string `help:"Branch name to save the delta under (typically $BRANCH_NAME / $GIT_BRANCH)." required:""`
	GradleUserHome string `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
}

func (c *SaveDeltaCmd) AfterApply() error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if c.Bucket == "" && c.CachewURL == "" {
		return errors.New("one of --bucket or --cachew-url is required")
	}
	if c.Bucket != "" && c.CachewURL != "" {
		return errors.New("--bucket and --cachew-url are mutually exclusive")
	}
	return nil
}

func (c *SaveDeltaCmd) Run(ctx context.Context) error {
	// Read the restore marker to establish the mtime baseline.
	markerPath := filepath.Join(c.GradleUserHome, ".cache-restore-marker")
	markerInfo, err := os.Stat(markerPath)
	if err != nil {
		return errors.Errorf("restore marker not found at %s — run restore first: %w", markerPath, err)
	}
	since := markerInfo.ModTime()
	slog.Debug("scanning for new cache files", "since", since.Format(time.RFC3339Nano))

	// Resolve the caches symlink so filepath.Walk descends into the real directory.
	cachesDir := filepath.Join(c.GradleUserHome, "caches")
	realCaches, err := filepath.EvalSymlinks(cachesDir)
	if err != nil {
		// Not a symlink or doesn't exist — use as-is and let the walk fail naturally.
		realCaches = cachesDir
	}

	// Collect files with mtime strictly after the restore marker.
	scanStart := time.Now()
	newFiles, err := collectNewFiles(realCaches, since, c.GradleUserHome)
	if err != nil {
		return errors.Wrap(err, "walk caches dir")
	}
	slog.Debug("cache scan complete",
		"duration", time.Since(scanStart).Round(time.Millisecond),
		"new_files", len(newFiles))

	if len(newFiles) == 0 {
		slog.Info("no new cache files since restore, skipping delta save")
		return nil
	}

	store, err := c.newStore()
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp("", "gradle-cache-delta-*")
	if err != nil {
		return errors.Wrap(err, "create temp file")
	}
	defer func() {
		tmp.Close()           //nolint:errcheck,gosec
		os.Remove(tmp.Name()) //nolint:errcheck,gosec
	}()

	dc := deltaCommit(c.Branch)
	slog.Info("saving delta bundle", "branch", c.Branch, "cache-key", c.CacheKey, "files", len(newFiles))
	saveStart := time.Now()

	if err := createDeltaTarZstd(ctx, tmp, c.GradleUserHome, newFiles); err != nil {
		return errors.Wrap(err, "create delta archive")
	}

	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "seek delta bundle")
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "rewind delta bundle")
	}

	if err := store.put(ctx, dc, c.CacheKey, tmp, size); err != nil {
		return errors.Wrap(err, "upload delta bundle")
	}

	elapsed := time.Since(saveStart)
	mbps := float64(size) / elapsed.Seconds() / 1e6
	slog.Info("delta bundle saved",
		"branch", c.Branch, "cache-key", c.CacheKey,
		"duration", elapsed.Round(time.Millisecond),
		"size_mb", fmt.Sprintf("%.1f", float64(size)/1e6),
		"speed_mbps", fmt.Sprintf("%.1f", mbps))
	return nil
}

// projectDirSources returns tarSource entries for project-specific dirs:
// configuration-cache (from projectDir/.gradle/) and included build output dirs,
// for any that exist on disk.
func projectDirSources(projectDir string, includedBuilds []string) []tarSource {
	var sources []tarSource

	// configuration-cache is archived at ./configuration-cache/ (not .gradle/configuration-cache/)
	// so that restore can symlink it to a different location (projectDir/.gradle/).
	gradleDir := filepath.Join(projectDir, ".gradle")
	if _, err := os.Stat(filepath.Join(gradleDir, "configuration-cache")); err == nil {
		sources = append(sources, tarSource{BaseDir: gradleDir, Path: "./configuration-cache"})
	}

	// Included build output dirs relative to projectDir.
	for _, rel := range conventionBuildDirs(projectDir, includedBuilds) {
		sources = append(sources, tarSource{BaseDir: projectDir, Path: "./" + rel})
	}

	return sources
}

func main() {
	cli := &CLI{}
	ctx := context.Background()
	kctx := kong.Parse(cli,
		kong.UsageOnError(),
		kong.HelpOptions{Compact: true},
		kong.BindTo(ctx, (*context.Context)(nil)), // needed by SaveCmd.AfterApply
	)
	setupLogger(cli.LogLevel)
	kctx.FatalIfErrorf(kctx.Run(ctx))
}

// setupLogger configures the global slog logger at the requested level.
// Timestamps are omitted — CI systems capture their own.
func setupLogger(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default: // "info"
		l = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: l,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{} // omit timestamp
			}
			return a
		},
	})
	slog.SetDefault(slog.New(handler))
}

// bundleFilename converts a cache key to its S3 filename, matching the Ruby bundled-cache-manager.
func bundleFilename(cacheKey string) string {
	return strings.ReplaceAll(cacheKey, ":", "-") + ".tar.zst"
}

// s3Key builds the S3 object key for a given commit, cache key, and bundle filename.
func s3Key(commit, cacheKey, bundleFile string) string {
	return commit + "/" + cacheKey + "/" + bundleFile
}

// tarSource specifies a (base directory, relative path) pair for inclusion in a tar archive.
type tarSource struct {
	BaseDir string
	Path    string
}

// historyCommits runs git log from the given ref and returns commit SHAs within
// maxBlocks distinct-author "blocks".
func historyCommits(ctx context.Context, gitDir, ref string, maxBlocks int) ([]string, error) {
	rawCount := maxBlocks * 10
	//nolint:gosec // ref is a user-supplied git ref, not a shell injection vector
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "log", "--first-parent",
		fmt.Sprintf("-n%d", rawCount), "--format=%H\t%an", ref)
	out, err := cmd.Output()
	if err != nil {
		return nil, errors.Errorf("git log: %w", err)
	}

	var commits []string
	prevAuthor := ""
	blocksSeen := 0

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) != 2 {
			continue
		}
		sha, author := parts[0], parts[1]
		if author != prevAuthor {
			blocksSeen++
			prevAuthor = author
			if blocksSeen > maxBlocks {
				break
			}
		}
		commits = append(commits, sha)
	}
	return commits, errors.Wrap(scanner.Err(), "scan git log")
}

// gitHead returns the SHA of HEAD in the given git directory.
func gitHead(ctx context.Context, gitDir string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "rev-parse", "HEAD") //nolint:gosec
	out, err := cmd.Output()
	if err != nil {
		return "", errors.Errorf("git rev-parse HEAD: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// zstdDecompressArgs returns the command + args for zstd decompression.
// Prefers pzstd (parallel, same format as pzstd-compressed bundles) and
// falls back to zstd -dc -TN.
func zstdDecompressCmd(ctx context.Context) *exec.Cmd {
	n := strconv.Itoa(max(1, runtime.NumCPU()))
	if path, err := exec.LookPath("pzstd"); err == nil {
		// -d decompress, -p N = N threads, -c write to stdout
		return exec.CommandContext(ctx, path, "-d", "-p", n, "-c") //nolint:gosec
	}
	return exec.CommandContext(ctx, "zstd", "-dc", "-T"+n) //nolint:gosec
}

// extractTarZstd decompresses a zstd-compressed tar archive from r into dir.
// pzstd/zstd decompresses in parallel; the resulting tar stream is extracted
// by extractTarGo (pooled-buffer parallel writer) or piped to system tar as
// a fallback when building without CGO on platforms where tar is unavailable.
// countingBody wraps an io.Reader, counts bytes consumed, and records the time
// at which the underlying reader returns io.EOF (i.e. when the last S3 byte
// was consumed by the downstream pipeline).
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

func extractTarZstd(ctx context.Context, r io.Reader, dir string) error {
	zstdCmd := zstdDecompressCmd(ctx)
	zstdCmd.Stdin = r

	var zstdStderr bytes.Buffer
	zstdCmd.Stderr = &zstdStderr

	zstdOut, err := zstdCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "zstd stdout pipe")
	}

	if err := zstdCmd.Start(); err != nil {
		return errors.Wrap(err, "start zstd")
	}

	extractErr := extractTarPlatform(zstdOut, dir)
	zstdErr := zstdCmd.Wait()

	var errs []error
	if extractErr != nil {
		errs = append(errs, extractErr)
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

// zstdCompressCmd returns the command for zstd compression.
// Prefers pzstd (creates parallel frames, decompressable in parallel) and
// falls back to zstd -TN -c.
func zstdCompressCmd(ctx context.Context) *exec.Cmd {
	n := strconv.Itoa(max(1, runtime.NumCPU()))
	if path, err := exec.LookPath("pzstd"); err == nil {
		// -p N = N threads, -c write to stdout
		return exec.CommandContext(ctx, path, "-p", n, "-c") //nolint:gosec
	}
	return exec.CommandContext(ctx, "zstd", "-T"+n, "-c") //nolint:gosec
}

// cacheExclusions are patterns for files and directories that should never be
// included in cache bundles. Patterns with a leading * are suffix-matched;
// all others are exact basename matches.
var cacheExclusions = []string{
	"daemon",
	".tmp",
	"gc.properties",
	"*.lock",
}

// isExcludedCache reports whether a file or directory name matches any cache exclusion pattern.
func isExcludedCache(name string) bool {
	for _, pat := range cacheExclusions {
		if strings.HasPrefix(pat, "*") {
			if strings.HasSuffix(name, pat[1:]) {
				return true
			}
		} else if name == pat {
			return true
		}
	}
	return false
}

// createTarZstd creates a zstd-compressed tar archive from the given sources and
// writes it to w. Uses -h to dereference symlinks.
// Multiple sources map to multiple -C baseDir path entries in the tar command,
// which is how we combine caches + configuration-cache + convention build dirs into a single flat
// archive.
func createTarZstd(ctx context.Context, w io.Writer, sources []tarSource) error {
	args := []string{"-chf", "-"}
	for _, pat := range cacheExclusions {
		args = append(args, "--exclude", pat)
	}
	for _, src := range sources {
		args = append(args, "-C", src.BaseDir, src.Path)
	}
	tarCmd := exec.CommandContext(ctx, "tar", args...) //nolint:gosec
	zstdCmd := zstdCompressCmd(ctx)

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "tar stdout pipe")
	}

	var tarStderr, zstdStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr
	zstdCmd.Stdin = tarStdout
	zstdCmd.Stdout = w
	zstdCmd.Stderr = &zstdStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Wrap(err, "start tar")
	}
	if err := zstdCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "start zstd"), tarCmd.Wait())
	}

	tarErr := tarCmd.Wait()
	zstdErr := zstdCmd.Wait()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar: %w: %s", tarErr, tarStderr.String()))
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

// collectNewFiles walks realCaches in parallel using os.ReadDir and returns paths
// of regular files with mtime strictly after since. Returned paths are relative to
// gradleHome, e.g. "caches/modules-2/…", so they can be passed directly to writeDeltaTar.
//
// Parallel ReadDir goroutines overlap directory-entry I/O latency for a ~4× speedup
// over a sequential filepath.Walk on a cold filesystem (benchmarked at 3.4 s vs 13–15 s
// for 212 k files on APFS). 8 workers is the empirical sweet spot before B-tree
// contention degrades throughput; workers is capped at min(8, NumCPU).
//
// The "early release" semaphore pattern prevents the classic tree-walk deadlock:
// each goroutine releases its slot immediately after ReadDir returns, before
// trying to acquire slots for its child directories.
func collectNewFiles(realCaches string, since time.Time, gradleHome string) ([]string, error) {
	workers := min(8, runtime.NumCPU())
	sem := make(chan struct{}, workers)

	var mu sync.Mutex
	var allFiles []string
	var firstErr error
	var wg sync.WaitGroup

	var walk func(dir, rel string)
	walk = func(dir, rel string) {
		defer wg.Done()

		entries, err := os.ReadDir(dir)
		<-sem // release slot immediately after I/O, before spawning children

		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			return
		}

		var localFiles []string
		for _, entry := range entries {
			name := entry.Name()
			childRel := name
			if rel != "" {
				childRel = rel + "/" + name
			}
			if entry.IsDir() {
				if isExcludedCache(name) {
					continue
				}
				sem <- struct{}{} // acquire slot for child after releasing ours
				wg.Add(1)
				go walk(filepath.Join(dir, name), childRel)
			} else if entry.Type().IsRegular() {
				if isExcludedCache(name) {
					continue
				}
				if fi, err := entry.Info(); err == nil && fi.ModTime().After(since) {
					localFiles = append(localFiles, filepath.Join("caches", childRel))
				}
			}
		}
		if len(localFiles) > 0 {
			mu.Lock()
			allFiles = append(allFiles, localFiles...)
			mu.Unlock()
		}
	}

	sem <- struct{}{}
	wg.Add(1)
	go walk(realCaches, "")
	wg.Wait()
	return allFiles, firstErr
}

// branchSlug converts a branch name into a safe, flat string for use as an S3 key
// segment. Slashes become "--" (to avoid creating unintended S3 path hierarchy while
// preserving branch-name semantics), and any other URL-unsafe characters become "-".
//
// Examples:
//
//	"main"              → "main"
//	"feature/my-pr"     → "feature--my-pr"
//	"fix/JIRA-123"      → "fix--JIRA-123"
func branchSlug(branch string) string {
	s := strings.ReplaceAll(branch, "/", "--")
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// deltaCommit returns the pseudo-commit identifier used to store and retrieve
// branch delta bundles via the bundleStore interface. The "branches/" prefix
// keeps delta objects in a distinct namespace from regular commit bundles.
func deltaCommit(branch string) string {
	return "branches/" + branchSlug(branch)
}

// touchMarkerFile creates or truncates the file at path, updating its mtime to now.
// The parent directory is created if it does not already exist.
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

// createDeltaTarZstd creates a zstd-compressed tar archive containing the files at
// relPaths (relative to baseDir) and writes it to w. It uses Go's archive/tar rather
// than the system tar command because the file list is already resolved to real paths
// (the caches-dir symlink has been followed by filepath.Walk + EvalSymlinks), so
// symlink dereferencing with -h is not required.
func createDeltaTarZstd(ctx context.Context, w io.Writer, baseDir string, relPaths []string) error {
	zstdCmd := zstdCompressCmd(ctx)

	pr, pw := io.Pipe()
	zstdCmd.Stdin = pr
	zstdCmd.Stdout = w
	var zstdStderr bytes.Buffer
	zstdCmd.Stderr = &zstdStderr

	if err := zstdCmd.Start(); err != nil {
		return errors.Wrap(err, "start zstd")
	}

	// Write the tar stream into the pipe concurrently with zstd compression.
	tarErr := writeDeltaTar(pw, baseDir, relPaths)
	pw.CloseWithError(tarErr) //nolint:errcheck,gosec

	zstdErr := zstdCmd.Wait()

	var errs []error
	if tarErr != nil {
		errs = append(errs, tarErr)
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

// writeDeltaTar writes a tar stream for the specified files to w.
// relPaths are relative to baseDir (e.g. "caches/modules-2/…"); the archive
// preserves these paths so the bundle can be extracted into GradleUserHome.
// Non-regular files and files that have disappeared since the scan are silently skipped.
func writeDeltaTar(w io.Writer, baseDir string, relPaths []string) error {
	tw := tar.NewWriter(w)
	for _, rel := range relPaths {
		absPath := filepath.Join(baseDir, rel)
		fi, err := os.Lstat(absPath)
		if os.IsNotExist(err) {
			continue // disappeared between scan and pack
		}
		if err != nil {
			return errors.Errorf("stat %s: %w", rel, err)
		}
		if !fi.Mode().IsRegular() {
			continue // skip symlinks, directories, etc.
		}

		hdr, err := tar.FileInfoHeader(fi, "")
		if err != nil {
			return errors.Errorf("tar header for %s: %w", rel, err)
		}
		hdr.Name = rel // store the GradleUserHome-relative path

		if err := tw.WriteHeader(hdr); err != nil {
			return errors.Errorf("write tar header %s: %w", rel, err)
		}

		f, err := os.Open(absPath)
		if err != nil {
			return errors.Errorf("open %s: %w", rel, err)
		}
		_, copyErr := io.Copy(tw, f)
		f.Close() //nolint:errcheck,gosec
		if copyErr != nil {
			return errors.Errorf("copy %s: %w", rel, copyErr)
		}
	}
	return tw.Close()
}

// conventionBuildDirs returns the relative paths of included build output directories
// (i.e. <dir>/build) that exist within root, based on the includedBuilds configuration.
//
// Each entry in includedBuilds is a directory path relative to root. If the entry ends
// with "/*", all immediate subdirectories of the parent are scanned and any that contain
// a build/ subdirectory are included. Otherwise, <entry>/build is checked directly.
//
// Example values: "buildSrc", "build-logic", "plugins/*"
func conventionBuildDirs(root string, includedBuilds []string) []string {
	var result []string
	for _, entry := range includedBuilds {
		if strings.HasSuffix(entry, "/*") {
			// Scan all immediate subdirectories of the parent for a build/ subdir.
			parent := strings.TrimSuffix(entry, "/*")
			entries, err := os.ReadDir(filepath.Join(root, parent))
			if err != nil {
				continue
			}
			for _, sub := range entries {
				if !sub.IsDir() {
					continue
				}
				rel := parent + "/" + sub.Name() + "/build"
				if info, err := os.Stat(filepath.Join(root, rel)); err == nil && info.IsDir() {
					result = append(result, rel)
				}
			}
		} else {
			rel := entry + "/build"
			if info, err := os.Stat(filepath.Join(root, rel)); err == nil && info.IsDir() {
				result = append(result, rel)
			}
		}
	}
	return result
}
