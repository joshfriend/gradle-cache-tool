package gradlecache

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/klauspost/compress/zstd"
)

// RestoreDeltaConfig holds the parameters for a delta restore operation.
type RestoreDeltaConfig struct {
	Bucket         string
	Region         string
	CachewURL      string
	KeyPrefix      string
	CacheKey       string
	Branch         string
	GradleUserHome string
	ProjectDir     string   // optional: route project-dir entries to their correct locations
	IncludedBuilds []string // included build directories (for convention plugin outputs)
	Metrics        MetricsClient
	Logger         *slog.Logger
}

func (c *RestoreDeltaConfig) defaults() {
	if c.Region == "" {
		c.Region = defaultRegion()
	}
	if c.GradleUserHome == "" {
		home, _ := os.UserHomeDir()
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if c.ProjectDir == "" {
		wd, err := os.Getwd()
		if err == nil {
			c.ProjectDir = wd
		}
	}
	if c.Metrics == nil {
		c.Metrics = NoopMetrics{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// RestoreDelta downloads and applies a branch-specific delta bundle on top of
// an already-restored base cache.
func RestoreDelta(ctx context.Context, cfg RestoreDeltaConfig) error {
	cfg.defaults()
	log := cfg.Logger

	cachesDir := filepath.Join(cfg.GradleUserHome, "caches")
	if _, err := os.Stat(cachesDir); err != nil {
		return errors.Errorf("caches directory not found at %s — run restore first: %w", cachesDir, err)
	}

	store, err := newStore(cfg.Bucket, cfg.Region, cfg.CachewURL, cfg.KeyPrefix)
	if err != nil {
		return err
	}

	dc := deltaCommit(cfg.Branch)
	deltaInfo, err := store.stat(ctx, dc, cfg.CacheKey)
	if err != nil {
		log.Info("no delta bundle found for branch", "branch", cfg.Branch, "cache-key", cfg.CacheKey)
		return nil
	}
	log.Info("found delta bundle", "branch", cfg.Branch, "cache-key", cfg.CacheKey)

	dlStart := time.Now()
	body, err := store.get(ctx, dc, cfg.CacheKey, deltaInfo)
	if err != nil {
		return errors.Wrap(err, "get delta bundle")
	}
	defer body.Close() //nolint:errcheck,gosec

	var pdSources []TarSource
	if cfg.ProjectDir != "" {
		pdSources = ProjectDirSources(cfg.ProjectDir, cfg.IncludedBuilds)
	}

	cb := &countingBody{r: body, dlStart: dlStart}
	if err := extractDeltaTarZstd(ctx, cb, cfg.GradleUserHome, pdSources); err != nil {
		return errors.Wrap(err, "extract delta bundle")
	}

	if !cb.eofAt.IsZero() {
		dlElapsed := cb.eofAt.Sub(dlStart)
		log.Info("delta download complete", "duration", dlElapsed.Round(time.Millisecond),
			"size_mb", fmt.Sprintf("%.1f", float64(cb.n)/1e6),
			"speed_mbps", fmt.Sprintf("%.1f", float64(cb.n)/dlElapsed.Seconds()/1e6))
	}
	deltaElapsed := time.Since(dlStart)
	log.Info("applied delta bundle", "branch", cfg.Branch, "cache-key", cfg.CacheKey,
		"total_duration", deltaElapsed.Round(time.Millisecond))
	cfg.Metrics.Distribution("gradle_cache.restore_delta.duration_ms", float64(deltaElapsed.Milliseconds()),
		"cache_key:"+cfg.CacheKey)
	cfg.Metrics.Distribution("gradle_cache.restore_delta.size_bytes", float64(cb.n),
		"cache_key:"+cfg.CacheKey)
	if !cb.eofAt.IsZero() {
		dlElapsed := cb.eofAt.Sub(dlStart)
		mbps := float64(cb.n) / dlElapsed.Seconds() / 1e6
		cfg.Metrics.Distribution("gradle_cache.restore_delta.speed_mbps", mbps,
			"cache_key:"+cfg.CacheKey)
	}
	return nil
}

// SaveConfig holds the parameters for a cache save operation.
type SaveConfig struct {
	Bucket         string
	Region         string
	CachewURL      string
	KeyPrefix      string
	CacheKey       string
	Commit         string
	GitDir         string
	GradleUserHome string
	IncludedBuilds []string
	SkipWarm       bool // skip page cache warming (for benchmarking cold baseline)
	Metrics        MetricsClient
	Logger         *slog.Logger
}

func (c *SaveConfig) defaults() {
	if c.Region == "" {
		c.Region = defaultRegion()
	}
	if c.GitDir == "" {
		c.GitDir = "."
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

// Save archives GRADLE_USER_HOME/caches and uploads it to the configured backend.
func Save(ctx context.Context, cfg SaveConfig) error {
	cfg.defaults()
	log := cfg.Logger

	if cfg.Commit == "" {
		sha, err := gitHead(ctx, cfg.GitDir)
		if err != nil {
			return errors.Wrap(err, "resolve HEAD commit (pass Commit to override)")
		}
		cfg.Commit = sha
	}
	if !IsFullSHA(cfg.Commit) {
		return errors.Errorf("Commit must be a full 40-character hex SHA, got %q", cfg.Commit)
	}

	cachesDir := filepath.Join(cfg.GradleUserHome, "caches")
	if _, err := os.Stat(cachesDir); err != nil {
		return errors.Errorf("caches directory not found at %s: %w", cachesDir, err)
	}

	store, err := newStore(cfg.Bucket, cfg.Region, cfg.CachewURL, cfg.KeyPrefix)
	if err != nil {
		return err
	}

	if _, err = store.stat(ctx, cfg.Commit, cfg.CacheKey); err == nil {
		log.Info("bundle already exists", "commit", cfg.Commit[:min(8, len(cfg.Commit))], "cache-key", cfg.CacheKey)
		return nil
	}

	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}
	if err := validateProjectDir(projectDir); err != nil {
		return err
	}
	sources := []TarSource{{BaseDir: cfg.GradleUserHome, Path: "./caches"}}
	if fi, err := os.Stat(filepath.Join(cfg.GradleUserHome, "wrapper")); err == nil && fi.IsDir() {
		sources = append(sources, TarSource{BaseDir: cfg.GradleUserHome, Path: "./wrapper"})
	}
	sources = append(sources, ProjectDirSources(projectDir, cfg.IncludedBuilds)...)

	pr, pw := io.Pipe()

	log.Info("saving bundle", "commit", cfg.Commit[:min(8, len(cfg.Commit))], "cache-key", cfg.CacheKey)

	if !cfg.SkipWarm {
		log.Debug("warming page cache")
		warmStart := time.Now()
		warmPageCache(ctx, sources)
		log.Debug("page cache warm", "duration", time.Since(warmStart).Round(time.Millisecond))
	} else {
		log.Debug("skipping page cache warm (SkipWarm=true)")
	}

	saveStart := time.Now()

	// Wrap the archive→upload boundary to measure upload wait time.
	uploadTiming := &timingReader{r: pr}

	var archiveErr error
	go func() {
		archiveErr = CreateTarZstd(ctx, pw, sources)
		pw.CloseWithError(archiveErr) //nolint:errcheck,gosec
	}()

	size, err := store.putStream(ctx, cfg.Commit, cfg.CacheKey, uploadTiming)
	pr.Close() //nolint:errcheck,gosec
	if archiveErr != nil {
		return errors.Wrap(archiveErr, "create bundle archive")
	}
	if err != nil {
		return errors.Wrap(err, "upload bundle")
	}

	elapsed := time.Since(saveStart)
	mbps := float64(size) / elapsed.Seconds() / 1e6

	// Upload pipeline: disk read (tar) → compress (zstd) → upload (S3).
	// uploadTiming.blocked = time S3 upload spent waiting for compressed bytes
	// (i.e. archive busy time). elapsed - uploadTiming.blocked = upload busy time.
	archiveBusy := uploadTiming.blocked
	uploadBusy := elapsed - archiveBusy
	var bottleneck string
	if archiveBusy > uploadBusy {
		bottleneck = "archive"
	} else {
		bottleneck = "upload"
	}

	attrs := []any{
		"duration", elapsed.Round(time.Millisecond),
		"size_mb", fmt.Sprintf("%.1f", float64(size)/1e6),
		"speed_mbps", fmt.Sprintf("%.1f", mbps),
		"bottleneck", bottleneck,
	}
	if archiveBusy > 0 {
		attrs = append(attrs, "archive_mbps", fmt.Sprintf("%.1f", float64(size)/archiveBusy.Seconds()/1e6))
	}
	if uploadBusy > 0 {
		attrs = append(attrs, "upload_mbps", fmt.Sprintf("%.1f", float64(size)/uploadBusy.Seconds()/1e6))
	}
	log.Info("archive+upload complete", attrs...)

	log.Info("saved bundle", "commit", cfg.Commit[:min(8, len(cfg.Commit))], "cache-key", cfg.CacheKey)
	cfg.Metrics.Distribution("gradle_cache.save.duration_ms", float64(elapsed.Milliseconds()), "cache_key:"+cfg.CacheKey)
	cfg.Metrics.Distribution("gradle_cache.save.size_bytes", float64(size), "cache_key:"+cfg.CacheKey)
	cfg.Metrics.Distribution("gradle_cache.save.speed_mbps", mbps, "cache_key:"+cfg.CacheKey)
	return nil
}

// SaveDeltaConfig holds the parameters for a delta save operation.
type SaveDeltaConfig struct {
	Bucket         string
	Region         string
	CachewURL      string
	KeyPrefix      string
	CacheKey       string
	Branch         string
	GradleUserHome string
	ProjectDir     string   // optional: scan project-dir sources for new entries
	IncludedBuilds []string // included build directories (for convention plugin outputs)
	Metrics        MetricsClient
	Logger         *slog.Logger
}

func (c *SaveDeltaConfig) defaults() {
	if c.Region == "" {
		c.Region = defaultRegion()
	}
	if c.GradleUserHome == "" {
		home, _ := os.UserHomeDir()
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if c.ProjectDir == "" {
		wd, err := os.Getwd()
		if err == nil {
			c.ProjectDir = wd
		}
	}
	if c.Metrics == nil {
		c.Metrics = NoopMetrics{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// SaveDelta packs all cache files created since the last restore and uploads
// them as a branch delta bundle.
func SaveDelta(ctx context.Context, cfg SaveDeltaConfig) error {
	cfg.defaults()
	log := cfg.Logger

	if err := validateProjectDir(cfg.ProjectDir); err != nil {
		return err
	}

	markerPath := filepath.Join(cfg.GradleUserHome, ".cache-restore-marker")
	markerInfo, err := os.Stat(markerPath)
	if err != nil {
		log.Info("no restore marker found, skipping delta save (no base restore was performed)")
		return nil
	}
	since := markerInfo.ModTime()
	log.Debug("scanning for new cache files", "since", since.Format(time.RFC3339Nano))

	cachesDir := filepath.Join(cfg.GradleUserHome, "caches")
	realCaches, err := filepath.EvalSymlinks(cachesDir)
	if err != nil {
		realCaches = cachesDir
	}

	scanStart := time.Now()
	newFiles, err := CollectNewFiles(realCaches, since, cfg.GradleUserHome)
	if err != nil {
		return errors.Wrap(err, "walk caches dir")
	}
	log.Debug("cache scan complete",
		"duration", time.Since(scanStart).Round(time.Millisecond),
		"new_files", len(newFiles))

	// Collect new files from project-dir sources (configuration-cache, included builds).
	var projectSources []DeltaSource
	if cfg.ProjectDir != "" {
		for _, src := range ProjectDirSources(cfg.ProjectDir, cfg.IncludedBuilds) {
			newProjectFiles := collectNewFilesSimple(
				filepath.Join(src.BaseDir, src.Path), src.BaseDir, since)
			if len(newProjectFiles) > 0 {
				log.Debug("project source scan complete", "path", src.Path, "new_files", len(newProjectFiles))
				projectSources = append(projectSources, DeltaSource{
					BaseDir: src.BaseDir, RelPaths: newProjectFiles,
				})
			}
		}
	}

	projectFileCount := 0
	for _, s := range projectSources {
		projectFileCount += len(s.RelPaths)
	}
	if len(newFiles) == 0 && projectFileCount == 0 {
		log.Info("no new cache files since restore, skipping delta save")
		return nil
	}

	store, err := newStore(cfg.Bucket, cfg.Region, cfg.CachewURL, cfg.KeyPrefix)
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

	totalFiles := len(newFiles) + projectFileCount
	dc := deltaCommit(cfg.Branch)
	log.Info("saving delta bundle", "branch", cfg.Branch, "cache-key", cfg.CacheKey, "files", totalFiles)
	saveStart := time.Now()

	sources := append([]DeltaSource{{BaseDir: cfg.GradleUserHome, RelPaths: newFiles}}, projectSources...)
	if err := CreateDeltaTarZstdMulti(tmp, sources...); err != nil {
		return errors.Wrap(err, "create delta archive")
	}

	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "seek delta bundle")
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "rewind delta bundle")
	}

	if err := store.put(ctx, dc, cfg.CacheKey, tmp, size); err != nil {
		return errors.Wrap(err, "upload delta bundle")
	}

	elapsed := time.Since(saveStart)
	mbps := float64(size) / elapsed.Seconds() / 1e6
	log.Info("delta bundle saved",
		"branch", cfg.Branch, "cache-key", cfg.CacheKey,
		"duration", elapsed.Round(time.Millisecond),
		"size_mb", fmt.Sprintf("%.1f", float64(size)/1e6),
		"speed_mbps", fmt.Sprintf("%.1f", mbps))
	cfg.Metrics.Distribution("gradle_cache.save_delta.duration_ms", float64(elapsed.Milliseconds()),
		"cache_key:"+cfg.CacheKey)
	cfg.Metrics.Distribution("gradle_cache.save_delta.size_bytes", float64(size),
		"cache_key:"+cfg.CacheKey)
	cfg.Metrics.Distribution("gradle_cache.save_delta.speed_mbps", mbps,
		"cache_key:"+cfg.CacheKey)
	return nil
}

// ── Tar / archive helpers ───────────────────────────────────────────────────

// TarSource specifies a (base directory, relative path) pair for inclusion in a tar archive.
type TarSource struct {
	BaseDir string
	Path    string
}

// CacheExclusions are patterns for files and directories that should never be
// included in cache bundles (base or delta).
var CacheExclusions = []string{
	// daemon/ contains per-daemon state (registry, logs, pid files) that is tied
	// to a running Gradle daemon process. Daemons are never reused across CI runs,
	// so this directory is pure waste in the bundle.
	"daemon",

	// .tmp/ holds intermediate temp files created during builds. They are
	// irrelevant after the build finishes and would only inflate the archive.
	".tmp",

	// gc.properties records the last time Gradle ran its own cache cleanup GC. On
	// ephemeral CI workers this timestamp is meaningless and Gradle recreates the
	// file when needed.
	"gc.properties",

	// *.lock files are advisory filesystem locks held by running Gradle processes.
	// They are stale after the build and would cause spurious "already locked"
	// errors if restored on a different machine.
	"*.lock",

	// cc-keystore holds a randomly generated AES key used to encrypt configuration
	// cache entries. The key is unique to each machine, so restoring one machine's
	// keystore onto another causes decryption failures for any existing CC entries.
	// Gradle generates a fresh keystore (and re-encrypts) when the file is missing.
	// If the GRADLE_ENCRYPTION_KEY env var is set, Gradle uses that shared key
	// instead of the keystore, making this file unused entirely.
	"cc-keystore",

	// file-changes/ contains only last-build.bin, a 1-byte marker whose mtime
	// records when the previous build finished. Gradle uses it to distrust file
	// timestamps that match the previous build's end time, forcing a full content
	// hash instead. After tar extraction every file shares the same mtime, so
	// including this marker causes Gradle to needlessly rehash thousands of files.
	// Without it Gradle sets lastBuildTimestamp=0 and trusts all mtimes — strictly
	// better for restored caches.
	"file-changes",

	// journal-1/ holds file-access.bin, an indexed cache that maps absolute File
	// paths to last-access timestamps for Gradle's cache GC (evicts entries not
	// used in 30 days). The absolute-path keys are wrong on any machine with a
	// different workspace path, and cache GC is irrelevant on ephemeral CI workers.
	// Gradle recreates it on first use with inceptionTimestamp=now, so all entries
	// appear recently accessed and nothing gets prematurely evicted.
	"journal-1",

	// user-id.txt is a persistent random UUID identifying the Gradle user, written
	// once and never changed. Bundling it overwrites every CI worker's identity
	// with the bundle creator's UUID. Gradle generates a new one if missing.
	"user-id.txt",
}

// isHexHash returns true if s is a non-empty string of hex characters (0-9, a-f).
func isHexHash(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !isHex {
			return false
		}
	}
	return true
}

// DeltaExclusions are additional file/directory names excluded only from delta
// bundles. These files are already present in the base bundle and get rewritten
// every build (Gradle's embedded BTree DB flushes on close even for read-only
// access), so the delta copy adds negligible value at significant size cost.
var DeltaExclusions = []string{
	// fileHashes/ is an indexed cache mapping absolute file paths to (hash, length,
	// mtime) tuples, letting Gradle skip re-hashing unchanged files. The base
	// bundle already provides the bulk of entries, and the DB is rewritten on every
	// build close even for read-only access (BTree compaction), so it always
	// appears "new". The incremental entries from a single build aren't worth the
	// transfer cost.
	"fileHashes",

	// module-metadata.bin is the dependency resolution metadata cache, mapping
	// (repositoryId, moduleComponentId) to parsed POM/module metadata. Portable
	// across machines but rewritten on every build due to DB compaction. The base
	// bundle already has it and a single build rarely adds new dependencies.
	"module-metadata.bin",
}

// wrapperZipExclusion excludes the downloaded Gradle distribution zip from
// wrapper/dists/. After extraction Gradle only needs the unpacked distribution
// directory; the zip is kept for offline re-extraction but is never read during
// normal builds. It's typically 100–150 MB and easily re-downloaded if needed.
const wrapperZipExclusion = "wrapper/dists/*/*/*.zip"

// IsExcludedCache reports whether a file or directory name matches any cache exclusion pattern.
func IsExcludedCache(name string) bool {
	return matchesAny(name, CacheExclusions)
}

// IsDeltaExcluded reports whether a name matches delta-only exclusion patterns.
func IsDeltaExcluded(name string) bool {
	return matchesAny(name, DeltaExclusions)
}

func matchesAny(name string, patterns []string) bool {
	for _, pat := range patterns {
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

// warmPageCache issues POSIX_FADV_WILLNEED on every regular file under each
// TarSource in parallel, hinting the kernel to start async readahead before
// tar reads them sequentially. On cold NVMe storage with many small files
// (e.g. 200K Gradle cache entries), tar is limited to ~80 MB/s by per-file
// IOPS overhead. Warming the cache saturates IOPS up front so that tar
// subsequently reads at memory speed (~1300 MB/s). On non-Linux platforms,
// falls back to reading files into the page cache directly.
func warmPageCache(ctx context.Context, sources []TarSource) {
	concurrency := min(runtime.GOMAXPROCS(0)*2, 32)
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, src := range sources {
		root := filepath.Join(src.BaseDir, src.Path)
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if ctx.Err() != nil {
				return filepath.SkipAll
			}
			if err != nil {
				return nil
			}
			if d.IsDir() {
				if IsExcludedCache(d.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if !d.Type().IsRegular() || IsExcludedCache(d.Name()) {
				return nil
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				adviseWillNeed(path)
			}()
			return nil
		})
	}
	wg.Wait()
}

// CreateTarZstd creates a zstd-compressed tar archive from the given sources.
// If pzstd is available it is used to produce a multi-frame archive that can
// be decompressed in parallel on restore. Otherwise klauspost is used.
func CreateTarZstd(ctx context.Context, w io.Writer, sources []TarSource) error {
	tarArgs := []string{"-chf", "-"}
	for _, pat := range CacheExclusions {
		tarArgs = append(tarArgs, "--exclude", pat)
	}
	tarArgs = append(tarArgs, "--exclude", wrapperZipExclusion)
	for _, src := range sources {
		tarArgs = append(tarArgs, "-C", src.BaseDir, src.Path)
	}

	if pzstdPath, err := exec.LookPath("pzstd"); err == nil {
		slog.Debug("using pzstd multi-frame encoder", "path", pzstdPath)
		return createTarPzstd(ctx, w, tarArgs, pzstdPath)
	}

	slog.Debug("using klauspost single-frame encoder")
	return createTarKlauspost(ctx, w, tarArgs)
}

// createTarPzstd pipes tar output through pzstd to produce a multi-frame zstd
// archive. An OS-level pipe connects the two processes directly without
// buffering through Go.
func createTarPzstd(ctx context.Context, w io.Writer, tarArgs []string, pzstdPath string) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "create tar-pzstd pipe")
	}

	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...) //nolint:gosec
	tarCmd.Stdout = pw
	var tarStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr

	procs := strconv.Itoa(runtime.GOMAXPROCS(0))
	pzstdCmd := exec.CommandContext(ctx, pzstdPath, "-p", procs, "-c") //nolint:gosec
	pzstdCmd.Stdin = pr
	pzstdCmd.Stdout = w
	var pzstdStderr bytes.Buffer
	pzstdCmd.Stderr = &pzstdStderr

	if err := tarCmd.Start(); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return errors.Wrap(err, "start tar")
	}
	_ = pw.Close() // parent no longer needs write end

	if err := pzstdCmd.Start(); err != nil {
		pr.Close()    //nolint:errcheck,gosec
		tarCmd.Wait() //nolint:errcheck,gosec
		return errors.Wrap(err, "start pzstd")
	}
	_ = pr.Close() // parent no longer needs read end

	pzstdWaitErr := pzstdCmd.Wait()
	tarWaitErr := tarCmd.Wait()

	var errs []error
	if tarWaitErr != nil {
		errs = append(errs, errors.Errorf("tar: %w: %s", tarWaitErr, tarStderr.String()))
	}
	if pzstdWaitErr != nil {
		errs = append(errs, errors.Errorf("pzstd: %w: %s", pzstdWaitErr, pzstdStderr.String()))
	}
	return errors.Join(errs...)
}

// createTarKlauspost pipes tar output through the klauspost zstd encoder.
func createTarKlauspost(ctx context.Context, w io.Writer, tarArgs []string) error {
	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...) //nolint:gosec
	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "tar stdout pipe")
	}
	var tarStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr
	if err := tarCmd.Start(); err != nil {
		return errors.Wrap(err, "start tar")
	}

	enc, err := zstd.NewWriter(w, zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return errors.Join(errors.Wrap(err, "create zstd encoder"), tarCmd.Wait())
	}

	_, copyErr := io.Copy(enc, tarStdout)
	encErr := enc.Close()
	tarStdout.Close() //nolint:errcheck,gosec
	tarErr := tarCmd.Wait()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar: %w: %s", tarErr, tarStderr.String()))
	}
	if copyErr != nil {
		errs = append(errs, errors.Wrap(copyErr, "compress stream"))
	}
	if encErr != nil {
		errs = append(errs, errors.Wrap(encErr, "close zstd encoder"))
	}
	return errors.Join(errs...)
}

// extractDeltaTarZstd extracts a delta bundle. When projectDirSources is
// non-empty, entries matching those prefixes are routed to their respective
// base directories; everything else goes to gradleUserHome.
func extractDeltaTarZstd(ctx context.Context, r io.Reader, gradleUserHome string, projectDirSources []TarSource) error {
	if len(projectDirSources) == 0 {
		return extractTarZstd(ctx, r, gradleUserHome)
	}
	br := bufio.NewReaderSize(r, 8<<20)
	dec, err := zstd.NewReader(br, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return errors.Wrap(err, "create zstd decoder")
	}
	defer dec.Close()

	// Build routing rules from project-dir sources.
	type route struct {
		prefix  string
		baseDir string
	}
	var routes []route
	for _, src := range projectDirSources {
		// src.Path is like "./configuration-cache" — strip leading "./"
		prefix := strings.TrimPrefix(src.Path, "./") + "/"
		routes = append(routes, route{prefix: prefix, baseDir: src.BaseDir})
	}

	targetFn := func(name string) string {
		for _, r := range routes {
			if strings.HasPrefix(name, r.prefix) {
				return filepath.Join(r.baseDir, name)
			}
		}
		return filepath.Join(gradleUserHome, name)
	}
	if err := extractTarPlatformRouted(dec, targetFn, false); err != nil {
		return err
	}
	return drainCompressedReader(br)
}

// collectNewFilesSimple walks dir for regular files with mtime after since,
// returning paths relative to baseDir.
func collectNewFilesSimple(dir, baseDir string, since time.Time) []string {
	var files []string
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.ModTime().After(since) {
			rel, _ := filepath.Rel(baseDir, path)
			files = append(files, rel)
		}
		return nil
	})
	return files
}

// DeltaSource groups files from a single base directory for delta archiving.
type DeltaSource struct {
	BaseDir  string
	RelPaths []string
}

// CreateDeltaTarZstd creates a zstd-compressed tar archive containing the files at
// relPaths (relative to baseDir).
func CreateDeltaTarZstd(_ context.Context, w io.Writer, baseDir string, relPaths []string) error {
	return CreateDeltaTarZstdMulti(w, DeltaSource{BaseDir: baseDir, RelPaths: relPaths})
}

// CreateDeltaTarZstdMulti creates a zstd-compressed tar archive from multiple sources.
func CreateDeltaTarZstdMulti(w io.Writer, sources ...DeltaSource) error {
	enc, err := zstd.NewWriter(w,
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return errors.Wrap(err, "create zstd encoder")
	}

	tarErr := WriteDeltaTarMulti(enc, sources...)
	encErr := enc.Close()

	return errors.Join(tarErr, encErr)
}

// WriteDeltaTar writes a tar stream for the specified files to w.
func WriteDeltaTar(w io.Writer, baseDir string, relPaths []string) error {
	return WriteDeltaTarMulti(w, DeltaSource{BaseDir: baseDir, RelPaths: relPaths})
}

// WriteDeltaTarMulti writes a tar stream for files from multiple source directories.
func WriteDeltaTarMulti(w io.Writer, sources ...DeltaSource) error {
	tw := tar.NewWriter(w)
	for _, src := range sources {
		for _, rel := range src.RelPaths {
			if err := writeDeltaTarEntry(tw, src.BaseDir, rel); err != nil {
				return err
			}
		}
	}
	return tw.Close()
}

func writeDeltaTarEntry(tw *tar.Writer, baseDir, rel string) error {
	absPath := filepath.Join(baseDir, rel)
	fi, err := os.Lstat(absPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return errors.Errorf("stat %s: %w", rel, err)
	}
	if !fi.Mode().IsRegular() {
		return nil
	}

	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return errors.Errorf("tar header for %s: %w", rel, err)
	}
	hdr.Name = rel

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
	return nil
}

// AtomicCacheParents lists directory names whose descendants include
// hash-keyed workspace directories that must be captured atomically.
// The walker recurses through intermediate subdirectories until it finds
// children whose names look like hex hashes, then treats those as atomic
// units: if ANY file inside has mtime > marker, ALL files are included.
//
// These directories contain hash-keyed subdirectories where Gradle expects
// all files to be present together. Some are true immutable workspaces
// (transforms, groovy-dsl, kotlin-dsl, dependencies-accessors) created via
// atomic directory renames. Others are persistent caches with receipt-based
// completion markers (jars-9). In both cases, a partial directory (e.g.
// metadata.bin without output files, or a .receipt without the transformed
// classes) causes Gradle to crash.
//
// Mtime skew across delta cycles can produce partial directories: after
// restoring base + delta, the base provides output files (old mtime) while
// the delta overwrites metadata/receipt files (new mtime). A per-file mtime
// check captures only the newer files, producing a partial directory in the
// next delta. When applied to a base that lacks the hash, Gradle crashes.
var AtomicCacheParents = map[string]bool{
	"transforms":             true,
	"groovy-dsl":             true,
	"kotlin-dsl":             true,
	"dependencies-accessors": true,
	"jars-9":                 true,
}

// CollectNewFiles walks realCaches in parallel and returns paths of regular files
// with mtime strictly after since. For directories listed in AtomicCacheParents,
// if any file in a child hash directory is newer than since, all files in that
// directory are included to prevent partial restores.
func CollectNewFiles(realCaches string, since time.Time, gradleHome string) ([]string, error) {
	workers := min(8, runtime.GOMAXPROCS(0))
	sem := make(chan struct{}, workers)

	var mu sync.Mutex
	var allFiles []string
	var firstErr error
	var wg sync.WaitGroup

	// collectAll recursively collects ALL regular files under dir, regardless of mtime.
	collectAll := func(dir, rel string) []string {
		var files []string
		_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				if IsExcludedCache(d.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if d.Type().IsRegular() && !IsExcludedCache(d.Name()) {
				childRel, _ := filepath.Rel(dir, path)
				files = append(files, filepath.Join("caches", rel, childRel))
			}
			return nil
		})
		return files
	}

	// walkWorkspaceParent handles directories like transforms/ whose children
	// are atomic workspace directories. It auto-detects whether children are
	// hash-keyed workspaces (hex names) or intermediate directories (like
	// kotlin-dsl/scripts/). For intermediates, it recurses; for hash dirs,
	// it checks if any file is new and captures all files atomically.
	var walkWorkspaceParent func(dir, rel string)
	walkWorkspaceParent = func(dir, rel string) {
		defer wg.Done()

		entries, err := os.ReadDir(dir)
		<-sem

		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			return
		}

		for _, entry := range entries {
			if !entry.IsDir() || IsExcludedCache(entry.Name()) {
				continue
			}
			childDir := filepath.Join(dir, entry.Name())
			childRel := rel + "/" + entry.Name()

			if !isHexHash(entry.Name()) {
				// Intermediate directory (e.g. kotlin-dsl/scripts/) — recurse.
				sem <- struct{}{}
				wg.Add(1)
				go walkWorkspaceParent(childDir, childRel)
				continue
			}

			hasNew := false
			_ = filepath.WalkDir(childDir, func(_ string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				if fi, e := d.Info(); e == nil && fi.ModTime().After(since) {
					hasNew = true
					return filepath.SkipAll
				}
				return nil
			})

			if hasNew {
				files := collectAll(childDir, childRel)
				// Skip workspace stubs that contain only metadata files
				// (metadata.bin, results.bin, *.receipt) with no actual
				// build output. These stubs provide no caching value and
				// can create broken workspace dirs when applied to a base
				// that lacks the workspace hash.
				hasOutput := false
				for _, f := range files {
					base := filepath.Base(f)
					if base != "metadata.bin" && base != "results.bin" &&
						filepath.Ext(base) != ".receipt" {
						hasOutput = true
						break
					}
				}
				if hasOutput {
					mu.Lock()
					allFiles = append(allFiles, files...)
					mu.Unlock()
				}
			}
		}
	}

	var walk func(dir, rel string)
	walk = func(dir, rel string) {
		defer wg.Done()

		entries, err := os.ReadDir(dir)
		<-sem

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
				if IsExcludedCache(name) || IsDeltaExcluded(name) {
					continue
				}
				if AtomicCacheParents[name] {
					sem <- struct{}{}
					wg.Add(1)
					go walkWorkspaceParent(filepath.Join(dir, name), childRel)
				} else {
					sem <- struct{}{}
					wg.Add(1)
					go walk(filepath.Join(dir, name), childRel)
				}
			} else if entry.Type().IsRegular() {
				if IsExcludedCache(name) || IsDeltaExcluded(name) {
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

// ProjectDirSources returns tarSource entries for project-specific dirs.
func ProjectDirSources(projectDir string, includedBuilds []string) []TarSource {
	var sources []TarSource

	gradleDir := filepath.Join(projectDir, ".gradle")
	if _, err := os.Stat(filepath.Join(gradleDir, "configuration-cache")); err == nil {
		sources = append(sources, TarSource{BaseDir: gradleDir, Path: "./configuration-cache"})
	}

	for _, rel := range ConventionBuildDirs(projectDir, includedBuilds) {
		sources = append(sources, TarSource{BaseDir: projectDir, Path: "./" + rel})
	}

	return sources
}

func validateProjectDir(projectDir string) error {
	if projectDir == "" {
		return errors.New("project directory is required")
	}
	dotGradle := filepath.Join(projectDir, ".gradle")
	info, err := os.Stat(dotGradle)
	if err != nil {
		return errors.Errorf("project directory %q is missing .gradle/: %w", projectDir, err)
	}
	if !info.IsDir() {
		return errors.Errorf("project directory %q has non-directory .gradle path", projectDir)
	}
	return nil
}

// ConventionBuildDirs returns the relative paths of included build output directories.
func ConventionBuildDirs(root string, includedBuilds []string) []string {
	var result []string
	for _, entry := range includedBuilds {
		if strings.HasSuffix(entry, "/*") {
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

// IsFullSHA returns true if s is a 40-character lowercase hex string.
func IsFullSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func gitHead(ctx context.Context, gitDir string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "rev-parse", "HEAD") //nolint:gosec
	out, err := cmd.Output()
	if err != nil {
		return "", errors.Errorf("git rev-parse HEAD: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
