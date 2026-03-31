package gradlecache

import (
	"archive/tar"
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
	"github.com/klauspost/compress/zstd"
)

// RestoreDeltaConfig holds the parameters for a delta restore operation.
type RestoreDeltaConfig struct {
	Bucket         string
	Region         string
	CachewURL      string
	CacheKey       string
	Branch         string
	GradleUserHome string
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

	store, err := newStore(cfg.Bucket, cfg.Region, cfg.CachewURL)
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

	cb := &countingBody{r: body, dlStart: dlStart}
	if err := extractTarZstd(ctx, cb, cfg.GradleUserHome); err != nil {
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
	CacheKey       string
	Commit         string
	GitDir         string
	GradleUserHome string
	IncludedBuilds []string
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

	store, err := newStore(cfg.Bucket, cfg.Region, cfg.CachewURL)
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
	sources := []TarSource{{BaseDir: cfg.GradleUserHome, Path: "./caches"}}
	if fi, err := os.Stat(filepath.Join(cfg.GradleUserHome, "wrapper")); err == nil && fi.IsDir() {
		sources = append(sources, TarSource{BaseDir: cfg.GradleUserHome, Path: "./wrapper"})
	}
	sources = append(sources, ProjectDirSources(projectDir, cfg.IncludedBuilds)...)

	pr, pw := io.Pipe()

	log.Info("saving bundle", "commit", cfg.Commit[:min(8, len(cfg.Commit))], "cache-key", cfg.CacheKey)
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
	CacheKey       string
	Branch         string
	GradleUserHome string
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

	markerPath := filepath.Join(cfg.GradleUserHome, ".cache-restore-marker")
	markerInfo, err := os.Stat(markerPath)
	if err != nil {
		return errors.Errorf("restore marker not found at %s — run restore first: %w", markerPath, err)
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

	if len(newFiles) == 0 {
		log.Info("no new cache files since restore, skipping delta save")
		return nil
	}

	store, err := newStore(cfg.Bucket, cfg.Region, cfg.CachewURL)
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

	dc := deltaCommit(cfg.Branch)
	log.Info("saving delta bundle", "branch", cfg.Branch, "cache-key", cfg.CacheKey, "files", len(newFiles))
	saveStart := time.Now()

	if err := CreateDeltaTarZstd(ctx, tmp, cfg.GradleUserHome, newFiles); err != nil {
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
		pr.Close() //nolint:errcheck,gosec
		pw.Close() //nolint:errcheck,gosec
		return errors.Wrap(err, "start tar")
	}
	pw.Close() // parent no longer needs write end //nolint:errcheck,gosec

	if err := pzstdCmd.Start(); err != nil {
		pr.Close()    //nolint:errcheck,gosec
		tarCmd.Wait() //nolint:errcheck,gosec
		return errors.Wrap(err, "start pzstd")
	}
	pr.Close() // parent no longer needs read end //nolint:errcheck,gosec

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

// CreateDeltaTarZstd creates a zstd-compressed tar archive containing the files at
// relPaths (relative to baseDir).
func CreateDeltaTarZstd(_ context.Context, w io.Writer, baseDir string, relPaths []string) error {
	enc, err := zstd.NewWriter(w,
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return errors.Wrap(err, "create zstd encoder")
	}

	tarErr := WriteDeltaTar(enc, baseDir, relPaths)
	encErr := enc.Close()

	return errors.Join(tarErr, encErr)
}

// WriteDeltaTar writes a tar stream for the specified files to w.
func WriteDeltaTar(w io.Writer, baseDir string, relPaths []string) error {
	tw := tar.NewWriter(w)
	for _, rel := range relPaths {
		absPath := filepath.Join(baseDir, rel)
		fi, err := os.Lstat(absPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return errors.Errorf("stat %s: %w", rel, err)
		}
		if !fi.Mode().IsRegular() {
			continue
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
	}
	return tw.Close()
}

// ImmutableWorkspaceParents lists directory names whose immediate children are
// immutable workspace directories (e.g. transforms/<hash>/, groovy-dsl/<hash>/).
//
// Each workspace is an atomic unit: Gradle creates all files together via an
// atomic directory rename, and expects all files to be present when reading.
// However, mtime skew can occur across delta cycles: after restoring base +
// delta, the base provides output files (old mtime, before the marker) while
// the delta overwrites metadata files like metadata.bin and results.bin (new
// mtime, after the marker). A naive per-file mtime check would capture only
// the metadata files, producing a partial workspace in the next delta. When
// that partial delta is applied to a different base that lacks the workspace
// hash, Gradle crashes with "Could not read workspace metadata".
//
// The fix: when ANY file in a workspace is newer than the marker, include ALL
// files from that workspace in the delta.
var ImmutableWorkspaceParents = map[string]bool{
	"transforms": true,
	"groovy-dsl": true,
	"kotlin-dsl": true,
}

// CollectNewFiles walks realCaches in parallel and returns paths of regular files
// with mtime strictly after since. For directories listed in ImmutableWorkspaceParents,
// if any file in a child workspace is newer than since, all files in that workspace
// are included to prevent partial restores.
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
	// are atomic workspace directories. For each child, if any file is newer
	// than since, all files are included.
	walkWorkspaceParent := func(dir, rel string) {
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

			hasNew := false
			filepath.WalkDir(childDir, func(_ string, d os.DirEntry, err error) error {
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
				if len(files) > 0 {
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
				if ImmutableWorkspaceParents[name] {
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
