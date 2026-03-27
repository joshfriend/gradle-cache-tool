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
	"strings"
	"sync"
	"time"

	"github.com/DataDog/zstd"
	"github.com/alecthomas/errors"
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

	var archiveErr error
	go func() {
		archiveErr = CreateTarZstd(ctx, pw, sources)
		pw.CloseWithError(archiveErr) //nolint:errcheck,gosec
	}()

	size, err := store.putStream(ctx, cfg.Commit, cfg.CacheKey, pr)
	pr.Close() //nolint:errcheck,gosec
	if archiveErr != nil {
		return errors.Wrap(archiveErr, "create bundle archive")
	}
	if err != nil {
		return errors.Wrap(err, "upload bundle")
	}

	elapsed := time.Since(saveStart)
	mbps := float64(size) / elapsed.Seconds() / 1e6
	log.Info("archive+upload complete", "duration", elapsed,
		"size_mb", fmt.Sprintf("%.1f", float64(size)/1e6),
		"speed_mbps", fmt.Sprintf("%.1f", mbps))
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
// included in cache bundles.
var CacheExclusions = []string{
	"daemon",
	".tmp",
	"gc.properties",
	"*.lock",
	"cc-keystore",
}

const wrapperZipExclusion = "wrapper/dists/*/*/*.zip"

// IsExcludedCache reports whether a file or directory name matches any cache exclusion pattern.
func IsExcludedCache(name string) bool {
	for _, pat := range CacheExclusions {
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
func CreateTarZstd(ctx context.Context, w io.Writer, sources []TarSource) error {
	args := []string{"-chf", "-"}
	for _, pat := range CacheExclusions {
		args = append(args, "--exclude", pat)
	}
	args = append(args, "--exclude", wrapperZipExclusion)
	for _, src := range sources {
		args = append(args, "-C", src.BaseDir, src.Path)
	}
	tarCmd := exec.CommandContext(ctx, "tar", args...) //nolint:gosec

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "tar stdout pipe")
	}

	var tarStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Wrap(err, "start tar")
	}

	enc := zstd.NewWriter(w)

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
	enc := zstd.NewWriter(w)

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

// CollectNewFiles walks realCaches in parallel and returns paths of regular files
// with mtime strictly after since.
func CollectNewFiles(realCaches string, since time.Time, gradleHome string) ([]string, error) {
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
				if IsExcludedCache(name) {
					continue
				}
				sem <- struct{}{}
				wg.Add(1)
				go walk(filepath.Join(dir, name), childRel)
			} else if entry.Type().IsRegular() {
				if IsExcludedCache(name) {
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
