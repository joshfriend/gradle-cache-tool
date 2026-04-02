package gradlecache

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// TrimConfig holds the parameters for a cache trim operation.
type TrimConfig struct {
	GradleUserHome string
	Metrics        MetricsClient
	Logger         *slog.Logger
}

func (c *TrimConfig) defaults() {
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

// TrimRestoredFiles removes cache entries that were restored from a bundle but
// never accessed during the subsequent build. It relies on atime: the extract
// step sets atime to epoch for all restored files, and on filesystems with
// relatime (the Linux default) the OS updates atime on first read. Files whose
// atime is still at epoch after the build were never read and are safe to remove.
//
// For atomic workspace directories (transforms, jars-9, etc.) the entire
// workspace is kept if ANY file inside was accessed.
func TrimRestoredFiles(cfg TrimConfig) error {
	cfg.defaults()
	log := cfg.Logger

	if !hasRelatime(cfg.GradleUserHome) {
		log.Info("filesystem does not support relatime, skipping trim")
		return nil
	}

	cachesDir := filepath.Join(cfg.GradleUserHome, "caches")
	realCaches, err := filepath.EvalSymlinks(cachesDir)
	if err != nil {
		realCaches = cachesDir
	}
	if _, err := os.Stat(realCaches); err != nil {
		log.Info("no caches directory found, skipping trim")
		return nil
	}

	scanStart := time.Now()
	var trimmed, kept atomic.Int64

	workers := min(8, runtime.GOMAXPROCS(0))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	// trimWorkspaceParent handles atomic workspace parent dirs. If no file
	// in a hex-keyed child was read during the build, the whole child is removed.
	var trimWorkspaceParent func(dir string)
	trimWorkspaceParent = func(dir string) {
		defer wg.Done()
		entries, err := os.ReadDir(dir)
		<-sem
		if err != nil {
			return
		}
		for _, entry := range entries {
			if !entry.IsDir() || IsExcludedCache(entry.Name()) {
				continue
			}
			childDir := filepath.Join(dir, entry.Name())

			if !isHexHash(entry.Name()) {
				sem <- struct{}{}
				wg.Add(1)
				go trimWorkspaceParent(childDir)
				continue
			}

			accessed := hasAccessedFile(childDir)
			if !accessed {
				if err := os.RemoveAll(childDir); err != nil {
					log.Debug("failed to remove stale workspace", "path", childDir, "error", err)
				} else {
					trimmed.Add(1)
				}
			} else {
				kept.Add(1)
			}
		}
	}

	// walkDir trims regular files and dispatches atomic workspace parents.
	var walkDir func(dir string)
	walkDir = func(dir string) {
		defer wg.Done()
		entries, err := os.ReadDir(dir)
		<-sem
		if err != nil {
			setErr(err)
			return
		}

		for _, entry := range entries {
			name := entry.Name()
			if IsExcludedCache(name) {
				continue
			}

			childPath := filepath.Join(dir, name)

			if entry.IsDir() {
				if AtomicCacheParents[name] {
					sem <- struct{}{}
					wg.Add(1)
					go trimWorkspaceParent(childPath)
				} else {
					sem <- struct{}{}
					wg.Add(1)
					go walkDir(childPath)
				}
				continue
			}

			if !entry.Type().IsRegular() {
				continue
			}

			if fileAtime(childPath).Before(time.Unix(1, 0)) {
				if err := os.Remove(childPath); err == nil {
					trimmed.Add(1)
				}
			} else {
				kept.Add(1)
			}
		}
	}

	sem <- struct{}{}
	wg.Add(1)
	go walkDir(realCaches)
	wg.Wait()

	cleanEmptyDirs(realCaches)

	t := trimmed.Load()
	k := kept.Load()
	log.Info("trim complete",
		"duration", time.Since(scanStart).Round(time.Millisecond),
		"removed", t,
		"kept", k)
	cfg.Metrics.Distribution("gradle_cache.trim.removed", float64(t))
	cfg.Metrics.Distribution("gradle_cache.trim.kept", float64(k))
	return firstErr
}

// hasAccessedFile returns true if any regular file under dir has atime after epoch.
func hasAccessedFile(dir string) bool {
	accessed := false
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if fileAtime(path).After(time.Unix(1, 0)) {
			accessed = true
			return filepath.SkipAll
		}
		return nil
	})
	return accessed
}

// hasRelatime probes whether the filesystem backing dir supports relatime by
// creating a temp file, setting its atime to epoch, reading it, and checking
// whether atime was updated.
func hasRelatime(dir string) bool {
	f, err := os.CreateTemp(dir, ".relatime-probe-*")
	if err != nil {
		return false
	}
	path := f.Name()
	defer os.Remove(path) //nolint:errcheck,gosec

	// Write some data so the read has something to access.
	_, _ = f.Write([]byte("probe"))
	_ = f.Close()

	// Set atime to epoch, mtime to epoch.
	if err := os.Chtimes(path, epoch, epoch); err != nil {
		return false
	}

	// Read the file — on relatime this should update atime.
	rf, err := os.Open(path)
	if err != nil {
		return false
	}
	_, _ = io.ReadAll(rf)
	_ = rf.Close()

	atime := fileAtime(path)
	return atime.After(time.Unix(1, 0))
}

// cleanEmptyDirs removes empty directories bottom-up under root.
func cleanEmptyDirs(root string) {
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		os.Remove(dirs[i]) //nolint:errcheck,gosec
	}
}
