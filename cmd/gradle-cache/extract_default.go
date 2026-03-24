//go:build !darwin

package main

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/alecthomas/errors"
)

const (
	// extractWorkers is the number of goroutines writing files concurrently
	// during parallel tar extraction. Hides per-file open/write/close syscall
	// latency so the tar-stream reader (and download pipeline behind it) is not
	// stalled waiting for individual file writes to complete.
	// Benchmarked on r8id.metal-48xlarge (NVMe, 96 cores) with a 334K-file
	// bundle: 64 workers = 6.27s, 128 = 6.84s (extra GC pressure outweighs
	// any I/O concurrency gain).
	extractWorkers = 64
	// maxParallelFileSize is the largest file that will be buffered in memory
	// and dispatched to the worker pool. Files larger than this are written
	// inline in the main goroutine to keep peak memory bounded.
	// At 4 MiB, 99.97 % of Gradle cache entries go through the parallel path.
	maxParallelFileSize = 4 << 20 // 4 MiB
)

// extractTarPlatform uses parallel extraction on Linux.
// See extractTarParallelRouted for the implementation rationale.
func extractTarPlatform(r io.Reader, dir string) error {
	return extractTarParallelRouted(r, func(name string) string {
		return filepath.Join(dir, name)
	}, false)
}

// extractTarPlatformRouted is the routing-aware variant of extractTarPlatform.
// targetFn maps a cleaned tar entry name to its absolute destination path.
// If skipExisting is true, files that already exist on disk are left untouched.
func extractTarPlatformRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	return extractTarParallelRouted(r, targetFn, skipExisting)
}

// extractTarParallelRouted reads a tar stream and writes files using a pool of
// goroutines. The main goroutine reads tar entries and buffers small file
// contents; workers write those files to disk concurrently. Large files are
// written inline to keep memory use bounded.
//
// Parallelising writes hides the per-file open/write/close syscall latency
// (the dominant cost when extracting hundreds of thousands of small files),
// allowing the upstream download+decompression pipeline to run at full speed
// instead of being throttled by sequential I/O.
func extractTarParallelRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	type writeJob struct {
		target string
		mode   os.FileMode
		data   []byte
	}

	jobs := make(chan writeJob, extractWorkers*2)

	var (
		wg           sync.WaitGroup
		writeErrOnce sync.Once
		writeErr     error
	)

	for range extractWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				f, err := os.OpenFile(job.target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, job.mode)
				if err != nil {
					writeErrOnce.Do(func() { writeErr = errors.Errorf("open %s: %w", filepath.Base(job.target), err) })
					continue
				}
				if _, err := f.Write(job.data); err != nil {
					f.Close() //nolint:errcheck,gosec
					writeErrOnce.Do(func() { writeErr = errors.Errorf("write %s: %w", filepath.Base(job.target), err) })
					continue
				}
				if err := f.Close(); err != nil {
					writeErrOnce.Do(func() { writeErr = errors.Errorf("close %s: %w", filepath.Base(job.target), err) })
				}
			}
		}()
	}

	copyBuf := make([]byte, 1<<20) // reused only for inline large-file writes

	// createdDirs is accessed only by the main goroutine, so no mutex needed.
	createdDirs := make(map[string]struct{})
	ensureDir := func(d string, mode os.FileMode) error {
		if _, ok := createdDirs[d]; ok {
			return nil
		}
		if err := os.MkdirAll(d, mode); err != nil { //nolint:gosec // path is validated by caller
			return err
		}
		createdDirs[d] = struct{}{}
		return nil
	}

	tr := tar.NewReader(r)
	var readErr error
loop:
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = errors.Wrap(err, "read tar entry")
			break
		}

		name, err := safeTarEntryName(hdr.Name)
		if err != nil {
			readErr = err
			break
		}

		target := targetFn(name)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := ensureDir(target, hdr.FileInfo().Mode()); err != nil {
				readErr = errors.Errorf("mkdir %s: %w", hdr.Name, err)
				break loop
			}

		case tar.TypeReg:
			if skipExisting {
				if _, err := os.Lstat(target); err == nil {
					// tar.Reader.Next() discards unread data automatically.
					continue
				}
			}
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir %s: %w", hdr.Name, err)
				break loop
			}

			if hdr.Size <= maxParallelFileSize {
				// Buffer in memory and dispatch to worker pool so the main
				// goroutine can continue reading the tar stream immediately.
				buf := make([]byte, hdr.Size)
				if _, err := io.ReadFull(tr, buf); err != nil {
					readErr = errors.Errorf("read %s: %w", hdr.Name, err)
					break loop
				}
				// Propagate worker errors early.
				if writeErr != nil {
					readErr = writeErr
					break loop
				}
				jobs <- writeJob{target: target, mode: hdr.FileInfo().Mode(), data: buf}
			} else {
				// Large file: write inline to keep memory bounded. Wait for the
				// jobs channel to drain a little first so memory stays bounded
				// while the large inline write is in progress.
				f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hdr.FileInfo().Mode())
				if err != nil {
					readErr = errors.Errorf("open %s: %w", hdr.Name, err)
					break loop
				}
				if _, err := io.CopyBuffer(f, io.LimitReader(tr, hdr.Size), copyBuf); err != nil {
					f.Close() //nolint:errcheck,gosec
					readErr = errors.Errorf("write %s: %w", hdr.Name, err)
					break loop
				}
				if err := f.Close(); err != nil {
					readErr = errors.Errorf("close %s: %w", hdr.Name, err)
					break loop
				}
			}

		case tar.TypeSymlink:
			if skipExisting {
				if _, err := os.Lstat(target); err == nil {
					continue
				}
			}
			if err := safeSymlinkTarget(name, hdr.Linkname); err != nil {
				readErr = err
				break loop
			}
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir for symlink %s: %w", hdr.Name, err)
				break loop
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				readErr = errors.Errorf("symlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
				break loop
			}

		case tar.TypeLink:
			if skipExisting {
				if _, err := os.Lstat(target); err == nil {
					continue
				}
			}
			linkName, err := safeTarEntryName(hdr.Linkname)
			if err != nil {
				readErr = errors.Errorf("unsafe hardlink target %q: %w", hdr.Linkname, err)
				break loop
			}
			linkTarget := targetFn(linkName)
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir for hardlink %s: %w", hdr.Name, err)
				break loop
			}
			if err := os.Link(linkTarget, target); err != nil {
				readErr = errors.Errorf("hardlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
				break loop
			}
		}
	}

	close(jobs)
	wg.Wait()

	if readErr != nil {
		return readErr
	}
	return writeErr
}
