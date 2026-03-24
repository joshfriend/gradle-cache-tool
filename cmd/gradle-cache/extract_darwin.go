package main

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	"github.com/alecthomas/errors"
)

// extractBufPool is a pool of reusable byte-slice pointers used by extractTarGo
// on macOS. Reusing slices eliminates per-file heap allocations for the parallel
// write path. Initial capacity is 256 KiB — large enough for most Gradle cache
// files without needing a separate allocation.
var extractBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 256<<10)
		return &b
	},
}

// mmapThreshold is the minimum file size above which ftruncate+mmap+memcpy is
// faster than write() on macOS APFS. Below this threshold, mmap setup overhead
// exceeds the savings. 64 KB covers most Gradle .jar files.
const mmapThreshold = 64 * 1024

// extractTarPlatform uses the optimised parallel extractor on macOS. APFS
// handles concurrent writes to independent files efficiently, so parallel
// workers saturate IOPS better than a single sequential writer would.
func extractTarPlatform(r io.Reader, dir string) error {
	return extractTarGoRouted(r, func(name string) string {
		return filepath.Join(dir, name)
	}, false)
}

// extractTarPlatformRouted is the routing-aware variant of extractTarPlatform.
// targetFn maps a cleaned tar entry name to its absolute destination path.
// If skipExisting is true, files that already exist on disk are left untouched.
func extractTarPlatformRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	return extractTarGoRouted(r, targetFn, skipExisting)
}

// extractTarGoRouted is the core parallel extractor. targetFn maps a cleaned
// tar entry name (e.g. "caches/8.14.3/foo") to its absolute destination path.
// skipExisting skips writing files that already exist on disk.
func extractTarGoRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	type fileJob struct {
		path string
		mode os.FileMode
		buf  *[]byte // must be returned to extractBufPool after use
	}

	numWorkers := max(16, runtime.NumCPU())
	jobs := make(chan fileJob, numWorkers*2)

	var workerErrs []error
	var mu sync.Mutex
	var wg sync.WaitGroup

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := writeFileMacos(job.path, *job.buf, job.mode); err != nil {
					mu.Lock()
					workerErrs = append(workerErrs, err)
					mu.Unlock()
				}
				extractBufPool.Put(job.buf)
			}
		}()
	}

	// createdDirs tracks directories the reader goroutine has already ensured
	// exist. Workers never call MkdirAll, so APFS B-tree updates from directory
	// creation are serialised through the reader and don't contend with writes.
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
readLoop:
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = errors.Wrap(err, "read tar entry")
			break
		}

		// Reject path traversal before passing to targetFn.
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
				break readLoop
			}

		case tar.TypeReg:
			if skipExisting {
				if _, err := os.Lstat(target); err == nil {
					continue
				}
			}
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir %s: %w", hdr.Name, err)
				break readLoop
			}
			bufPtr := extractBufPool.Get().(*[]byte)
			if int64(cap(*bufPtr)) >= hdr.Size {
				*bufPtr = (*bufPtr)[:hdr.Size]
			} else {
				*bufPtr = make([]byte, hdr.Size)
			}
			if _, err := io.ReadFull(tr, *bufPtr); err != nil {
				extractBufPool.Put(bufPtr)
				readErr = errors.Errorf("read %s: %w", hdr.Name, err)
				break readLoop
			}
			jobs <- fileJob{path: target, mode: hdr.FileInfo().Mode(), buf: bufPtr}

		case tar.TypeSymlink:
			if skipExisting {
				if _, err := os.Lstat(target); err == nil {
					continue
				}
			}
			if err := safeSymlinkTarget(name, hdr.Linkname); err != nil {
				readErr = err
				break readLoop
			}
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir for symlink %s: %w", hdr.Name, err)
				break readLoop
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				readErr = errors.Errorf("symlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
				break readLoop
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
				break readLoop
			}
			linkTarget := targetFn(linkName)
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir for hardlink %s: %w", hdr.Name, err)
				break readLoop
			}
			if err := os.Link(linkTarget, target); err != nil {
				readErr = errors.Errorf("hardlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
				break readLoop
			}
		}
	}

	close(jobs)
	wg.Wait()

	var allErrs []error
	if readErr != nil {
		allErrs = append(allErrs, readErr)
	}
	allErrs = append(allErrs, workerErrs...)
	return errors.Join(allErrs...)
}

// writeFileMacos writes data to path. Files >= mmapThreshold use ftruncate +
// mmap + memcpy: APFS allocates one contiguous extent from the truncate call
// and the Mach VM subsystem writes pages lazily, reducing per-page syscall
// overhead compared to write(). Smaller files use write() directly since mmap
// setup cost exceeds the savings for short payloads.
func writeFileMacos(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return errors.Errorf("open %s: %w", path, err)
	}
	defer f.Close() //nolint:errcheck

	if len(data) >= mmapThreshold {
		if tErr := f.Truncate(int64(len(data))); tErr == nil {
			if mapped, mErr := syscall.Mmap(int(f.Fd()), 0, len(data), syscall.PROT_WRITE, syscall.MAP_SHARED); mErr == nil {
				copy(mapped, data)
				if uErr := syscall.Munmap(mapped); uErr != nil {
					return errors.Errorf("munmap %s: %w", path, uErr)
				}
				return nil
			}
		}
		// Truncate or mmap failed — fall through to write().
	}

	if _, err := f.Write(data); err != nil {
		return errors.Errorf("write %s: %w", path, err)
	}
	return nil
}
