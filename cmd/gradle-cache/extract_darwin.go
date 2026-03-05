package main

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	return extractTarGo(r, dir)
}

// extractTarGo reads a tar stream and extracts it into dir using a Go worker
// pool. Two macOS/APFS-specific optimisations over the naive implementation:
//
//  1. Directory creation is hoisted into the reader goroutine and tracked in a
//     seen-set so each unique parent directory is MkdirAll'd exactly once.
//     Workers never call MkdirAll, eliminating concurrent B-tree contention on
//     APFS directory metadata.
//
//  2. Large files (>= mmapThreshold) are written via ftruncate + mmap + memcpy
//     instead of write(). ftruncate tells APFS the final size upfront so it
//     allocates one contiguous extent; the Mach VM subsystem then writes pages
//     lazily via fault-in rather than blocking on each write() call.
func extractTarGo(r io.Reader, dir string) error {
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
	ensureDir := func(d string) error {
		if _, ok := createdDirs[d]; ok {
			return nil
		}
		if err := os.MkdirAll(d, 0o750); err != nil {
			return err
		}
		createdDirs[d] = struct{}{}
		return nil
	}

	tr := tar.NewReader(r)
	cleanDir := filepath.Clean(dir) + string(os.PathSeparator)
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

		target := filepath.Join(dir, hdr.Name)
		if !strings.HasPrefix(filepath.Clean(target)+string(os.PathSeparator), cleanDir) {
			readErr = errors.Errorf("tar entry %q escapes destination directory", hdr.Name)
			break
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := ensureDir(target); err != nil {
				readErr = errors.Errorf("mkdir %s: %w", hdr.Name, err)
				break readLoop
			}

		case tar.TypeReg:
			if err := ensureDir(filepath.Dir(target)); err != nil {
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
			if err := ensureDir(filepath.Dir(target)); err != nil {
				readErr = errors.Errorf("mkdir for symlink %s: %w", hdr.Name, err)
				break readLoop
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				readErr = errors.Errorf("symlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
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
