package gradlecache

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/errgroup"
)

// concurrentDirCache creates directories at most once across multiple goroutines.
// Invariant: if Load returns ok, the directory exists on disk.
type concurrentDirCache struct {
	m sync.Map
}

func (c *concurrentDirCache) ensure(d string, mode os.FileMode) error {
	if _, ok := c.m.Load(d); ok {
		return nil
	}
	if err := os.MkdirAll(d, mode); err != nil {
		return err
	}
	c.m.Store(d, struct{}{})
	return nil
}

const (
	// maxBufferedFileSize is the threshold below which files are buffered in
	// memory and dispatched to a small-file worker. Above this size the reader
	// goroutine streams chunks through a bounded channel to a large-file worker,
	// so the reader can advance to the next tar entry before the write completes.
	maxBufferedFileSize = 4 << 20 // 4 MB

	// largeChunkSize is the chunk size used when streaming large files.
	largeChunkSize = 1 << 20 // 1 MB

	// largeChunkCap is the channel buffer depth per in-flight large file.
	// Memory per large file: largeChunkCap × largeChunkSize = 4 MB.
	largeChunkCap = 4

	// numLargeWorkers is the number of concurrent large-file writer goroutines.
	// Total bounded memory for large files: numLargeWorkers × largeChunkCap × largeChunkSize = 16 MB.
	numLargeWorkers = 4
)

// largeChunkPool reuses fixed-size chunk buffers across large-file writes.
var largeChunkPool = sync.Pool{
	New: func() any {
		b := make([]byte, largeChunkSize)
		return &b
	},
}

// largeWriteJob carries a destination path and a channel of data chunks for a
// single large file. The reader closes chunks after dispatching all bytes,
// allowing it to advance to the next tar entry before the write finishes.
type largeWriteJob struct {
	target string
	mode   os.FileMode
	chunks chan *[]byte // closed by reader when all chunks are dispatched
}

func extractWorkerCount() int {
	return 16
}

func extractTarPlatform(r io.Reader, dir string) error {
	return extractTarParallelRouted(r, func(name string) string {
		return filepath.Join(dir, name)
	}, false)
}

func extractTarPlatformRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	return extractTarParallelRouted(r, targetFn, skipExisting)
}

type writeJob struct {
	target string
	mode   os.FileMode
	data   []byte
}

func extractTarParallelRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	numWorkers := extractWorkerCount()
	jobs := make(chan writeJob, numWorkers*2)
	// Unbuffered: reader blocks until a large-file worker is free, providing
	// backpressure that bounds the number of in-flight large writes to numLargeWorkers.
	largeJobs := make(chan largeWriteJob)

	dc := &concurrentDirCache{}

	g, ctx := errgroup.WithContext(context.Background())

	// Small-file workers: buffer the whole file in memory, create parent dir, write.
	for range numWorkers {
		g.Go(func() error {
			for job := range jobs {
				if err := dc.ensure(filepath.Dir(job.target), 0o755); err != nil {
					return errors.Errorf("mkdir %s: %w", filepath.Base(filepath.Dir(job.target)), err)
				}
				f, err := os.OpenFile(job.target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, job.mode)
				if err != nil {
					return errors.Errorf("open %s: %w", filepath.Base(job.target), err)
				}
				if _, err := f.Write(job.data); err != nil {
					f.Close() //nolint:errcheck,gosec
					return errors.Errorf("write %s: %w", filepath.Base(job.target), err)
				}
				if err := f.Close(); err != nil {
					return errors.Errorf("close %s: %w", filepath.Base(job.target), err)
				}
			}
			return nil
		})
	}

	// Large-file workers: drain a per-job chunks channel and stream to disk.
	// The reader advances to the next tar entry as soon as it closes the channel,
	// so writes overlap with reading subsequent entries.
	for range numLargeWorkers {
		g.Go(func() error {
			for job := range largeJobs {
				if err := dc.ensure(filepath.Dir(job.target), 0o755); err != nil {
					return errors.Errorf("mkdir %s: %w", filepath.Base(filepath.Dir(job.target)), err)
				}
				f, err := os.OpenFile(job.target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, job.mode)
				if err != nil {
					// Drain the channel so the reader isn't blocked on a send.
					for buf := range job.chunks {
						largeChunkPool.Put(buf)
					}
					return errors.Errorf("open %s: %w", filepath.Base(job.target), err)
				}
				var writeErr error
				for buf := range job.chunks {
					if writeErr == nil {
						if _, err := f.Write(*buf); err != nil {
							writeErr = errors.Errorf("write %s: %w", filepath.Base(job.target), err)
						}
					}
					*buf = (*buf)[:cap(*buf)] //nolint:gosec
					largeChunkPool.Put(buf)
				}
				if err := f.Close(); err != nil && writeErr == nil {
					writeErr = errors.Errorf("close %s: %w", filepath.Base(job.target), err)
				}
				if writeErr != nil {
					return writeErr
				}
			}
			return nil
		})
	}

	// Reader uses the same cache for TypeDir, symlinks, and hardlinks; regular
	// files are dispatched to small- or large-file workers depending on size.
	readErr := readTarEntries(r, targetFn, skipExisting, dc.ensure, jobs, largeJobs, ctx)

	close(jobs)
	close(largeJobs)
	writeErr := g.Wait()

	if readErr != nil {
		return readErr
	}
	return writeErr
}

func readTarEntries(
	r io.Reader,
	targetFn func(string) string,
	skipExisting bool,
	ensureDir func(string, os.FileMode) error,
	jobs chan<- writeJob,
	largeJobs chan<- largeWriteJob,
	ctx context.Context,
) error {
	tr := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "context cancelled")
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "read tar entry")
		}

		if err := processEntry(tr, hdr, targetFn, skipExisting, ensureDir, jobs, largeJobs, ctx); err != nil {
			return err
		}
	}
}

func processEntry(
	tr *tar.Reader,
	hdr *tar.Header,
	targetFn func(string) string,
	skipExisting bool,
	ensureDir func(string, os.FileMode) error,
	jobs chan<- writeJob,
	largeJobs chan<- largeWriteJob,
	ctx context.Context,
) error {
	name, err := safeTarEntryName(hdr.Name)
	if err != nil {
		return err
	}

	target := targetFn(name)

	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := ensureDir(target, hdr.FileInfo().Mode()); err != nil {
			return errors.Errorf("mkdir %s: %w", hdr.Name, err)
		}

	case tar.TypeReg:
		if skipExisting {
			if _, err := os.Lstat(target); err == nil {
				return nil
			}
		}
		if hdr.Size > maxBufferedFileSize {
			// Large file: stream chunks through a bounded channel to a large-file
			// worker. The reader must consume all bytes from tr before advancing,
			// but it does not wait for the worker to finish writing — that write
			// overlaps with the reader processing subsequent tar entries.
			//
			// Backpressure: largeJobs is unbuffered (reader blocks until a worker
			// is free); chunks has largeChunkCap slots (reader blocks if the worker
			// falls behind on disk writes).
			chunks := make(chan *[]byte, largeChunkCap)
			select {
			case largeJobs <- largeWriteJob{target: target, mode: hdr.FileInfo().Mode(), chunks: chunks}:
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "context cancelled waiting for large-file worker")
			}
			remaining := hdr.Size
			for remaining > 0 {
				buf := largeChunkPool.Get().(*[]byte) //nolint:errcheck
				n, err := io.ReadFull(tr, (*buf)[:min(int64(largeChunkSize), remaining)])
				if err != nil {
					largeChunkPool.Put(buf)
					close(chunks)
					// Remove the partially-written file so a truncated
					// bundle doesn't leave corrupt artifacts on disk.
					os.Remove(target) //nolint:errcheck
					return errors.Errorf("read %s: %w", hdr.Name, err)
				}
				*buf = (*buf)[:n]
				select {
				case chunks <- buf:
				case <-ctx.Done():
					largeChunkPool.Put(buf)
					close(chunks)
					return errors.Wrap(ctx.Err(), "context cancelled dispatching large-file chunks")
				}
				remaining -= int64(n)
			}
			close(chunks)
			return nil
		}
		// Small file: buffer in memory and dispatch to a small-file worker.
		// Parent dir creation is deferred to the worker.
		buf := make([]byte, hdr.Size)
		if _, err := io.ReadFull(tr, buf); err != nil {
			return errors.Errorf("read %s: %w", hdr.Name, err)
		}
		select {
		case jobs <- writeJob{target: target, mode: hdr.FileInfo().Mode(), data: buf}:
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled dispatching small-file job")
		}

	case tar.TypeSymlink:
		if skipExisting {
			if _, err := os.Lstat(target); err == nil {
				return nil
			}
		}
		if err := safeSymlinkTarget(name, hdr.Linkname); err != nil {
			return err
		}
		if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
			return errors.Errorf("mkdir for symlink %s: %w", hdr.Name, err)
		}
		if err := os.Symlink(hdr.Linkname, target); err != nil {
			return errors.Errorf("symlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
		}

	case tar.TypeLink:
		if skipExisting {
			if _, err := os.Lstat(target); err == nil {
				return nil
			}
		}
		linkName, err := safeTarEntryName(hdr.Linkname)
		if err != nil {
			return errors.Errorf("unsafe hardlink target %q: %w", hdr.Linkname, err)
		}
		linkTarget := targetFn(linkName)
		if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
			return errors.Errorf("mkdir for hardlink %s: %w", hdr.Name, err)
		}
		if err := os.Link(linkTarget, target); err != nil {
			return errors.Errorf("hardlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
		}
	}

	return nil
}
