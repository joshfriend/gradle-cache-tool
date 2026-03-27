//go:build !darwin

package gradlecache

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/errgroup"
)

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

	g, ctx := errgroup.WithContext(context.Background())

	for range numWorkers {
		g.Go(func() error {
			for job := range jobs {
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

	createdDirs := make(map[string]struct{})
	ensureDir := func(d string, mode os.FileMode) error {
		if _, ok := createdDirs[d]; ok {
			return nil
		}
		if err := os.MkdirAll(d, mode); err != nil { //nolint:gosec
			return err
		}
		createdDirs[d] = struct{}{}
		return nil
	}

	readErr := readTarEntries(r, targetFn, skipExisting, ensureDir, jobs, ctx)

	close(jobs)
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

		if err := processEntry(tr, hdr, targetFn, skipExisting, ensureDir, jobs); err != nil {
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
		if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
			return errors.Errorf("mkdir %s: %w", hdr.Name, err)
		}

		buf := make([]byte, hdr.Size)
		if _, err := io.ReadFull(tr, buf); err != nil {
			return errors.Errorf("read %s: %w", hdr.Name, err)
		}
		jobs <- writeJob{target: target, mode: hdr.FileInfo().Mode(), data: buf}

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
