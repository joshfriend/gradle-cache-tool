package main

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/errors"
)

// extractTarPlatform uses sequential extraction on Linux. The Linux VFS
// writeback path coalesces dirty pages most efficiently when a single writer
// produces sequential writes, matching the behaviour of GNU tar. Parallel
// writes from multiple goroutines fragment the writeback queue and are
// consistently slower on Linux ext4/xfs despite identical throughput on APFS.
func extractTarPlatform(r io.Reader, dir string) error {
	return extractTarSeq(r, dir)
}

// extractTarSeq extracts a tar stream sequentially using a fixed-size copy
// buffer. Files are streamed directly from the tar reader to disk one 1 MiB
// block at a time — the same block-streaming pattern GNU tar uses — so the
// decompressor pipe keeps flowing without large per-file allocations.
func extractTarSeq(r io.Reader, dir string) error {
	// Single fixed-size copy buffer for all file writes in this call.
	// 1 MiB is large enough to amortise write syscall overhead without
	// creating memory pressure for many-file archives.
	copyBuf := make([]byte, 1<<20)

	tr := tar.NewReader(r)
	cleanDir := filepath.Clean(dir) + string(os.PathSeparator)

	// createdDirs tracks parent directories we have already MkdirAll'd so
	// each unique path is only created once (same optimisation as darwin).
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

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "read tar entry")
		}

		target := filepath.Join(dir, hdr.Name)
		if !strings.HasPrefix(filepath.Clean(target)+string(os.PathSeparator), cleanDir) {
			return errors.Errorf("tar entry %q escapes destination directory", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := ensureDir(target); err != nil {
				return errors.Errorf("mkdir %s: %w", hdr.Name, err)
			}

		case tar.TypeReg:
			if err := ensureDir(filepath.Dir(target)); err != nil {
				return errors.Errorf("mkdir %s: %w", hdr.Name, err)
			}
			f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hdr.FileInfo().Mode())
			if err != nil {
				return errors.Errorf("open %s: %w", hdr.Name, err)
			}
			if _, err := io.CopyBuffer(f, io.LimitReader(tr, hdr.Size), copyBuf); err != nil {
				f.Close() //nolint:errcheck
				return errors.Errorf("write %s: %w", hdr.Name, err)
			}
			if err := f.Close(); err != nil {
				return errors.Errorf("close %s: %w", hdr.Name, err)
			}

		case tar.TypeSymlink:
			if err := ensureDir(filepath.Dir(target)); err != nil {
				return errors.Errorf("mkdir for symlink %s: %w", hdr.Name, err)
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return errors.Errorf("symlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
			}
		}
	}
	return nil
}
