//go:build !darwin

package main

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/errors"
)

// extractTarPlatform uses sequential extraction on non-darwin platforms. The
// Linux VFS writeback path coalesces dirty pages most efficiently when a single
// writer produces sequential writes, matching the behaviour of GNU tar. Parallel
// writes from multiple goroutines fragment the writeback queue and are
// consistently slower on Linux ext4/xfs despite identical throughput on APFS.
func extractTarPlatform(r io.Reader, dir string) error {
	return extractTarSeqRouted(r, func(name string) string {
		return filepath.Join(dir, name)
	}, false)
}

// extractTarPlatformRouted is the routing-aware variant of extractTarPlatform.
// targetFn maps a cleaned tar entry name to its absolute destination path.
// If skipExisting is true, files that already exist on disk are left untouched.
func extractTarPlatformRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	return extractTarSeqRouted(r, targetFn, skipExisting)
}

// extractTarSeqRouted is the core sequential extractor. targetFn maps a
// cleaned tar entry name (e.g. "caches/8.14.3/foo") to its absolute
// destination path. skipExisting skips writing files that already exist.
func extractTarSeqRouted(r io.Reader, targetFn func(string) string, skipExisting bool) error {
	// Single fixed-size copy buffer for all file writes in this call.
	// 1 MiB is large enough to amortise write syscall overhead without
	// creating memory pressure for many-file archives.
	copyBuf := make([]byte, 1<<20)

	tr := tar.NewReader(r)

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

		// Reject path traversal before passing to targetFn.
		name := filepath.Clean(hdr.Name)
		if strings.HasPrefix(name, "..") {
			return errors.Errorf("tar entry %q escapes destination directory", hdr.Name)
		}

		target := targetFn(name)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := ensureDir(target); err != nil {
				return errors.Errorf("mkdir %s: %w", hdr.Name, err)
			}

		case tar.TypeReg:
			if skipExisting {
				if _, err := os.Lstat(target); err == nil {
					continue
				}
			}
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
			if skipExisting {
				if _, err := os.Lstat(target); err == nil {
					continue
				}
			}
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
