//go:build !darwin && !linux

package main

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/errors"
)

// extractTarPlatform falls back to sequential streaming extraction on unknown platforms.
func extractTarPlatform(r io.Reader, dir string) error {
	return extractTarSeq(r, dir)
}

func extractTarSeq(r io.Reader, dir string) error {
	copyBuf := make([]byte, 1<<20)
	tr := tar.NewReader(r)
	cleanDir := filepath.Clean(dir) + string(os.PathSeparator)

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
