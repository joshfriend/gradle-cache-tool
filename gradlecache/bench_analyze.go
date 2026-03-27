package gradlecache

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/DataDog/zstd"
	"github.com/alecthomas/errors"
)

// BenchAnalyzeConfig holds the parameters for a bundle content analysis.
type BenchAnalyzeConfig struct {
	Bucket    string
	Region    string
	CacheKey  string
	GitDir    string
	Ref       string
	Commit    string
	MaxBlocks int
	Depth     int // directory depth for grouping (default 2)
	Logger    *slog.Logger
}

func (c *BenchAnalyzeConfig) defaults() {
	if c.Region == "" {
		c.Region = defaultRegion()
	}
	if c.CacheKey == "" {
		c.CacheKey = "gradle"
	}
	if c.GitDir == "" {
		c.GitDir = "."
	}
	if c.Ref == "" {
		c.Ref = "HEAD"
	}
	if c.MaxBlocks == 0 {
		c.MaxBlocks = 20
	}
	if c.Depth == 0 {
		c.Depth = 2
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

func (c *BenchAnalyzeConfig) validate() error {
	if c.Bucket == "" {
		return errors.New("Bucket is required")
	}
	if c.CacheKey == "" {
		return errors.New("CacheKey is required")
	}
	return nil
}

type dirStats struct {
	files     int64
	dirs      int64
	symlinks  int64
	totalSize int64
	maxSize   int64
	minSize   int64
}

// BenchAnalyze downloads a bundle and prints a breakdown of tar entries by
// directory prefix to understand where the file count comes from.
func BenchAnalyze(ctx context.Context, cfg BenchAnalyzeConfig) error {
	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	log := cfg.Logger

	store, err := newStore(cfg.Bucket, cfg.Region, "")
	if err != nil {
		return err
	}

	// Find bundle
	var commits []string
	if cfg.Commit != "" {
		commits = []string{cfg.Commit}
	} else {
		commits, err = historyCommits(ctx, cfg.GitDir, cfg.Ref, cfg.MaxBlocks)
		if err != nil {
			return errors.Wrap(err, "walk git history")
		}
	}

	var hitCommit string
	var hitInfo bundleStatInfo
	for _, sha := range commits {
		if info, err := store.stat(ctx, sha, cfg.CacheKey); err == nil {
			hitCommit = sha
			hitInfo = info
			break
		}
	}
	if hitCommit == "" {
		return errors.New("no cache bundle found in history")
	}
	log.Info("found bundle", "commit", hitCommit[:min(8, len(hitCommit))],
		"size_mb", fmt.Sprintf("%.0f", float64(hitInfo.Size)/1e6))

	// Download and scan
	dlStart := time.Now()
	body, err := store.get(ctx, hitCommit, cfg.CacheKey, hitInfo)
	if err != nil {
		return errors.Wrap(err, "get bundle")
	}
	defer body.Close() //nolint:errcheck,gosec

	br := bufio.NewReaderSize(body, 8<<20)
	dec := zstd.NewReader(br)
	defer dec.Close() //nolint:errcheck

	stats := make(map[string]*dirStats)
	var totalFiles, totalDirs, totalSymlinks int64
	var totalSize int64

	tr := tar.NewReader(dec)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "read tar entry")
		}

		prefix := dirPrefix(hdr.Name, cfg.Depth)
		s, ok := stats[prefix]
		if !ok {
			s = &dirStats{minSize: 1<<63 - 1}
			stats[prefix] = s
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			s.dirs++
			totalDirs++
		case tar.TypeSymlink:
			s.symlinks++
			totalSymlinks++
		case tar.TypeReg:
			s.files++
			s.totalSize += hdr.Size
			if hdr.Size > s.maxSize {
				s.maxSize = hdr.Size
			}
			if hdr.Size < s.minSize {
				s.minSize = hdr.Size
			}
			totalFiles++
			totalSize += hdr.Size
		}
	}

	elapsed := time.Since(dlStart)
	fmt.Printf("download + scan: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("compressed size: %.0f MB   decompressed: %.0f MB\n\n",
		float64(hitInfo.Size)/1e6, float64(totalSize)/1e6)

	// Sort by file count descending
	type row struct {
		prefix string
		s      *dirStats
	}
	var rows []row
	for k, v := range stats {
		rows = append(rows, row{k, v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].s.files > rows[j].s.files })

	fmt.Printf("%-60s %8s %8s %8s %10s %10s %10s\n",
		"PREFIX", "FILES", "DIRS", "SYMLINKS", "TOTAL_MB", "AVG_KB", "MAX_KB")
	fmt.Println(strings.Repeat("─", 120))

	for _, r := range rows {
		var avgKB float64
		if r.s.files > 0 {
			avgKB = float64(r.s.totalSize) / float64(r.s.files) / 1024
		}
		fmt.Printf("%-60s %8d %8d %8d %10.1f %10.1f %10.1f\n",
			r.prefix,
			r.s.files, r.s.dirs, r.s.symlinks,
			float64(r.s.totalSize)/1e6,
			avgKB,
			float64(r.s.maxSize)/1024)
	}

	fmt.Println(strings.Repeat("─", 120))
	fmt.Printf("%-60s %8d %8d %8d %10.1f\n",
		"TOTAL", totalFiles, totalDirs, totalSymlinks, float64(totalSize)/1e6)

	return nil
}

// dirPrefix returns the first n path components of a tar entry name.
func dirPrefix(name string, depth int) string {
	name = path.Clean(name)
	parts := strings.Split(name, "/")
	if len(parts) > depth {
		parts = parts[:depth]
	}
	return strings.Join(parts, "/")
}
