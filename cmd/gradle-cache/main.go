// gradle-cache restores and saves Gradle build cache bundles from S3.
//
// Bundles are stored at s3://{bucket}/{commit}/{cache-key}/{bundle-file},
// where bundle-file is the cache key with colons replaced by dashes + ".tar.zst".
// This format is compatible with the bundled-cache-manager Ruby script.
//
// On restore, the tool walks the local git history (counting distinct-author
// "blocks") to find the most recent S3 hit, downloads it, extracts it to a
// temporary directory, and symlinks $GRADLE_USER_HOME/caches into place.
// With --project-dir, also restores configuration-cache and convention build
// dirs (buildSrc/build, plugins/*/build) if present in the bundle.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/kong"
)

type CLI struct {
	LogLevel string     `help:"Log level." default:"info" enum:"debug,info,warn,error"`
	Restore  RestoreCmd `cmd:"" help:"Find the newest cached bundle in history and restore it to GRADLE_USER_HOME."`
	Save     SaveCmd    `cmd:"" help:"Bundle GRADLE_USER_HOME/caches and upload to S3 tagged with a commit SHA."`
}

type s3Flags struct {
	Bucket string `help:"S3 bucket name." required:""`
	Region string `help:"AWS region." default:"us-west-2"`
}

// RestoreCmd downloads and extracts a Gradle cache bundle, then symlinks
// $GRADLE_USER_HOME/caches to the extracted directory.
// Also restores configuration-cache and included build output dirs if present in the bundle.
type RestoreCmd struct {
	s3Flags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	GitDir         string   `help:"Path to the git repository used for history walking." default:"." type:"path"`
	Ref            string   `help:"Git ref to start the history walk from." default:"HEAD"`
	Commit         string   `help:"Specific commit SHA to try directly, skipping history walk."`
	MaxBlocks      int      `help:"Number of distinct-author commit blocks to search." default:"20"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to restore (relative to project root). Use 'dir/*' to restore build/ for all subdirectories. May be repeated." name:"included-build"`
}

func (c *RestoreCmd) AfterApply() error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if len(c.IncludedBuilds) == 0 {
		c.IncludedBuilds = []string{"buildSrc"}
	}
	return nil
}

func (c *RestoreCmd) Run(ctx context.Context) error {
	totalStart := time.Now()

	client, err := newS3Client(c.Region)
	if err != nil {
		return err
	}

	bundleFile := bundleFilename(c.CacheKey)

	// ── Find phase ────────────────────────────────────────────────────────────
	findStart := time.Now()

	var commits []string
	if c.Commit != "" {
		commits = []string{c.Commit}
	} else {
		commits, err = historyCommits(ctx, c.GitDir, c.Ref, c.MaxBlocks)
		if err != nil {
			return errors.Wrap(err, "walk git history")
		}
	}

	var hitKey string
	for _, sha := range commits {
		key := s3Key(sha, c.CacheKey, bundleFile)
		if err := client.stat(ctx, c.Bucket, key); err == nil {
			hitKey = key
			break
		}
		slog.Debug("cache miss", "sha", sha[:min(8, len(sha))])
	}
	slog.Debug("find complete", "duration", time.Since(findStart), "commits_checked", len(commits))

	if hitKey == "" {
		slog.Info("no cache bundle found in history")
		return nil
	}
	slog.Info("cache hit", "key", hitKey)

	// ── Download + extract phase ───────────────────────────────────────────────
	dlStart := time.Now()
	slog.Info("downloading bundle", "key", hitKey)

	tmpDir, err := os.MkdirTemp("", "gradle-cache-*")
	if err != nil {
		return errors.Wrap(err, "create temp dir")
	}

	obj, contentLen, err := client.get(ctx, c.Bucket, hitKey)
	if err != nil {
		return errors.Wrap(err, "get object")
	}
	defer obj.Close() //nolint:errcheck

	// Wrap with a counting reader so we can log download speed.
	cr := &countingReader{r: obj}
	if err := extractTarZstd(ctx, cr, tmpDir); err != nil {
		return errors.Wrap(err, "extract bundle")
	}
	elapsed := time.Since(dlStart)
	mbps := float64(cr.n) / elapsed.Seconds() / 1e6
	if contentLen > 0 {
		slog.Info("download+extract complete", "duration", elapsed,
			"size_mb", fmt.Sprintf("%.1f", float64(contentLen)/1e6),
			"speed_mbps", fmt.Sprintf("%.1f", mbps))
	} else {
		slog.Info("download+extract complete", "duration", elapsed,
			"size_mb", fmt.Sprintf("%.1f", float64(cr.n)/1e6),
			"speed_mbps", fmt.Sprintf("%.1f", mbps))
	}

	// Symlink $GRADLE_USER_HOME/caches → tmpDir/caches.
	cachesTarget := filepath.Join(tmpDir, "caches")
	if _, err := os.Stat(cachesTarget); err != nil {
		return errors.Errorf("extracted bundle does not contain a caches/ directory: %w", err)
	}
	localCaches := filepath.Join(c.GradleUserHome, "caches")
	if err := os.RemoveAll(localCaches); err != nil {
		return errors.Wrap(err, "remove existing caches dir")
	}
	if err := os.Symlink(cachesTarget, localCaches); err != nil {
		return errors.Wrap(err, "symlink caches dir")
	}
	slog.Info("restored", "link", localCaches, "target", cachesTarget)

	// Restore configuration-cache and convention build dirs from the current directory.
	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}
	if err := restoreProjectDirs(tmpDir, projectDir, c.IncludedBuilds); err != nil {
		return err
	}

	slog.Debug("restore complete", "total_duration", time.Since(totalStart))
	return nil
}

// restoreProjectDirs symlinks configuration-cache and included build output dirs
// from tmpDir into projectDir, if present in the extracted bundle.
// includedBuilds specifies which directories to check (see conventionBuildDirs).
func restoreProjectDirs(tmpDir, projectDir string, includedBuilds []string) error {
	// configuration-cache: archived at ./configuration-cache/ relative to the bundle root
	// (not under .gradle/), matching the bundled-cache-manager.rb archive format.
	srcCC := filepath.Join(tmpDir, "configuration-cache")
	if _, err := os.Stat(srcCC); err == nil {
		dstCC := filepath.Join(projectDir, ".gradle", "configuration-cache")
		if err := os.MkdirAll(filepath.Dir(dstCC), 0o750); err != nil {
			return errors.Wrap(err, "create .gradle dir")
		}
		if err := os.RemoveAll(dstCC); err != nil {
			return errors.Wrap(err, "remove existing configuration-cache")
		}
		if err := os.Symlink(srcCC, dstCC); err != nil {
			return errors.Wrap(err, "symlink configuration-cache")
		}
		slog.Info("restored", "link", dstCC, "target", srcCC)
	}

	// Included build output dirs present in the extracted bundle.
	for _, rel := range conventionBuildDirs(tmpDir, includedBuilds) {
		src := filepath.Join(tmpDir, rel)
		dst := filepath.Join(projectDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return errors.Errorf("create parent of %s: %w", dst, err)
		}
		if err := os.RemoveAll(dst); err != nil {
			return errors.Errorf("remove existing %s: %w", dst, err)
		}
		if err := os.Symlink(src, dst); err != nil {
			return errors.Errorf("symlink %s: %w", rel, err)
		}
		slog.Info("restored", "link", dst, "target", src)
	}

	return nil
}

// SaveCmd archives $GRADLE_USER_HOME/caches and uploads it to S3.
// Also includes configuration-cache and included build output dirs if they exist.
type SaveCmd struct {
	s3Flags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Commit         string   `help:"Commit SHA to tag this bundle with. Defaults to HEAD of --git-dir."`
	GitDir         string   `help:"Path to the git repository (used to resolve HEAD when --commit is not set)." default:"." type:"path"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to archive (relative to project root). Use 'dir/*' to archive build/ for all subdirectories. May be repeated." name:"included-build"`
}

func (c *SaveCmd) AfterApply(ctx context.Context) error {
	if c.GradleUserHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "resolve home dir")
		}
		c.GradleUserHome = filepath.Join(home, ".gradle")
	}
	if len(c.IncludedBuilds) == 0 {
		c.IncludedBuilds = []string{"buildSrc"}
	}
	if c.Commit == "" {
		sha, err := gitHead(ctx, c.GitDir)
		if err != nil {
			return errors.Wrap(err, "resolve HEAD commit (pass --commit to override)")
		}
		c.Commit = sha
	}
	return nil
}

func (c *SaveCmd) Run(ctx context.Context) error {
	cachesDir := filepath.Join(c.GradleUserHome, "caches")
	if _, err := os.Stat(cachesDir); err != nil {
		return errors.Errorf("caches directory not found at %s: %w", cachesDir, err)
	}

	client, err := newS3Client(c.Region)
	if err != nil {
		return err
	}

	bundleFile := bundleFilename(c.CacheKey)
	key := s3Key(c.Commit, c.CacheKey, bundleFile)

	// Skip upload if bundle already exists.
	if err := client.stat(ctx, c.Bucket, key); err == nil {
		slog.Info("bundle already exists", "key", key)
		return nil
	}

	// Build the list of tar sources: always include caches, plus any
	// configuration-cache and convention build dirs in the current directory.
	projectDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "get working directory")
	}
	sources := []tarSource{{BaseDir: c.GradleUserHome, Path: "./caches"}}
	sources = append(sources, projectDirSources(projectDir, c.IncludedBuilds)...)

	// Buffer the bundle to a temp file so we have a known Content-Length for
	// the S3 PUT (required for single-part upload).
	tmp, err := os.CreateTemp("", "gradle-cache-bundle-*")
	if err != nil {
		return errors.Wrap(err, "create temp file")
	}
	defer func() {
		tmp.Close()           //nolint:errcheck,gosec
		os.Remove(tmp.Name()) //nolint:errcheck,gosec
	}()

	slog.Info("saving bundle", "key", key)
	saveStart := time.Now()

	if err := createTarZstd(ctx, tmp, sources); err != nil {
		return errors.Wrap(err, "create bundle archive")
	}

	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "seek bundle")
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "rewind bundle")
	}

	if err := client.put(ctx, c.Bucket, key, tmp, size, "application/zstd"); err != nil {
		return errors.Wrap(err, "upload bundle")
	}

	elapsed := time.Since(saveStart)
	mbps := float64(size) / elapsed.Seconds() / 1e6
	slog.Info("archive+upload complete", "duration", elapsed,
		"size_mb", fmt.Sprintf("%.1f", float64(size)/1e6),
		"speed_mbps", fmt.Sprintf("%.1f", mbps))
	slog.Info("saved bundle", "key", key)
	return nil
}

// projectDirSources returns tarSource entries for project-specific dirs:
// configuration-cache (from projectDir/.gradle/) and included build output dirs,
// for any that exist on disk. The archive paths match bundled-cache-manager.rb.
func projectDirSources(projectDir string, includedBuilds []string) []tarSource {
	var sources []tarSource

	// configuration-cache is archived at ./configuration-cache/ (not .gradle/configuration-cache/)
	// so that restore can symlink it to a different location (projectDir/.gradle/).
	gradleDir := filepath.Join(projectDir, ".gradle")
	if _, err := os.Stat(filepath.Join(gradleDir, "configuration-cache")); err == nil {
		sources = append(sources, tarSource{BaseDir: gradleDir, Path: "./configuration-cache"})
	}

	// Included build output dirs relative to projectDir.
	for _, rel := range conventionBuildDirs(projectDir, includedBuilds) {
		sources = append(sources, tarSource{BaseDir: projectDir, Path: "./" + rel})
	}

	return sources
}

func main() {
	cli := &CLI{}
	kctx := kong.Parse(cli, kong.UsageOnError(), kong.HelpOptions{Compact: true})
	setupLogger(cli.LogLevel)
	ctx := context.Background()
	kctx.BindTo(ctx, (*context.Context)(nil))
	kctx.FatalIfErrorf(kctx.Run(ctx))
}

// setupLogger configures the global slog logger at the requested level.
// Timestamps are omitted — CI systems capture their own.
func setupLogger(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default: // "info"
		l = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: l,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{} // omit timestamp
			}
			return a
		},
	})
	slog.SetDefault(slog.New(handler))
}

// bundleFilename converts a cache key to its S3 filename, matching the Ruby bundled-cache-manager.
func bundleFilename(cacheKey string) string {
	return strings.ReplaceAll(cacheKey, ":", "-") + ".tar.zst"
}

// s3Key builds the S3 object key for a given commit, cache key, and bundle filename.
func s3Key(commit, cacheKey, bundleFile string) string {
	return commit + "/" + cacheKey + "/" + bundleFile
}

// tarSource specifies a (base directory, relative path) pair for inclusion in a tar archive.
type tarSource struct {
	BaseDir string
	Path    string
}

// historyCommits runs git log from the given ref and returns commit SHAs within
// maxBlocks distinct-author "blocks" (same algorithm as bundled-cache-manager.rb).
func historyCommits(ctx context.Context, gitDir, ref string, maxBlocks int) ([]string, error) {
	rawCount := maxBlocks * 10
	//nolint:gosec // ref is a user-supplied git ref, not a shell injection vector
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "log", "--first-parent",
		fmt.Sprintf("-n%d", rawCount), "--format=%H\t%an", ref)
	out, err := cmd.Output()
	if err != nil {
		return nil, errors.Errorf("git log: %w", err)
	}

	var commits []string
	prevAuthor := ""
	blocksSeen := 0

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) != 2 {
			continue
		}
		sha, author := parts[0], parts[1]
		if author != prevAuthor {
			blocksSeen++
			prevAuthor = author
			if blocksSeen > maxBlocks {
				break
			}
		}
		commits = append(commits, sha)
	}
	return commits, errors.Wrap(scanner.Err(), "scan git log")
}

// gitHead returns the SHA of HEAD in the given git directory.
func gitHead(ctx context.Context, gitDir string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "rev-parse", "HEAD") //nolint:gosec
	out, err := cmd.Output()
	if err != nil {
		return "", errors.Errorf("git rev-parse HEAD: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// countingReader wraps an io.Reader and counts bytes read.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// zstdDecompressArgs returns the command + args for zstd decompression.
// Prefers pzstd (parallel, same format as pzstd-compressed bundles) and
// falls back to zstd -dc -TN.
func zstdDecompressCmd(ctx context.Context) *exec.Cmd {
	n := strconv.Itoa(max(1, runtime.NumCPU()))
	if path, err := exec.LookPath("pzstd"); err == nil {
		// -d decompress, -p N = N threads, -c write to stdout
		return exec.CommandContext(ctx, path, "-d", "-p", n, "-c") //nolint:gosec
	}
	return exec.CommandContext(ctx, "zstd", "-dc", "-T"+n) //nolint:gosec
}

// extractTarZstd decompresses a zstd-compressed tar stream from r into dir.
// Uses pzstd when available for parallel frame decompression (matching the
// parallel compression used on the save path); falls back to zstd -dc -T0.
// File writes are dispatched to a worker pool so NVMe IOPS can be exploited.
func extractTarZstd(ctx context.Context, r io.Reader, dir string) error {
	zstdCmd := zstdDecompressCmd(ctx)
	zstdCmd.Stdin = r

	zstdOut, err := zstdCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "zstd stdout pipe")
	}

	var zstdStderr bytes.Buffer
	zstdCmd.Stderr = &zstdStderr

	if err := zstdCmd.Start(); err != nil {
		return errors.Wrap(err, "start zstd")
	}

	extractErr := extractTar(zstdOut, dir)
	zstdErr := zstdCmd.Wait()

	var errs []error
	if extractErr != nil {
		errs = append(errs, extractErr)
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

// extractTar reads a tar stream from r and extracts it into dir.
// Directories and symlinks are created on the calling goroutine; regular file
// writes are dispatched to a worker pool (one worker per logical CPU) so that
// many small files can be flushed to disk concurrently.
func extractTar(r io.Reader, dir string) error {
	// File writes are I/O bound. Benchmarks show the sweet spot is ~1×CPU;
	// 16 is a floor for machines with few cores where 1×CPU underutilizes IOPS.
	return extractTarN(r, dir, max(16, runtime.NumCPU()))
}

func extractTarN(r io.Reader, dir string, numWorkers int) error {
	type fileJob struct {
		path string
		mode os.FileMode
		data []byte
	}

	jobs := make(chan fileJob, numWorkers*2)

	var (
		workerErrs []error
		mu         sync.Mutex
	)

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := os.MkdirAll(filepath.Dir(job.path), 0o750); err != nil {
					mu.Lock()
					workerErrs = append(workerErrs, errors.Errorf("create parent of %s: %w", job.path, err))
					mu.Unlock()
					continue
				}
				if err := os.WriteFile(job.path, job.data, job.mode); err != nil {
					mu.Lock()
					workerErrs = append(workerErrs, errors.Errorf("write %s: %w", job.path, err))
					mu.Unlock()
				}
			}
		}()
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
			if err := os.MkdirAll(target, hdr.FileInfo().Mode()); err != nil {
				readErr = errors.Errorf("mkdir %s: %w", hdr.Name, err)
				break readLoop
			}
		case tar.TypeReg:
			data, err := io.ReadAll(tr)
			if err != nil {
				readErr = errors.Errorf("read %s: %w", hdr.Name, err)
				break readLoop
			}
			jobs <- fileJob{path: target, mode: hdr.FileInfo().Mode(), data: data}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
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

// zstdCompressCmd returns the command for zstd compression.
// Prefers pzstd (creates parallel frames, decompressable in parallel) and
// falls back to zstd -TN -c.
func zstdCompressCmd(ctx context.Context) *exec.Cmd {
	n := strconv.Itoa(max(1, runtime.NumCPU()))
	if path, err := exec.LookPath("pzstd"); err == nil {
		// -p N = N threads, -c write to stdout
		return exec.CommandContext(ctx, path, "-p", n, "-c") //nolint:gosec
	}
	return exec.CommandContext(ctx, "zstd", "-T"+n, "-c") //nolint:gosec
}

// createTarZstd creates a zstd-compressed tar archive from the given sources and
// writes it to w. Uses -h to dereference symlinks, matching bundled-cache-manager.rb.
// Multiple sources map to multiple -C baseDir path entries in the tar command,
// which is how bundled-cache-manager.rb combines caches + configuration-cache +
// convention build dirs into a single flat archive.
func createTarZstd(ctx context.Context, w io.Writer, sources []tarSource) error {
	args := []string{"-chf", "-"}
	for _, src := range sources {
		args = append(args, "-C", src.BaseDir, src.Path)
	}
	tarCmd := exec.CommandContext(ctx, "tar", args...) //nolint:gosec
	zstdCmd := zstdCompressCmd(ctx)

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "tar stdout pipe")
	}

	var tarStderr, zstdStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr
	zstdCmd.Stdin = tarStdout
	zstdCmd.Stdout = w
	zstdCmd.Stderr = &zstdStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Wrap(err, "start tar")
	}
	if err := zstdCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "start zstd"), tarCmd.Wait())
	}

	tarErr := tarCmd.Wait()
	zstdErr := zstdCmd.Wait()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar: %w: %s", tarErr, tarStderr.String()))
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

// conventionBuildDirs returns the relative paths of included build output directories
// (i.e. <dir>/build) that exist within root, based on the includedBuilds configuration.
//
// Each entry in includedBuilds is a directory path relative to root. If the entry ends
// with "/*", all immediate subdirectories of the parent are scanned and any that contain
// a build/ subdirectory are included. Otherwise, <entry>/build is checked directly.
//
// Example values: "buildSrc", "build-logic", "plugins/*"
func conventionBuildDirs(root string, includedBuilds []string) []string {
	var result []string
	for _, entry := range includedBuilds {
		if strings.HasSuffix(entry, "/*") {
			// Scan all immediate subdirectories of the parent for a build/ subdir.
			parent := strings.TrimSuffix(entry, "/*")
			entries, err := os.ReadDir(filepath.Join(root, parent))
			if err != nil {
				continue
			}
			for _, sub := range entries {
				if !sub.IsDir() {
					continue
				}
				rel := parent + "/" + sub.Name() + "/build"
				if info, err := os.Stat(filepath.Join(root, rel)); err == nil && info.IsDir() {
					result = append(result, rel)
				}
			}
		} else {
			rel := entry + "/build"
			if info, err := os.Stat(filepath.Join(root, rel)); err == nil && info.IsDir() {
				result = append(result, rel)
			}
		}
	}
	return result
}
