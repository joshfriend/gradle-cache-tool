//nolint:gosec // test file: all paths and subprocess args are controlled inputs
package gradlecache

import (
	archive_tar "archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ─── Pure unit tests ────────────────────────────────────────────────────────

func TestIsExcludedCache(t *testing.T) {
	excluded := []string{
		"daemon",
		".tmp",
		"gc.properties",
		"cc-keystore",
		"foo.lock",
		"gradle-cache.lock",
		"file-changes",
		"journal-1",
		"user-id.txt",
	}
	for _, name := range excluded {
		if !IsExcludedCache(name) {
			t.Errorf("expected %q to be excluded", name)
		}
	}

	allowed := []string{
		"modules-2",
		"transforms-4",
		"wrapper",
		"caches",
		"foo.jar",
		"build.gradle.kts",
	}
	for _, name := range allowed {
		if IsExcludedCache(name) {
			t.Errorf("expected %q to NOT be excluded", name)
		}
	}
}

func TestIsDeltaExcluded(t *testing.T) {
	excluded := []string{
		"fileHashes",
		"module-metadata.bin",
	}
	for _, name := range excluded {
		if !IsDeltaExcluded(name) {
			t.Errorf("expected %q to be delta-excluded", name)
		}
	}

	allowed := []string{
		"transforms",
		"build-cache-1",
		"metadata.bin",
		"results.bin",
	}
	for _, name := range allowed {
		if IsDeltaExcluded(name) {
			t.Errorf("expected %q to NOT be delta-excluded", name)
		}
	}
}

// TestCollectNewFilesWorkspaceCompleteness verifies that CollectNewFiles
// includes ALL files from an immutable workspace when any file is new.
func TestCollectNewFilesWorkspaceCompleteness(t *testing.T) {
	cacheDir := t.TempDir()
	gradleHome := filepath.Join(t.TempDir(), "gradle-home")
	must(t, os.MkdirAll(gradleHome, 0o755))

	wsDir := filepath.Join(cacheDir, "8.14.3", "transforms", "abc123")
	must(t, os.MkdirAll(filepath.Join(wsDir, "transformed"), 0o755))

	oldTime := time.Now().Add(-10 * time.Second)
	newTime := time.Now()
	since := time.Now().Add(-5 * time.Second)

	// metadata.bin — old (before marker).
	must(t, os.WriteFile(filepath.Join(wsDir, "metadata.bin"), []byte("meta"), 0o644))
	must(t, os.Chtimes(filepath.Join(wsDir, "metadata.bin"), oldTime, oldTime))

	// results.bin — old.
	must(t, os.WriteFile(filepath.Join(wsDir, "results.bin"), []byte("res"), 0o644))
	must(t, os.Chtimes(filepath.Join(wsDir, "results.bin"), oldTime, oldTime))

	// transformed/output.bin — NEW (after marker).
	must(t, os.WriteFile(filepath.Join(wsDir, "transformed", "output.bin"), []byte("out"), 0o644))
	must(t, os.Chtimes(filepath.Join(wsDir, "transformed", "output.bin"), newTime, newTime))

	files, err := CollectNewFiles(cacheDir, since, gradleHome)
	must(t, err)

	// All 3 files must be collected because output.bin is new and the
	// workspace is under an ImmutableWorkspaceParent (transforms/).
	fileSet := make(map[string]bool)
	for _, f := range files {
		fileSet[f] = true
	}
	for _, expected := range []string{
		"caches/8.14.3/transforms/abc123/metadata.bin",
		"caches/8.14.3/transforms/abc123/results.bin",
		"caches/8.14.3/transforms/abc123/transformed/output.bin",
	} {
		if !fileSet[expected] {
			t.Errorf("expected %q in collected files, got: %v", expected, files)
		}
	}
}

// TestCollectNewFilesWorkspaceSkipped verifies that an immutable workspace
// where NO files are new is entirely skipped.
func TestCollectNewFilesWorkspaceSkipped(t *testing.T) {
	cacheDir := t.TempDir()
	gradleHome := filepath.Join(t.TempDir(), "gradle-home")
	must(t, os.MkdirAll(gradleHome, 0o755))

	wsDir := filepath.Join(cacheDir, "8.14.3", "transforms", "abc123")
	must(t, os.MkdirAll(filepath.Join(wsDir, "transformed"), 0o755))

	oldTime := time.Now().Add(-10 * time.Second)
	since := time.Now().Add(-5 * time.Second)

	must(t, os.WriteFile(filepath.Join(wsDir, "metadata.bin"), []byte("meta"), 0o644))
	must(t, os.Chtimes(filepath.Join(wsDir, "metadata.bin"), oldTime, oldTime))
	must(t, os.WriteFile(filepath.Join(wsDir, "transformed", "output.bin"), []byte("out"), 0o644))
	must(t, os.Chtimes(filepath.Join(wsDir, "transformed", "output.bin"), oldTime, oldTime))

	files, err := CollectNewFiles(cacheDir, since, gradleHome)
	must(t, err)

	for _, f := range files {
		if strings.Contains(f, "transforms/abc123") {
			t.Errorf("workspace abc123 should have been skipped, but found: %s", f)
		}
	}
}

func TestBundleFilename(t *testing.T) {
	tests := []struct {
		cacheKey string
		want     string
	}{
		{"my-project:assembleRelease", "my-project-assembleRelease.tar.zst"},
		{"my-project:assemble", "my-project-assemble.tar.zst"},
		{"simple", "simple.tar.zst"},
		{"a:b:c", "a-b-c.tar.zst"},
	}
	for _, tt := range tests {
		if got := bundleFilename(tt.cacheKey); got != tt.want {
			t.Errorf("bundleFilename(%q) = %q, want %q", tt.cacheKey, got, tt.want)
		}
	}
}

func TestS3PathEscape(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"simple", "simple"},
		{"apos-beta", "apos-beta"},
		{"address-typeahead-sample:assembleDebug", "address-typeahead-sample%3AassembleDebug"},
		{"a:b:c", "a%3Ab%3Ac"},
		{":leadingColon", "%3AleadingColon"},
		{"file.tar.zst", "file.tar.zst"},
		{"with spaces", "with%20spaces"},
		{"tilde~ok", "tilde~ok"},
		{"hash#bad", "hash%23bad"},
	}
	for _, tt := range tests {
		if got := s3PathEscape(tt.input); got != tt.want {
			t.Errorf("s3PathEscape(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestS3Key(t *testing.T) {
	tests := []struct {
		commit, cacheKey, bundleFile, want string
	}{
		{
			"abc123", "my-key", "my-key.tar.zst",
			"abc123/my-key/my-key.tar.zst",
		},
		{
			"deadbeef", "my-project:assemble", "my-project-assemble.tar.zst",
			"deadbeef/my-project:assemble/my-project-assemble.tar.zst",
		},
	}
	for _, tt := range tests {
		if got := s3Key("", tt.commit, tt.cacheKey, tt.bundleFile); got != tt.want {
			t.Errorf("s3Key(%q, %q, %q) = %q, want %q",
				tt.commit, tt.cacheKey, tt.bundleFile, got, tt.want)
		}
	}
}

// ─── ConventionBuildDirs tests ───────────────────────────────────────────────

func TestConventionBuildDirs(t *testing.T) {
	t.Run("empty directory returns nothing", func(t *testing.T) {
		root := t.TempDir()
		if got := ConventionBuildDirs(root, []string{"buildSrc"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("buildSrc without build subdir is excluded", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc"), 0o755))
		if got := ConventionBuildDirs(root, []string{"buildSrc"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("buildSrc/build is included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		got := ConventionBuildDirs(root, []string{"buildSrc"})
		if len(got) != 1 || got[0] != "buildSrc/build" {
			t.Errorf("got %v, want [buildSrc/build]", got)
		}
	})

	t.Run("buildSrc not included when not in config", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		// build-logic is configured but doesn't exist; buildSrc exists but isn't configured.
		got := ConventionBuildDirs(root, []string{"build-logic"})
		if len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("build-logic/build included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		got := ConventionBuildDirs(root, []string{"build-logic"})
		if len(got) != 1 || got[0] != "build-logic/build" {
			t.Errorf("got %v, want [build-logic/build]", got)
		}
	})

	t.Run("multiple explicit dirs all checked", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		got := ConventionBuildDirs(root, []string{"buildSrc", "build-logic"})
		sort.Strings(got)
		if len(got) != 2 || got[0] != "build-logic/build" || got[1] != "buildSrc/build" {
			t.Errorf("got %v, want [build-logic/build buildSrc/build]", got)
		}
	})

	t.Run("glob plugins/* finds subdirectory build dirs", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		got := ConventionBuildDirs(root, []string{"plugins/*"})
		if len(got) != 1 || got[0] != "plugins/foo/build" {
			t.Errorf("got %v, want [plugins/foo/build]", got)
		}
	})

	t.Run("glob plugins/* excludes subdirs without a build dir", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo"), 0o755))
		if got := ConventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("glob plugins/* excludes files named build", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo"), 0o755))
		must(t, os.WriteFile(filepath.Join(root, "plugins", "foo", "build"), []byte("nope"), 0o644))
		if got := ConventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("glob plugins/* finds multiple subdirectories", func(t *testing.T) {
		root := t.TempDir()
		for _, p := range []string{"alpha", "beta", "gamma"} {
			must(t, os.MkdirAll(filepath.Join(root, "plugins", p, "build"), 0o755))
		}
		got := ConventionBuildDirs(root, []string{"plugins/*"})
		if len(got) != 3 {
			t.Errorf("expected 3 entries, got %v", got)
		}
	})

	t.Run("missing glob parent directory is silently ignored", func(t *testing.T) {
		root := t.TempDir()
		if got := ConventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty for missing parent, got %v", got)
		}
	})

	t.Run("buildSrc and plugins/* combined", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		got := ConventionBuildDirs(root, []string{"buildSrc", "plugins/*"})
		sort.Strings(got)
		if len(got) != 2 || got[0] != "buildSrc/build" || got[1] != "plugins/foo/build" {
			t.Errorf("got %v, want [buildSrc/build plugins/foo/build]", got)
		}
	})
}

// ─── ProjectDirSources tests ─────────────────────────────────────────────────

func TestProjectDirSources(t *testing.T) {
	defaultBuilds := []string{"buildSrc"}

	t.Run("empty project dir returns no sources", func(t *testing.T) {
		root := t.TempDir()
		if got := ProjectDirSources(root, defaultBuilds); len(got) != 0 {
			t.Errorf("expected no sources, got %v", got)
		}
	})

	t.Run("configuration-cache source has correct BaseDir and Path", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, ".gradle", "configuration-cache"), 0o755))
		sources := ProjectDirSources(root, defaultBuilds)
		if len(sources) != 1 {
			t.Fatalf("expected 1 source, got %v", sources)
		}
		wantBase := filepath.Join(root, ".gradle")
		if sources[0].BaseDir != wantBase {
			t.Errorf("BaseDir = %q, want %q", sources[0].BaseDir, wantBase)
		}
		if sources[0].Path != "./configuration-cache" {
			t.Errorf("Path = %q, want ./configuration-cache", sources[0].Path)
		}
	})

	t.Run("buildSrc/build source has correct BaseDir and Path", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		sources := ProjectDirSources(root, defaultBuilds)
		if len(sources) != 1 {
			t.Fatalf("expected 1 source, got %v", sources)
		}
		if sources[0].BaseDir != root {
			t.Errorf("BaseDir = %q, want %q", sources[0].BaseDir, root)
		}
		if sources[0].Path != "./buildSrc/build" {
			t.Errorf("Path = %q, want ./buildSrc/build", sources[0].Path)
		}
	})

	t.Run("build-logic included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		sources := ProjectDirSources(root, []string{"build-logic"})
		if len(sources) != 1 || sources[0].Path != "./build-logic/build" {
			t.Errorf("expected build-logic/build, got %v", sources)
		}
	})

	t.Run("all dirs present with plugins glob returns expected count", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, ".gradle", "configuration-cache"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		sources := ProjectDirSources(root, []string{"buildSrc", "plugins/*"})
		// configuration-cache + buildSrc/build + plugins/foo/build = 3
		if len(sources) != 3 {
			t.Errorf("expected 3 sources, got %d: %v", len(sources), sources)
		}
	})
}

// ─── extractBundleZstd routing tests ─────────────────────────────────────────

// TestExtractBundleRouting verifies that the routing function used by
// extractBundleZstd places tar entries in the correct destination directories.
func TestExtractBundleRouting(t *testing.T) {
	gradleHome := t.TempDir()
	projectDir := t.TempDir()

	rules := []extractRule{
		{prefix: "caches/", baseDir: gradleHome},
		{prefix: "wrapper/", baseDir: gradleHome},
		{prefix: "configuration-cache/", baseDir: filepath.Join(projectDir, ".gradle")},
	}

	targetFn := func(name string) string {
		for _, rule := range rules {
			if strings.HasPrefix(name, rule.prefix) {
				return filepath.Join(rule.baseDir, name)
			}
		}
		return filepath.Join(projectDir, name)
	}

	cases := []struct {
		entry string
		want  string
	}{
		{
			entry: "caches/8.14.3/foo.jar",
			want:  filepath.Join(gradleHome, "caches/8.14.3/foo.jar"),
		},
		{
			entry: "wrapper/dists/gradle-8.14.3-bin/abc123/gradle-8.14.3/lib/gradle-core.jar",
			want:  filepath.Join(gradleHome, "wrapper/dists/gradle-8.14.3-bin/abc123/gradle-8.14.3/lib/gradle-core.jar"),
		},
		{
			entry: "configuration-cache/abc/entry",
			want:  filepath.Join(projectDir, ".gradle", "configuration-cache/abc/entry"),
		},
		{
			entry: "buildSrc/build/libs/buildSrc.jar",
			want:  filepath.Join(projectDir, "buildSrc/build/libs/buildSrc.jar"),
		},
		{
			entry: "plugins/foo/build/libs/foo.jar",
			want:  filepath.Join(projectDir, "plugins/foo/build/libs/foo.jar"),
		},
	}

	for _, tc := range cases {
		got := targetFn(tc.entry)
		if got != tc.want {
			t.Errorf("targetFn(%q) = %q, want %q", tc.entry, got, tc.want)
		}
	}
}

func TestExtractZstdDrainsBufferedReaderToEOF(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(srcDir, "caches"), 0o755))
	must(t, os.WriteFile(filepath.Join(srcDir, "caches", "entry.bin"), []byte("gradle data"), 0o644))

	var archive bytes.Buffer
	must(t, CreateDeltaTarZstd(ctx, &archive, srcDir, []string{"caches/entry.bin"}))

	t.Run("bundle restore", func(t *testing.T) {
		gradleHome := t.TempDir()
		projectDir := t.TempDir()
		cb := &countingBody{r: bytes.NewReader(archive.Bytes())}

		_, err := extractBundleZstd(ctx, cb, []extractRule{
			{prefix: "caches/", baseDir: gradleHome},
		}, projectDir, false)
		must(t, err)

		if cb.eofAt.IsZero() {
			t.Fatal("expected extractBundleZstd to drain the buffered reader to EOF")
		}
		if _, err := os.Stat(filepath.Join(gradleHome, "caches", "entry.bin")); err != nil {
			t.Fatalf("expected extracted file: %v", err)
		}
	})

	t.Run("delta restore", func(t *testing.T) {
		dstDir := t.TempDir()
		cb := &countingBody{r: bytes.NewReader(archive.Bytes())}

		must(t, extractTarZstd(ctx, cb, dstDir))
		if cb.eofAt.IsZero() {
			t.Fatal("expected extractTarZstd to drain the buffered reader to EOF")
		}
		if _, err := os.Stat(filepath.Join(dstDir, "caches", "entry.bin")); err != nil {
			t.Fatalf("expected extracted file: %v", err)
		}
	})
}

// ─── Round-trip archive test ─────────────────────────────────────────────────

// TestTarZstdRoundTrip verifies that CreateTarZstd → extractTarZstd preserves
// the expected directory structure, including multi-source archives.
func TestTarZstdRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("tar"); err != nil {
		t.Skip("tar not available")
	}
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd not available")
	}

	ctx := context.Background()
	srcDir := t.TempDir()

	// caches/ source (under gradle-home)
	gradleHome := filepath.Join(srcDir, "gradle-home")
	must(t, os.MkdirAll(filepath.Join(gradleHome, "caches", "modules"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleHome, "caches", "modules", "entry.bin"), []byte("gradle data"), 0o644))
	// cc-keystore should be excluded
	must(t, os.MkdirAll(filepath.Join(gradleHome, "caches", "8.14.3", "cc-keystore"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleHome, "caches", "8.14.3", "cc-keystore", "keystore.bin"), []byte("secret"), 0o644))

	// wrapper/ source (under gradle-home) — includes a .zip that should be excluded
	// and a .ok marker file that must be preserved (Gradle checks for it to skip re-downloading)
	must(t, os.MkdirAll(filepath.Join(gradleHome, "wrapper", "dists", "gradle-8.14.3-bin", "abc123"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleHome, "wrapper", "dists", "gradle-8.14.3-bin", "abc123", "gradle-8.14.3-bin.zip"), []byte("should be excluded"), 0o644))
	must(t, os.WriteFile(filepath.Join(gradleHome, "wrapper", "dists", "gradle-8.14.3-bin", "abc123", "gradle-8.14.3-bin.zip.ok"), []byte(""), 0o644))
	must(t, os.MkdirAll(filepath.Join(gradleHome, "wrapper", "dists", "gradle-8.14.3-bin", "abc123", "gradle-8.14.3", "lib"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleHome, "wrapper", "dists", "gradle-8.14.3-bin", "abc123", "gradle-8.14.3", "lib", "gradle-core.jar"), []byte("wrapper data"), 0o644))

	// configuration-cache/ source (under .gradle/ inside project)
	projectDir := filepath.Join(srcDir, "project")
	gradleDir := filepath.Join(projectDir, ".gradle")
	must(t, os.MkdirAll(filepath.Join(gradleDir, "configuration-cache"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleDir, "configuration-cache", "hash.bin"), []byte("config cache"), 0o644))

	// included build (buildSrc) output directory
	must(t, os.MkdirAll(filepath.Join(projectDir, "buildSrc", "build", "libs"), 0o755))
	must(t, os.WriteFile(filepath.Join(projectDir, "buildSrc", "build", "libs", "buildSrc.jar"), []byte("buildsrc jar"), 0o644))

	sources := []TarSource{
		{BaseDir: gradleHome, Path: "./caches"},
		{BaseDir: gradleHome, Path: "./wrapper"},
		{BaseDir: gradleDir, Path: "./configuration-cache"},
		{BaseDir: projectDir, Path: "./buildSrc/build"},
	}

	// Create archive into a buffer.
	var buf bytes.Buffer
	if err := CreateTarZstd(ctx, &buf, sources); err != nil {
		t.Fatalf("CreateTarZstd: %v", err)
	}

	// Extract into a fresh directory.
	dstDir := t.TempDir()
	if err := extractTarZstd(ctx, &buf, dstDir); err != nil {
		t.Fatalf("extractTarZstd: %v", err)
	}

	// Verify expected files are present in the extracted archive.
	for _, rel := range []string{
		"caches/modules/entry.bin",
		"wrapper/dists/gradle-8.14.3-bin/abc123/gradle-8.14.3/lib/gradle-core.jar",
		"wrapper/dists/gradle-8.14.3-bin/abc123/gradle-8.14.3-bin.zip.ok",
		"configuration-cache/hash.bin",
		"buildSrc/build/libs/buildSrc.jar",
	} {
		path := filepath.Join(dstDir, rel)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected %s in extracted dir: %v", rel, err)
		}
	}

	// Verify excluded files are absent from the archive.
	for _, rel := range []string{
		"wrapper/dists/gradle-8.14.3-bin/abc123/gradle-8.14.3-bin.zip",
		"caches/8.14.3/cc-keystore/keystore.bin",
	} {
		if _, err := os.Stat(filepath.Join(dstDir, rel)); err == nil {
			t.Errorf("%s should have been excluded from archive", rel)
		}
	}

	// Verify file contents round-trip correctly.
	data, err := os.ReadFile(filepath.Join(dstDir, "caches", "modules", "entry.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "gradle data" {
		t.Errorf("content = %q, want %q", string(data), "gradle data")
	}
}

// TestTarZstdSymlinkDereference verifies that -h causes symlinked directories
// to be archived as real content.
func TestTarZstdSymlinkDereference(t *testing.T) {
	if _, err := exec.LookPath("tar"); err != nil {
		t.Skip("tar not available")
	}
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd not available")
	}

	ctx := context.Background()
	srcDir := t.TempDir()
	realDir := t.TempDir()

	// Write a file into the real directory.
	must(t, os.WriteFile(filepath.Join(realDir, "data.txt"), []byte("hello"), 0o644))

	// Create caches/ as a symlink pointing to realDir.
	cachesLink := filepath.Join(srcDir, "caches")
	must(t, os.Symlink(realDir, cachesLink))

	var buf bytes.Buffer
	if err := CreateTarZstd(ctx, &buf, []TarSource{{BaseDir: srcDir, Path: "./caches"}}); err != nil {
		t.Fatalf("CreateTarZstd: %v", err)
	}

	dstDir := t.TempDir()
	if err := extractTarZstd(ctx, &buf, dstDir); err != nil {
		t.Fatalf("extractTarZstd: %v", err)
	}

	// The symlink should have been dereferenced — extracted as a real directory.
	info, err := os.Lstat(filepath.Join(dstDir, "caches"))
	if err != nil {
		t.Fatalf("caches/ not found in extracted dir: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Error("expected caches/ to be a real directory, got symlink (tar -h not working)")
	}

	// The file inside should be present.
	if _, err := os.Stat(filepath.Join(dstDir, "caches", "data.txt")); err != nil {
		t.Errorf("expected data.txt inside extracted caches/: %v", err)
	}
}

// ─── branchSlug tests ────────────────────────────────────────────────────────

func TestBranchSlug(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"main", "main"},
		{"feature/my-pr", "feature--my-pr"},
		{"fix/JIRA-123", "fix--JIRA-123"},
		{"refs/heads/main", "refs--heads--main"},
		{"branch with spaces", "branch-with-spaces"},
		{"a#b?c&d", "a-b-c-d"},
		{"feature/foo/bar", "feature--foo--bar"},
	}
	for _, tt := range tests {
		if got := branchSlug(tt.input); got != tt.want {
			t.Errorf("branchSlug(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDeltaCommit(t *testing.T) {
	tests := []struct {
		branch, want string
	}{
		{"main", "branches/main"},
		{"feature/my-pr", "branches/feature--my-pr"},
	}
	for _, tt := range tests {
		if got := deltaCommit(tt.branch); got != tt.want {
			t.Errorf("deltaCommit(%q) = %q, want %q", tt.branch, got, tt.want)
		}
	}
}

// ─── Delta archive round-trip test ───────────────────────────────────────────

// TestDeltaTarZstdRoundTrip verifies that CreateDeltaTarZstd/writeDeltaTar pack
// only the files listed and that they can be extracted back via extractTarZstd.
// It also exercises the mtime-based file selection used by SaveDeltaCmd: a
// "base" file is written before the marker and a "new" file after, and only the
// new file should appear in the delta.
func TestDeltaTarZstdRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd not available")
	}

	ctx := context.Background()

	// Set up a fake GradleUserHome with a caches/ directory.
	gradleHome := t.TempDir()
	cachesDir := filepath.Join(gradleHome, "caches")
	must(t, os.MkdirAll(filepath.Join(cachesDir, "modules-2"), 0o755))

	// Write a "base" file that predates the marker.
	baseFile := filepath.Join(cachesDir, "modules-2", "base.jar")
	must(t, os.WriteFile(baseFile, []byte("base content"), 0o644))

	// Touch the marker.
	markerPath := filepath.Join(gradleHome, ".cache-restore-marker")
	must(t, touchMarkerFile(markerPath))

	// Write a "new" file after the marker — this is what the build created.
	newFile := filepath.Join(cachesDir, "modules-2", "new.jar")
	must(t, os.WriteFile(newFile, []byte("new content"), 0o644))

	// Determine which files are newer than the marker (simulating SaveDeltaCmd's scan).
	markerInfo, err := os.Stat(markerPath)
	must(t, err)
	since := markerInfo.ModTime()

	var newFiles []string
	must(t, filepath.Walk(cachesDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil || !fi.Mode().IsRegular() {
			return err
		}
		if fi.ModTime().After(since) {
			rel, _ := filepath.Rel(cachesDir, path)
			newFiles = append(newFiles, filepath.Join("caches", rel))
		}
		return nil
	}))

	if len(newFiles) == 0 {
		t.Skip("mtime resolution too coarse to distinguish marker from new file; skipping")
	}

	// Pack the delta.
	var buf bytes.Buffer
	must(t, CreateDeltaTarZstd(ctx, &buf, gradleHome, newFiles))

	// Extract into a fresh directory and verify only the new file is present.
	dstDir := t.TempDir()
	must(t, extractTarZstd(ctx, &buf, dstDir))

	newPath := filepath.Join(dstDir, "caches", "modules-2", "new.jar")
	if _, err := os.Stat(newPath); err != nil {
		t.Errorf("expected new.jar in delta: %v", err)
	}
	basePath := filepath.Join(dstDir, "caches", "modules-2", "base.jar")
	if _, err := os.Stat(basePath); err == nil {
		t.Error("base.jar should not be in delta — it predates the marker")
	}

	data, err := os.ReadFile(newPath)
	must(t, err)
	if string(data) != "new content" {
		t.Errorf("new.jar content = %q, want %q", string(data), "new content")
	}
}

func TestSaveDeltaDefaultsProjectDirToWorkingDirectory(t *testing.T) {
	ctx := context.Background()
	gradleHome := t.TempDir()
	projectDir := t.TempDir()

	cachesDir := filepath.Join(gradleHome, "caches", "modules-2")
	must(t, os.MkdirAll(cachesDir, 0o755))
	must(t, os.WriteFile(filepath.Join(cachesDir, "base.bin"), []byte("base"), 0o644))

	markerPath := filepath.Join(gradleHome, ".cache-restore-marker")
	must(t, touchMarkerFile(markerPath))

	ccDir := filepath.Join(projectDir, ".gradle", "configuration-cache")
	must(t, os.MkdirAll(ccDir, 0o755))
	must(t, os.WriteFile(filepath.Join(ccDir, "entry.bin"), []byte("cc"), 0o644))

	origWD, err := os.Getwd()
	must(t, err)
	must(t, os.Chdir(projectDir))
	defer func() {
		must(t, os.Chdir(origWD))
	}()

	var uploaded bytes.Buffer
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPost:
			_, err := io.Copy(&uploaded, r.Body)
			must(t, err)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	err = SaveDelta(ctx, SaveDeltaConfig{
		CachewURL:      srv.URL,
		CacheKey:       "test-cache",
		Branch:         "feature/test",
		GradleUserHome: gradleHome,
	})
	must(t, err)

	restoreDir := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(restoreDir, ".gradle"), 0o755))
	must(t, extractDeltaTarZstd(ctx, bytes.NewReader(uploaded.Bytes()), restoreDir, []TarSource{
		{BaseDir: filepath.Join(restoreDir, ".gradle"), Path: "./configuration-cache"},
	}))

	if _, err := os.Stat(filepath.Join(restoreDir, ".gradle", "configuration-cache", "entry.bin")); err != nil {
		t.Fatalf("expected configuration-cache entry in delta bundle: %v", err)
	}
}

func TestSaveDeltaErrorsWhenProjectDirHasNoDotGradle(t *testing.T) {
	ctx := context.Background()
	gradleHome := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(gradleHome, "caches"), 0o755))
	must(t, touchMarkerFile(filepath.Join(gradleHome, ".cache-restore-marker")))

	projectDir := t.TempDir()
	err := SaveDelta(ctx, SaveDeltaConfig{
		CachewURL:      "http://example.invalid",
		CacheKey:       "test-cache",
		Branch:         "feature/test",
		GradleUserHome: gradleHome,
		ProjectDir:     projectDir,
	})
	if err == nil {
		t.Fatal("expected SaveDelta to fail when project dir has no .gradle directory")
	}
	if !strings.Contains(err.Error(), "missing .gradle/") {
		t.Fatalf("expected missing .gradle error, got %v", err)
	}
}

func TestSaveErrorsWhenWorkingDirectoryHasNoDotGradle(t *testing.T) {
	ctx := context.Background()
	gradleHome := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(gradleHome, "caches"), 0o755))

	projectDir := t.TempDir()
	origWD, err := os.Getwd()
	must(t, err)
	must(t, os.Chdir(projectDir))
	defer func() {
		must(t, os.Chdir(origWD))
	}()

	err = Save(ctx, SaveConfig{
		CachewURL:      "http://example.invalid",
		CacheKey:       "test-cache",
		Commit:         strings.Repeat("a", 40),
		GradleUserHome: gradleHome,
		SkipWarm:       true,
	})
	if err == nil {
		t.Fatal("expected Save to fail when working directory has no .gradle directory")
	}
	if !strings.Contains(err.Error(), "missing .gradle/") {
		t.Fatalf("expected missing .gradle error, got %v", err)
	}
}

// ─── Delta scan benchmark ─────────────────────────────────────────────────────

// BenchmarkDeltaScan measures the mtime-walk hot path used by SaveDeltaCmd:
// EvalSymlinks + filepath.Walk + fi.ModTime().After(marker). The directory
// structure mirrors a real Gradle cache (nested group/artifact/version dirs).
//
// Run with:
//
//	go test -bench=BenchmarkDeltaScan -benchtime=5s ./cmd/gradle-cache/
//
// Output includes a "files/op" metric so ns/file is straightforward to derive.
func BenchmarkDeltaScan(b *testing.B) {
	for _, nFiles := range []int{5_000, 20_000, 50_000} {
		nFiles := nFiles
		b.Run(fmt.Sprintf("files=%d", nFiles), func(b *testing.B) {
			// Build a simulated Gradle cache:
			//   root/caches/group-N/artifact-N/vX.Y/file-N.jar
			// 50 groups × 20 artifacts gives ~1000 leaf dirs for 50k files.
			root := b.TempDir()
			caches := filepath.Join(root, "caches")
			for i := range nFiles {
				dir := filepath.Join(caches,
					fmt.Sprintf("group%d", i%50),
					fmt.Sprintf("artifact%d", i%20),
				)
				if err := os.MkdirAll(dir, 0o755); err != nil {
					b.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d.jar", i)), []byte("x"), 0o644); err != nil {
					b.Fatal(err)
				}
			}

			// Write marker after all "base" files — mirrors what restore does.
			markerPath := filepath.Join(root, ".cache-restore-marker")
			if err := touchMarkerFile(markerPath); err != nil {
				b.Fatal(err)
			}
			markerInfo, err := os.Stat(markerPath)
			if err != nil {
				b.Fatal(err)
			}
			since := markerInfo.ModTime()

			// Resolve the caches dir, just as SaveDeltaCmd does (it may be a symlink).
			realCaches, err := filepath.EvalSymlinks(caches)
			if err != nil {
				realCaches = caches
			}

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				var found int
				if walkErr := filepath.Walk(realCaches, func(path string, fi os.FileInfo, err error) error {
					if err != nil || !fi.Mode().IsRegular() {
						return err
					}
					if fi.ModTime().After(since) {
						found++
					}
					return nil
				}); walkErr != nil {
					b.Fatal(walkErr)
				}
				_ = found
			}
			b.ReportMetric(float64(nFiles), "files/op")
		})
	}
}

// BenchmarkDeltaScanReal exercises the production CollectNewFiles path against a real
// extracted cache. Point GRADLE_CACHE_BENCH_DIR at the caches/ directory from a prior
// restore (the symlink or its real target) and run:
//
//	GRADLE_CACHE_BENCH_DIR=~/.gradle/caches \
//	  go test -bench=BenchmarkDeltaScanReal -benchtime=3x ./cmd/gradle-cache/
func BenchmarkDeltaScanReal(b *testing.B) {
	cachesDir := os.Getenv("GRADLE_CACHE_BENCH_DIR")
	if cachesDir == "" {
		b.Skip("set GRADLE_CACHE_BENCH_DIR to a Gradle caches/ directory to run this benchmark")
	}

	realCaches, err := filepath.EvalSymlinks(cachesDir)
	if err != nil {
		b.Fatalf("EvalSymlinks(%s): %v", cachesDir, err)
	}
	gradleHome := filepath.Dir(realCaches) // parent of caches/ is the Gradle user home

	// Count total files once, outside the timed loop.
	var totalFiles int
	if err := filepath.Walk(realCaches, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() {
			totalFiles++
		}
		return nil
	}); err != nil {
		b.Fatalf("pre-count walk: %v", err)
	}
	b.Logf("cache: %d regular files at %s", totalFiles, realCaches)

	// Write the marker after all cache files so they all predate it — simulating
	// the "clean restore, no build has run yet" baseline.
	markerPath := filepath.Join(b.TempDir(), ".bench-marker")
	if err := touchMarkerFile(markerPath); err != nil {
		b.Fatal(err)
	}
	markerInfo, err := os.Stat(markerPath)
	if err != nil {
		b.Fatal(err)
	}
	since := markerInfo.ModTime()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		files, err := CollectNewFiles(realCaches, since, gradleHome)
		if err != nil {
			b.Fatal(err)
		}
		_ = files
	}
	b.ReportMetric(float64(totalFiles), "files/op")
}

// ─── Zip-slip / path-traversal tests ──────────────────────────────────────────

// TestExtractRejectsPathTraversal verifies that tar entries with ".." in the
// name or symlinks pointing outside the destination are rejected.
func TestExtractRejectsPathTraversal(t *testing.T) {
	// Helper: build a tar archive in memory from a list of entries.
	type entry struct {
		name     string
		typeflag byte
		linkname string
		body     string
	}
	buildTar := func(entries []entry) *bytes.Buffer {
		var buf bytes.Buffer
		tw := archive_tar.NewWriter(&buf)
		for _, e := range entries {
			hdr := &archive_tar.Header{
				Name:     e.name,
				Typeflag: e.typeflag,
				Mode:     0o644,
				Size:     int64(len(e.body)),
				Linkname: e.linkname,
			}
			if e.typeflag == archive_tar.TypeDir {
				hdr.Mode = 0o755
				hdr.Size = 0
			}
			must(t, tw.WriteHeader(hdr))
			if len(e.body) > 0 {
				_, err := tw.Write([]byte(e.body))
				must(t, err)
			}
		}
		must(t, tw.Close())
		return &buf
	}

	for _, tc := range []struct {
		name    string
		entries []entry
	}{
		{
			name: "dotdot_file",
			entries: []entry{
				{name: "../etc/passwd", typeflag: archive_tar.TypeReg, body: "pwned"},
			},
		},
		{
			name: "dotdot_nested_file",
			entries: []entry{
				{name: "foo/../../etc/passwd", typeflag: archive_tar.TypeReg, body: "pwned"},
			},
		},
		{
			name: "absolute_file",
			entries: []entry{
				{name: "/etc/passwd", typeflag: archive_tar.TypeReg, body: "pwned"},
			},
		},
		{
			name: "symlink_absolute_escape",
			entries: []entry{
				{name: "link", typeflag: archive_tar.TypeSymlink, linkname: "/etc/passwd"},
			},
		},
		{
			name: "symlink_relative_escape",
			entries: []entry{
				{name: "link", typeflag: archive_tar.TypeSymlink, linkname: "../../etc/passwd"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dstDir := t.TempDir()
			buf := buildTar(tc.entries)
			err := extractTarPlatform(bytes.NewReader(buf.Bytes()), dstDir)
			if err == nil {
				t.Fatal("expected error for path-traversal entry, got nil")
			}
			if !strings.Contains(err.Error(), "escapes") && !strings.Contains(err.Error(), "not allowed") {
				t.Fatalf("expected 'escapes' or 'not allowed' in error, got: %v", err)
			}
		})
	}
}

// TestExtractRoutedSymlinkWithinArchive verifies that symlinks between routed
// directories (e.g. configuration-cache/ → caches/) are accepted when they
// stay within the archive root, even though the two directories are extracted
// to different filesystem locations.
func TestExtractRoutedSymlinkWithinArchive(t *testing.T) {
	var buf bytes.Buffer
	tw := archive_tar.NewWriter(&buf)

	// A file in caches/ and a symlink in configuration-cache/ pointing to it.
	for _, e := range []struct {
		name, linkname, body string
		typeflag             byte
	}{
		{name: "caches/modules/foo.bin", body: "data", typeflag: archive_tar.TypeReg},
		{name: "configuration-cache/link", linkname: "../caches/modules/foo.bin", typeflag: archive_tar.TypeSymlink},
	} {
		hdr := &archive_tar.Header{
			Name:     e.name,
			Typeflag: e.typeflag,
			Mode:     0o644,
			Size:     int64(len(e.body)),
			Linkname: e.linkname,
		}
		must(t, tw.WriteHeader(hdr))
		if len(e.body) > 0 {
			_, err := tw.Write([]byte(e.body))
			must(t, err)
		}
	}
	must(t, tw.Close())

	gradleHome := t.TempDir()
	projectDir := t.TempDir()
	dotGradle := filepath.Join(projectDir, ".gradle")

	rules := []extractRule{
		{prefix: "caches/", baseDir: gradleHome},
		{prefix: "configuration-cache/", baseDir: dotGradle},
	}
	targetFn := func(name string) string {
		for _, rule := range rules {
			if strings.HasPrefix(name, rule.prefix) {
				return filepath.Join(rule.baseDir, name)
			}
		}
		return filepath.Join(projectDir, name)
	}

	err := extractTarPlatformRouted(bytes.NewReader(buf.Bytes()), targetFn, false)
	if err != nil {
		t.Fatalf("expected routed cross-directory symlink to succeed, got: %v", err)
	}
}

// ─── Extraction benchmark ─────────────────────────────────────────────────────

// BenchmarkExtract measures extractTarPlatformRouted throughput against a
// synthetic tar archive that mimics the structure and file-size distribution of
// a real Gradle cache bundle: many small metadata/index files (~1 KB) and a
// smaller number of large jar files (~500 KB). Routing is exercised by
// including both caches/ and configuration-cache/ entries.
//
// Run with:
//
//	go test -bench=BenchmarkExtract -benchtime=3x ./cmd/gradle-cache/
//
// Output includes files/op and MB/op so you can derive ns/file and MB/s.
func BenchmarkExtract(b *testing.B) {
	for _, tc := range []struct {
		name       string
		smallFiles int // ~1 KB each (metadata, index, lock files)
		largeFiles int // ~512 KB each (jars)
		includeCC  bool
	}{
		{"small_5k", 5_000, 0, false},
		{"mixed_5k_small_500_large", 5_000, 500, false},
		{"mixed_with_cc", 5_000, 500, true},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			// Build the tar in memory once; each iteration re-extracts the same bytes.
			tarBuf := buildSyntheticTar(b, tc.smallFiles, tc.largeFiles, tc.includeCC)
			totalBytes := int64(tarBuf.Len())

			destHome := b.TempDir()
			destProject := b.TempDir()

			rules := []extractRule{
				{prefix: "caches/", baseDir: destHome},
				{prefix: "configuration-cache/", baseDir: filepath.Join(destProject, ".gradle")},
			}
			targetFn := func(name string) string {
				for _, rule := range rules {
					if strings.HasPrefix(name, rule.prefix) {
						return filepath.Join(rule.baseDir, name)
					}
				}
				return filepath.Join(destProject, name)
			}

			nFiles := tc.smallFiles + tc.largeFiles
			if tc.includeCC {
				nFiles += 10
			}

			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				// Each iteration extracts into a fresh directory so we're not
				// benchmarking the skipExisting fast-path.
				iterHome := b.TempDir()
				iterProject := b.TempDir()
				iterRules := []extractRule{
					{prefix: "caches/", baseDir: iterHome},
					{prefix: "configuration-cache/", baseDir: filepath.Join(iterProject, ".gradle")},
				}
				iterFn := func(name string) string {
					for _, rule := range iterRules {
						if strings.HasPrefix(name, rule.prefix) {
							return filepath.Join(rule.baseDir, name)
						}
					}
					return filepath.Join(iterProject, name)
				}
				_ = targetFn // suppress unused warning from outer scope
				if err := extractTarPlatformRouted(bytes.NewReader(tarBuf.Bytes()), iterFn, false); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportMetric(float64(nFiles), "files/op")
			b.ReportMetric(float64(totalBytes)/1e6, "MB/op")
		})
	}
}

// BenchmarkExtractLargeFiles benchmarks the large-file streaming path
// (files > maxBufferedFileSize = 4 MB). These files are written inline on the
// reader goroutine via io.Copy rather than being buffered in memory and
// dispatched to a worker.
func BenchmarkExtractLargeFiles(b *testing.B) {
	for _, tc := range []struct {
		name       string
		smallFiles int
		bigFiles   int
		bigSizeMB  int
	}{
		{"100x_8MB", 0, 100, 8},
		{"1k_small_20x_8MB", 1_000, 20, 8},
		{"1k_small_5x_32MB", 1_000, 5, 32},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			bigSize := tc.bigSizeMB << 20
			smallData := bytes.Repeat([]byte{0x42}, 1024)
			bigData := bytes.Repeat([]byte{0x55}, bigSize)

			var buf bytes.Buffer
			tw := archive_tar.NewWriter(&buf)
			writeE := func(name string, data []byte) {
				b.Helper()
				hdr := &archive_tar.Header{
					Typeflag: archive_tar.TypeReg,
					Name:     name,
					Size:     int64(len(data)),
					Mode:     0o644,
				}
				if err := tw.WriteHeader(hdr); err != nil {
					b.Fatalf("write header: %v", err)
				}
				if _, err := tw.Write(data); err != nil {
					b.Fatalf("write data: %v", err)
				}
			}
			for i := range tc.smallFiles {
				writeE(fmt.Sprintf("caches/8.14.3/meta/f%d.index", i), smallData)
			}
			for i := range tc.bigFiles {
				writeE(fmt.Sprintf("caches/8.14.3/jars/group%d/fat-%d.jar", i%5, i), bigData)
			}
			if err := tw.Close(); err != nil {
				b.Fatalf("close tar: %v", err)
			}

			totalBytes := int64(buf.Len())
			nFiles := tc.smallFiles + tc.bigFiles
			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				iterHome := b.TempDir()
				iterFn := func(name string) string {
					return filepath.Join(iterHome, name)
				}
				if err := extractTarPlatformRouted(bytes.NewReader(buf.Bytes()), iterFn, false); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(nFiles), "files/op")
			b.ReportMetric(float64(totalBytes)/1e6, "MB/op")
		})
	}
}

// buildSyntheticTar builds an uncompressed tar archive in memory with
// smallFiles entries of ~1 KB and largeFiles entries of ~512 KB under caches/,
// plus 10 configuration-cache entries if includeCC is true. The data is
// deterministic (repeated 0x42 bytes) so it compresses well but still exercises
// the full write path.
func buildSyntheticTar(b *testing.B, smallFiles, largeFiles int, includeCC bool) *bytes.Buffer {
	b.Helper()

	const smallSize = 1024
	const largeSize = 512 * 1024

	smallData := bytes.Repeat([]byte{0x42}, smallSize)
	largeData := bytes.Repeat([]byte{0x55}, largeSize)

	var buf bytes.Buffer
	tw := archive_tar.NewWriter(&buf)

	writeEntry := func(name string, data []byte) {
		b.Helper()
		hdr := &archive_tar.Header{
			Typeflag: archive_tar.TypeReg,
			Name:     name,
			Size:     int64(len(data)),
			Mode:     0o644,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			b.Fatalf("write tar header %s: %v", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			b.Fatalf("write tar data %s: %v", name, err)
		}
	}

	for i := range smallFiles {
		writeEntry(fmt.Sprintf("caches/8.14.3/group%d/artifact%d/f%d.index", i%50, i%20, i), smallData)
	}
	for i := range largeFiles {
		writeEntry(fmt.Sprintf("caches/8.14.3/jars-%d/group%d/artifact-%d.jar", i%10, i%30, i), largeData)
	}
	if includeCC {
		for i := range 10 {
			writeEntry(fmt.Sprintf("configuration-cache/entry-%d/work.bin", i), smallData)
		}
	}

	if err := tw.Close(); err != nil {
		b.Fatalf("close tar: %v", err)
	}
	return &buf
}

// BenchmarkExtractVsSymlink compares the current direct-extraction approach
// against the old extract-to-tmpDir+symlink approach on the same synthetic
// bundle. Both sub-benchmarks write identical bytes; the difference is whether
// extraction targets the final directory directly or a sibling staging dir that
// is then symlinked into place.
//
// Run with:
//
//	go test -bench=BenchmarkExtractVsSymlink -benchtime=3x ./cmd/gradle-cache/
func BenchmarkExtractVsSymlink(b *testing.B) {
	for _, tc := range []struct {
		name       string
		smallFiles int
		largeFiles int
		includeCC  bool
	}{
		{"small_5k", 5_000, 0, false},
		{"mixed_5k_small_500_large", 5_000, 500, false},
		{"mixed_with_cc", 5_000, 500, true},
	} {
		tc := tc
		tarBuf := buildSyntheticTar(b, tc.smallFiles, tc.largeFiles, tc.includeCC)
		totalBytes := int64(tarBuf.Len())
		nFiles := tc.smallFiles + tc.largeFiles
		if tc.includeCC {
			nFiles += 10
		}

		// ── direct: extract straight to final destinations ──────────────────
		b.Run(tc.name+"/direct", func(b *testing.B) {
			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				gradleHome := b.TempDir()
				projectDir := b.TempDir()
				rules := []extractRule{
					{prefix: "caches/", baseDir: gradleHome},
					{prefix: "configuration-cache/", baseDir: filepath.Join(projectDir, ".gradle")},
				}
				targetFn := func(name string) string {
					for _, rule := range rules {
						if strings.HasPrefix(name, rule.prefix) {
							return filepath.Join(rule.baseDir, name)
						}
					}
					return filepath.Join(projectDir, name)
				}
				if err := extractTarPlatformRouted(bytes.NewReader(tarBuf.Bytes()), targetFn, false); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(nFiles), "files/op")
			b.ReportMetric(float64(totalBytes)/1e6, "MB/op")
		})

		// ── tmp+symlink: extract to sibling staging dir, then symlink ────────
		// Mirrors the old approach: MkdirTemp alongside gradleHome, extract
		// everything flat, then os.Symlink(tmpDir/caches, gradleHome/caches)
		// and os.Symlink(tmpDir/configuration-cache, project/.gradle/cc).
		b.Run(tc.name+"/tmp_symlink", func(b *testing.B) {
			b.SetBytes(totalBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				gradleHome := b.TempDir()
				projectDir := b.TempDir()

				// Stage into a sibling of gradleHome (same filesystem → rename/symlink is instant).
				tmpDir, err := os.MkdirTemp(filepath.Dir(gradleHome), "gradle-cache-bench-*")
				if err != nil {
					b.Fatal(err)
				}

				if err := extractTarPlatform(bytes.NewReader(tarBuf.Bytes()), tmpDir); err != nil {
					os.RemoveAll(tmpDir) //nolint:errcheck
					b.Fatal(err)
				}

				// Symlink caches/ into gradleHome.
				if err := os.Symlink(filepath.Join(tmpDir, "caches"), filepath.Join(gradleHome, "caches")); err != nil {
					os.RemoveAll(tmpDir) //nolint:errcheck
					b.Fatal(err)
				}

				// Symlink configuration-cache/ into project/.gradle/.
				if tc.includeCC {
					if err := os.MkdirAll(filepath.Join(projectDir, ".gradle"), 0o750); err != nil {
						os.RemoveAll(tmpDir) //nolint:errcheck
						b.Fatal(err)
					}
					if err := os.Symlink(
						filepath.Join(tmpDir, "configuration-cache"),
						filepath.Join(projectDir, ".gradle", "configuration-cache"),
					); err != nil {
						os.RemoveAll(tmpDir) //nolint:errcheck
						b.Fatal(err)
					}
				}
				// Leave tmpDir in place — symlinks point into it, same as old behaviour.
			}
			b.ReportMetric(float64(nFiles), "files/op")
			b.ReportMetric(float64(totalBytes)/1e6, "MB/op")
		})
	}
}

// ─── S3 bundle store tests ────────────────────────────────────────────────────

// fakeS3 is a minimal in-memory S3-compatible HTTP server for testing.
// It supports HEAD, GET (with Range header), single-part PUT, and S3 multipart
// upload (CreateMultipartUpload, UploadPart, CompleteMultipartUpload).
type fakeS3 struct {
	mu        sync.Mutex
	objects   map[string][]byte         // finalized objects
	mpUploads map[string]map[int][]byte // uploadID → partNum → data (in-progress)
	mpKeys    map[string]string         // uploadID → object key
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects:   make(map[string][]byte),
		mpUploads: make(map[string]map[int][]byte),
		mpKeys:    make(map[string]string),
	}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	q := r.URL.Query()

	switch {
	case r.Method == http.MethodHead:
		f.mu.Lock()
		_, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodGet:
		f.mu.Lock()
		data, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
			var start, end int64
			if n, _ := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end); n != 2 {
				http.Error(w, "bad range", http.StatusBadRequest)
				return
			}
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start : end+1])
		} else {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			_, _ = w.Write(data)
		}

	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		// UploadPart
		partNum := 0
		fmt.Sscanf(q.Get("partNumber"), "%d", &partNum) //nolint:errcheck
		body, _ := io.ReadAll(r.Body)
		uploadID := q.Get("uploadId")
		f.mu.Lock()
		if f.mpUploads[uploadID] == nil {
			f.mpUploads[uploadID] = make(map[int][]byte)
		}
		f.mpUploads[uploadID][partNum] = body
		f.mu.Unlock()
		w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, partNum))
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPut:
		// Single-part PUT
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.objects[key] = body
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPost && q.Has("uploads"):
		// CreateMultipartUpload
		uploadID := fmt.Sprintf("upload-%d", len(f.mpUploads))
		f.mu.Lock()
		f.mpUploads[uploadID] = make(map[int][]byte)
		f.mpKeys[uploadID] = key
		f.mu.Unlock()
		_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, uploadID)

	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		// CompleteMultipartUpload: assemble parts in order
		uploadID := q.Get("uploadId")
		f.mu.Lock()
		parts := f.mpUploads[uploadID]
		objKey := f.mpKeys[uploadID]
		delete(f.mpUploads, uploadID)
		delete(f.mpKeys, uploadID)
		f.mu.Unlock()
		// Sort part numbers and concatenate.
		maxPart := 0
		for pn := range parts {
			if pn > maxPart {
				maxPart = pn
			}
		}
		var assembled []byte
		for i := 1; i <= maxPart; i++ {
			assembled = append(assembled, parts[i]...)
		}
		f.mu.Lock()
		f.objects[objKey] = assembled
		f.mu.Unlock()
		_, _ = fmt.Fprintf(w, `<CompleteMultipartUploadResult><Key>%s</Key></CompleteMultipartUploadResult>`, objKey)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// newTestS3BundleStore returns an s3BundleStore pointing at a fake S3 server.
func newTestS3BundleStore(server *httptest.Server) *s3BundleStore {
	client := &s3Client{
		region:      "us-east-1",
		http:        server.Client(),
		chunkSize:   defaultDownloadChunkSize,
		dlWorkers:   defaultDownloadWorkers,
		testBaseURL: server.URL,
	}
	return &s3BundleStore{client: client, bucket: "test-bucket"}
}

func TestS3BundleStoreRoundTrip(t *testing.T) {
	fs := newFakeS3()
	srv := httptest.NewServer(fs)
	defer srv.Close()
	store := newTestS3BundleStore(srv)

	ctx := context.Background()
	commit := "abc1234567890000000000000000000000000000"
	cacheKey := "apos-beta"
	payload := []byte("bundle contents")

	legacyKey := "test-bucket/" + s3Key("", commit, cacheKey, bundleFilename(cacheKey))
	fs.mu.Lock()
	fs.objects[legacyKey] = payload
	fs.mu.Unlock()

	info, err := store.stat(ctx, commit, cacheKey)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	r, err := store.get(ctx, commit, cacheKey, info)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer r.Close() //nolint:errcheck
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("got %q, want %q", got, payload)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
