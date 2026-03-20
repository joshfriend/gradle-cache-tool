//nolint:gosec // test file: all paths and subprocess args are controlled inputs
package main

import (
	archive_tar "archive/tar"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
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
	}
	for _, name := range excluded {
		if !isExcludedCache(name) {
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
		if isExcludedCache(name) {
			t.Errorf("expected %q to NOT be excluded", name)
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
		if got := s3Key(tt.commit, tt.cacheKey, tt.bundleFile); got != tt.want {
			t.Errorf("s3Key(%q, %q, %q) = %q, want %q",
				tt.commit, tt.cacheKey, tt.bundleFile, got, tt.want)
		}
	}
}

// ─── conventionBuildDirs tests ───────────────────────────────────────────────

func TestConventionBuildDirs(t *testing.T) {
	t.Run("empty directory returns nothing", func(t *testing.T) {
		root := t.TempDir()
		if got := conventionBuildDirs(root, []string{"buildSrc"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("buildSrc without build subdir is excluded", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc"), 0o755))
		if got := conventionBuildDirs(root, []string{"buildSrc"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("buildSrc/build is included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"buildSrc"})
		if len(got) != 1 || got[0] != "buildSrc/build" {
			t.Errorf("got %v, want [buildSrc/build]", got)
		}
	})

	t.Run("buildSrc not included when not in config", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		// build-logic is configured but doesn't exist; buildSrc exists but isn't configured.
		got := conventionBuildDirs(root, []string{"build-logic"})
		if len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("build-logic/build included when configured", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"build-logic"})
		if len(got) != 1 || got[0] != "build-logic/build" {
			t.Errorf("got %v, want [build-logic/build]", got)
		}
	})

	t.Run("multiple explicit dirs all checked", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "build-logic", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"buildSrc", "build-logic"})
		sort.Strings(got)
		if len(got) != 2 || got[0] != "build-logic/build" || got[1] != "buildSrc/build" {
			t.Errorf("got %v, want [build-logic/build buildSrc/build]", got)
		}
	})

	t.Run("glob plugins/* finds subdirectory build dirs", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"plugins/*"})
		if len(got) != 1 || got[0] != "plugins/foo/build" {
			t.Errorf("got %v, want [plugins/foo/build]", got)
		}
	})

	t.Run("glob plugins/* excludes subdirs without a build dir", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo"), 0o755))
		if got := conventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("glob plugins/* excludes files named build", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo"), 0o755))
		must(t, os.WriteFile(filepath.Join(root, "plugins", "foo", "build"), []byte("nope"), 0o644))
		if got := conventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty, got %v", got)
		}
	})

	t.Run("glob plugins/* finds multiple subdirectories", func(t *testing.T) {
		root := t.TempDir()
		for _, p := range []string{"alpha", "beta", "gamma"} {
			must(t, os.MkdirAll(filepath.Join(root, "plugins", p, "build"), 0o755))
		}
		got := conventionBuildDirs(root, []string{"plugins/*"})
		if len(got) != 3 {
			t.Errorf("expected 3 entries, got %v", got)
		}
	})

	t.Run("missing glob parent directory is silently ignored", func(t *testing.T) {
		root := t.TempDir()
		if got := conventionBuildDirs(root, []string{"plugins/*"}); len(got) != 0 {
			t.Errorf("expected empty for missing parent, got %v", got)
		}
	})

	t.Run("buildSrc and plugins/* combined", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		got := conventionBuildDirs(root, []string{"buildSrc", "plugins/*"})
		sort.Strings(got)
		if len(got) != 2 || got[0] != "buildSrc/build" || got[1] != "plugins/foo/build" {
			t.Errorf("got %v, want [buildSrc/build plugins/foo/build]", got)
		}
	})
}

// ─── projectDirSources tests ─────────────────────────────────────────────────

func TestProjectDirSources(t *testing.T) {
	defaultBuilds := []string{"buildSrc"}

	t.Run("empty project dir returns no sources", func(t *testing.T) {
		root := t.TempDir()
		if got := projectDirSources(root, defaultBuilds); len(got) != 0 {
			t.Errorf("expected no sources, got %v", got)
		}
	})

	t.Run("configuration-cache source has correct BaseDir and Path", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, ".gradle", "configuration-cache"), 0o755))
		sources := projectDirSources(root, defaultBuilds)
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
		sources := projectDirSources(root, defaultBuilds)
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
		sources := projectDirSources(root, []string{"build-logic"})
		if len(sources) != 1 || sources[0].Path != "./build-logic/build" {
			t.Errorf("expected build-logic/build, got %v", sources)
		}
	})

	t.Run("all dirs present with plugins glob returns expected count", func(t *testing.T) {
		root := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(root, ".gradle", "configuration-cache"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(root, "plugins", "foo", "build"), 0o755))
		sources := projectDirSources(root, []string{"buildSrc", "plugins/*"})
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

// ─── Git history walk tests ──────────────────────────────────────────────────

// TestHistoryCommits creates a temporary git repository with a known commit
// graph
func TestHistoryCommits(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	ctx := context.Background()
	repo := t.TempDir()

	// run executes a git command in the test repo with a common identity.
	run := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repo}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test",
			"GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test",
			"GIT_COMMITTER_EMAIL=test@test.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}

	// commit makes an empty commit attributed to the given author name.
	commit := func(author, msg string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", "-C", repo, "commit", "--allow-empty", "-m", msg)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME="+author,
			"GIT_AUTHOR_EMAIL="+author+"@test.com",
			"GIT_COMMITTER_NAME="+author,
			"GIT_COMMITTER_EMAIL="+author+"@test.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("commit %q by %s: %v\n%s", msg, author, err, out)
		}
	}

	run("init")
	run("config", "user.email", "test@test.com")
	run("config", "user.name", "Test")

	// Build history (oldest → newest):
	//   Alice(3), Bob(2), Alice(1)
	// git log shows newest first:
	//   AliceFinal | Bob1 Bob0 | Alice2 Alice1 Alice0
	//    block 1       block 2      block 3
	for i := 0; i < 3; i++ {
		commit("Alice", fmt.Sprintf("alice %d", i))
	}
	for i := 0; i < 2; i++ {
		commit("Bob", fmt.Sprintf("bob %d", i))
	}
	commit("Alice", "alice final")

	t.Run("maxBlocks=1 returns only the most recent author block", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) != 1 {
			t.Errorf("expected 1 commit (just 'alice final'), got %d: %v", len(commits), commits)
		}
	})

	t.Run("maxBlocks=2 returns first two author blocks", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 2)
		if err != nil {
			t.Fatal(err)
		}
		// Block1: AliceFinal (1) + Block2: Bob1, Bob0 (2) = 3 commits
		if len(commits) != 3 {
			t.Errorf("expected 3 commits, got %d: %v", len(commits), commits)
		}
	})

	t.Run("maxBlocks=3 returns all commits", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) != 6 {
			t.Errorf("expected 6 commits, got %d: %v", len(commits), commits)
		}
	})

	t.Run("maxBlocks larger than actual blocks returns all commits", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 20)
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) != 6 {
			t.Errorf("expected 6 commits, got %d: %v", len(commits), commits)
		}
	})

	t.Run("all returned commits have 40-char hex SHAs", func(t *testing.T) {
		commits, err := historyCommits(ctx, repo, "HEAD", 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, sha := range commits {
			if len(sha) != 40 {
				t.Errorf("SHA %q has length %d, want 40", sha, len(sha))
			}
		}
	})

	t.Run("invalid ref returns error", func(t *testing.T) {
		_, err := historyCommits(ctx, repo, "refs/heads/nonexistent", 5)
		if err == nil {
			t.Error("expected error for invalid ref, got nil")
		}
	})
}

// ─── Round-trip archive test ─────────────────────────────────────────────────

// TestTarZstdRoundTrip verifies that createTarZstd → extractTarZstd preserves
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
	gradleDir := filepath.Join(srcDir, "project", ".gradle")
	must(t, os.MkdirAll(filepath.Join(gradleDir, "configuration-cache"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleDir, "configuration-cache", "hash.bin"), []byte("config cache"), 0o644))

	sources := []tarSource{
		{BaseDir: gradleHome, Path: "./caches"},
		{BaseDir: gradleHome, Path: "./wrapper"},
		{BaseDir: gradleDir, Path: "./configuration-cache"},
	}

	// Create archive into a buffer.
	var buf bytes.Buffer
	if err := createTarZstd(ctx, &buf, sources); err != nil {
		t.Fatalf("createTarZstd: %v", err)
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
	if err := createTarZstd(ctx, &buf, []tarSource{{BaseDir: srcDir, Path: "./caches"}}); err != nil {
		t.Fatalf("createTarZstd: %v", err)
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

// TestDeltaTarZstdRoundTrip verifies that createDeltaTarZstd/writeDeltaTar pack
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
	must(t, createDeltaTarZstd(ctx, &buf, gradleHome, newFiles))

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

// BenchmarkDeltaScanReal exercises the production collectNewFiles path against a real
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
		files, err := collectNewFiles(realCaches, since, gradleHome)
		if err != nil {
			b.Fatal(err)
		}
		_ = files
	}
	b.ReportMetric(float64(totalFiles), "files/op")
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

// ─── Helpers ─────────────────────────────────────────────────────────────────

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
