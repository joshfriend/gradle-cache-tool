//nolint:gosec // test file: all paths and subprocess args are controlled inputs
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

// ─── Pure unit tests ────────────────────────────────────────────────────────

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

// ─── restoreProjectDirs tests ────────────────────────────────────────────────

func TestRestoreProjectDirs(t *testing.T) {
	defaultBuilds := []string{"buildSrc"}

	t.Run("no project dirs in bundle — no error, nothing created", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		if err := restoreProjectDirs(tmpDir, projectDir, defaultBuilds); err != nil {
			t.Fatal(err)
		}
		entries, _ := os.ReadDir(projectDir)
		if len(entries) != 0 {
			t.Errorf("expected empty project dir, got %v", entries)
		}
	})

	t.Run("configuration-cache symlinked into .gradle/", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		srcCC := filepath.Join(tmpDir, "configuration-cache")
		must(t, os.Mkdir(srcCC, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, defaultBuilds))

		dst := filepath.Join(projectDir, ".gradle", "configuration-cache")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != srcCC {
			t.Errorf("symlink target = %q, want %q", target, srcCC)
		}
	})

	t.Run("buildSrc/build symlinked into project", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		src := filepath.Join(tmpDir, "buildSrc", "build")
		must(t, os.MkdirAll(src, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"buildSrc"}))

		dst := filepath.Join(projectDir, "buildSrc", "build")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != src {
			t.Errorf("symlink target = %q, want %q", target, src)
		}
	})

	t.Run("build-logic/build symlinked when configured", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		src := filepath.Join(tmpDir, "build-logic", "build")
		must(t, os.MkdirAll(src, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"build-logic"}))

		dst := filepath.Join(projectDir, "build-logic", "build")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != src {
			t.Errorf("symlink target = %q, want %q", target, src)
		}
	})

	t.Run("build-logic not restored when not in config", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		// build-logic is in the bundle but not configured.
		must(t, os.MkdirAll(filepath.Join(tmpDir, "build-logic", "build"), 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"buildSrc"}))

		dst := filepath.Join(projectDir, "build-logic", "build")
		if _, err := os.Lstat(dst); err == nil {
			t.Errorf("build-logic/build should not have been restored when not configured")
		}
	})

	t.Run("plugins/foo/build symlinked via glob", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		src := filepath.Join(tmpDir, "plugins", "foo", "build")
		must(t, os.MkdirAll(src, 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"plugins/*"}))

		dst := filepath.Join(projectDir, "plugins", "foo", "build")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s: %v", dst, err)
		}
		if target != src {
			t.Errorf("symlink target = %q, want %q", target, src)
		}
	})

	t.Run("existing directory at destination is replaced by symlink", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		srcCC := filepath.Join(tmpDir, "configuration-cache")
		must(t, os.Mkdir(srcCC, 0o755))
		// Pre-create destination as a real directory (simulating a prior run).
		must(t, os.MkdirAll(filepath.Join(projectDir, ".gradle", "configuration-cache"), 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, defaultBuilds))

		dst := filepath.Join(projectDir, ".gradle", "configuration-cache")
		target, err := os.Readlink(dst)
		if err != nil {
			t.Fatalf("expected symlink at %s after replacement: %v", dst, err)
		}
		if target != srcCC {
			t.Errorf("symlink target = %q, want %q", target, srcCC)
		}
	})

	t.Run("all dirs present with correct config — all symlinked", func(t *testing.T) {
		tmpDir := t.TempDir()
		projectDir := t.TempDir()
		must(t, os.MkdirAll(filepath.Join(tmpDir, "configuration-cache"), 0o755))
		must(t, os.MkdirAll(filepath.Join(tmpDir, "buildSrc", "build"), 0o755))
		must(t, os.MkdirAll(filepath.Join(tmpDir, "plugins", "bar", "build"), 0o755))

		must(t, restoreProjectDirs(tmpDir, projectDir, []string{"buildSrc", "plugins/*"}))

		for _, dst := range []string{
			filepath.Join(projectDir, ".gradle", "configuration-cache"),
			filepath.Join(projectDir, "buildSrc", "build"),
			filepath.Join(projectDir, "plugins", "bar", "build"),
		} {
			if _, err := os.Readlink(dst); err != nil {
				t.Errorf("expected symlink at %s: %v", dst, err)
			}
		}
	})
}

// ─── Git history walk tests ──────────────────────────────────────────────────

// TestHistoryCommits creates a temporary git repository with a known commit
// graph and verifies that the author-block counting logic matches the
// bundled-cache-manager.rb algorithm.
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

// ─── extractTar parallel extraction tests ────────────────────────────────────

func TestExtractTar(t *testing.T) {
	t.Run("regular file and directory are extracted", func(t *testing.T) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		must(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: "subdir/", Mode: 0o755}))
		data := []byte("hello world")
		must(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "subdir/hello.txt", Mode: 0o644, Size: int64(len(data))}))
		_, err := tw.Write(data)
		must(t, err)
		must(t, tw.Close())

		dir := t.TempDir()
		must(t, extractTar(&buf, dir))

		got, err := os.ReadFile(filepath.Join(dir, "subdir", "hello.txt"))
		if err != nil {
			t.Fatalf("read extracted file: %v", err)
		}
		if string(got) != "hello world" {
			t.Errorf("content = %q, want %q", string(got), "hello world")
		}
	})

	t.Run("symlink is created", func(t *testing.T) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		data := []byte("target content")
		must(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "real.txt", Mode: 0o644, Size: int64(len(data))}))
		_, err := tw.Write(data)
		must(t, err)
		must(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeSymlink, Name: "link.txt", Linkname: "real.txt"}))
		must(t, tw.Close())

		dir := t.TempDir()
		must(t, extractTar(&buf, dir))

		target, err := os.Readlink(filepath.Join(dir, "link.txt"))
		if err != nil {
			t.Fatalf("readlink: %v", err)
		}
		if target != "real.txt" {
			t.Errorf("symlink target = %q, want %q", target, "real.txt")
		}
	})

	t.Run("multiple files extracted in parallel", func(t *testing.T) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		for i := range 20 {
			name := fmt.Sprintf("file%02d.txt", i)
			data := []byte(fmt.Sprintf("content %d", i))
			must(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(data))}))
			_, err := tw.Write(data)
			must(t, err)
		}
		must(t, tw.Close())

		dir := t.TempDir()
		must(t, extractTar(&buf, dir))

		for i := range 20 {
			name := fmt.Sprintf("file%02d.txt", i)
			got, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Errorf("read %s: %v", name, err)
				continue
			}
			want := fmt.Sprintf("content %d", i)
			if string(got) != want {
				t.Errorf("%s: content = %q, want %q", name, string(got), want)
			}
		}
	})

	t.Run("path traversal is rejected", func(t *testing.T) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		data := []byte("evil")
		must(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "../escape.txt", Mode: 0o644, Size: int64(len(data))}))
		_, err := tw.Write(data)
		must(t, err)
		must(t, tw.Close())

		dir := t.TempDir()
		if err := extractTar(&buf, dir); err == nil {
			t.Error("expected error for path traversal entry, got nil")
		}
	})

	t.Run("parent directories are created implicitly", func(t *testing.T) {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		// No explicit directory entry — parent must be created by the worker.
		data := []byte("nested")
		must(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "a/b/c/file.txt", Mode: 0o644, Size: int64(len(data))}))
		_, err := tw.Write(data)
		must(t, err)
		must(t, tw.Close())

		dir := t.TempDir()
		must(t, extractTar(&buf, dir))

		got, err := os.ReadFile(filepath.Join(dir, "a", "b", "c", "file.txt"))
		if err != nil {
			t.Fatalf("read nested file: %v", err)
		}
		if string(got) != "nested" {
			t.Errorf("content = %q, want %q", string(got), "nested")
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

	// configuration-cache/ source (under .gradle/ inside project)
	gradleDir := filepath.Join(srcDir, "project", ".gradle")
	must(t, os.MkdirAll(filepath.Join(gradleDir, "configuration-cache"), 0o755))
	must(t, os.WriteFile(filepath.Join(gradleDir, "configuration-cache", "hash.bin"), []byte("config cache"), 0o644))

	sources := []tarSource{
		{BaseDir: gradleHome, Path: "./caches"},
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

	// Verify both source trees are present at the bundle root level.
	for _, rel := range []string{
		"caches/modules/entry.bin",
		"configuration-cache/hash.bin",
	} {
		path := filepath.Join(dstDir, rel)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected %s in extracted dir: %v", rel, err)
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
// to be archived as real content (matching bundled-cache-manager.rb's -h flag).
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

// ─── extractTar parallelism benchmarks ──────────────────────────────────────

// BenchmarkExtractTarParallelism sweeps over worker counts and two realistic
// Gradle cache profiles (many small files, fewer larger files) so you can see
// where the parallel write pool stops helping on the current machine.
//
// Run with:
//
//	go test -run='^$' -bench=BenchmarkExtractTarParallelism -benchtime=3s -count=3
func BenchmarkExtractTarParallelism(b *testing.B) {
	profiles := []struct {
		name      string
		fileCount int
		fileSize  int
	}{
		{"100x1KB", 100, 1 << 10},
		{"1000x1KB", 1000, 1 << 10},
		{"100x64KB", 100, 64 << 10},
		{"1000x64KB", 1000, 64 << 10},
	}

	workerCounts := []int{1, 2, 4, 8, 16, 32, 64}

	for _, p := range profiles {
		// Build the tar archive once; reuse it for every sub-benchmark.
		tarData := makeBenchTar(b, p.fileCount, p.fileSize)

		for _, n := range workerCounts {
			b.Run(fmt.Sprintf("%s/workers=%d", p.name, n), func(b *testing.B) {
				b.SetBytes(int64(p.fileCount * p.fileSize))
				b.ResetTimer()
				for range b.N {
					dir := b.TempDir()
					if err := extractTarN(bytes.NewReader(tarData), dir, n); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// makeBenchTar builds an in-memory tar archive with count files of the given size.
func makeBenchTar(b *testing.B, count, size int) []byte {
	b.Helper()
	payload := bytes.Repeat([]byte("x"), size)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i := range count {
		name := fmt.Sprintf("file%04d.bin", i)
		must2(b, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     0o644,
			Size:     int64(size),
		}))
		_, err := tw.Write(payload)
		must2(b, err)
	}
	must2(b, tw.Close())
	return buf.Bytes()
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func must2(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
