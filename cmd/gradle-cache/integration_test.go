//nolint:gosec // test file: paths and subprocess args are controlled inputs
package main

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fileBundleStore is a file-system-backed bundleStore for integration tests.
type fileBundleStore struct {
	dir string
}

func (f *fileBundleStore) path(commit, cacheKey string) string {
	return filepath.Join(f.dir, commit, bundleFilename(cacheKey))
}

func (f *fileBundleStore) stat(_ context.Context, commit, cacheKey string) (int64, error) {
	fi, err := os.Stat(f.path(commit, cacheKey))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (f *fileBundleStore) get(_ context.Context, commit, cacheKey string, _ int64) (io.ReadCloser, error) {
	return os.Open(f.path(commit, cacheKey))
}

func (f *fileBundleStore) put(_ context.Context, commit, cacheKey string, r io.ReadSeeker, _ int64) error {
	p := f.path(commit, cacheKey)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

// copyDir recursively copies src to dst, preserving file modes.
func copyDir(dst, src string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
}

// TestIntegrationGradleBuildCycle exercises the full save/restore cycle with a
// real Gradle build. It verifies that after restoring a saved cache, the
// configuration cache reports a hit and build tasks are UP-TO-DATE.
//
// The test uses a committed fixture project in testdata/gradle-project rather
// than generating Gradle files from Go.
//
// Requirements: Java on PATH, internet access (first run downloads Gradle wrapper).
// Skipped automatically if Java is not available or in -short mode.
func TestIntegrationGradleBuildCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	for _, tool := range []string{"java", "zstd", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}

	ctx := context.Background()

	// Copy the fixture project into a temp dir so we can mutate it freely.
	fixtureDir := filepath.Join("testdata", "gradle-project")
	if _, err := os.Stat(fixtureDir); err != nil {
		t.Fatalf("fixture not found: %v", err)
	}

	projectDir := t.TempDir()
	if err := copyDir(projectDir, fixtureDir); err != nil {
		t.Fatalf("copying fixture: %v", err)
	}

	gradleUserHome := filepath.Join(t.TempDir(), "gradle-home")
	must(t, os.MkdirAll(gradleUserHome, 0o755))

	gradlew := filepath.Join(projectDir, "gradlew")
	must(t, os.Chmod(gradlew, 0o755))

	// ── Initialize git repo (needed for commit-based cache keys) ─────────────
	gitRun := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", projectDir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test",
			"GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test",
			"GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	gitRun("init")
	gitRun("add", ".")
	gitRun("commit", "-m", "initial")
	commitSHA := gitRun("rev-parse", "HEAD")

	// ── Step 1: Initial Gradle build ─────────────────────────────────────────
	t.Log("Step 1: Running initial Gradle build...")
	gradleRun(t, ctx, gradlew, projectDir, gradleUserHome, "build")

	// Verify compilation produced class files.
	classesDir := filepath.Join(projectDir, "build", "classes")
	if _, err := os.Stat(classesDir); err != nil {
		t.Fatalf("expected compiled classes: %v", err)
	}

	// ── Step 2: Save the cache ───────────────────────────────────────────────
	t.Log("Step 2: Saving cache...")
	store := &fileBundleStore{dir: t.TempDir()}
	cacheKey := "cache-test:build"

	sources := []tarSource{{BaseDir: gradleUserHome, Path: "./caches"}}
	if fi, err := os.Stat(filepath.Join(gradleUserHome, "wrapper")); err == nil && fi.IsDir() {
		sources = append(sources, tarSource{BaseDir: gradleUserHome, Path: "./wrapper"})
	}
	if fi, err := os.Stat(filepath.Join(projectDir, ".gradle", "configuration-cache")); err == nil && fi.IsDir() {
		sources = append(sources, tarSource{
			BaseDir: filepath.Join(projectDir, ".gradle"),
			Path:    "./configuration-cache",
		})
	}

	tmp, err := os.CreateTemp("", "gradle-cache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()

	if err := createTarZstd(ctx, tmp, sources); err != nil {
		t.Fatalf("createTarZstd: %v", err)
	}
	size, _ := tmp.Seek(0, io.SeekCurrent)
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	t.Logf("  Bundle size: %.1f MB", float64(size)/1e6)

	if err := store.put(ctx, commitSHA, cacheKey, tmp, size); err != nil {
		t.Fatalf("store.put: %v", err)
	}
	_ = tmp.Close()

	// ── Step 3: Clear all Gradle state ───────────────────────────────────────
	t.Log("Step 3: Clearing Gradle state...")
	must(t, os.RemoveAll(gradleUserHome))
	must(t, os.MkdirAll(gradleUserHome, 0o755))
	must(t, os.RemoveAll(filepath.Join(projectDir, ".gradle")))
	must(t, os.RemoveAll(filepath.Join(projectDir, "build")))

	if _, err := os.Stat(filepath.Join(gradleUserHome, "caches")); err == nil {
		t.Fatal("expected caches dir to be gone after cleanup")
	}

	// ── Step 4: Restore the cache ────────────────────────────────────────────
	t.Log("Step 4: Restoring cache...")
	body, err := store.get(ctx, commitSHA, cacheKey, 0)
	if err != nil {
		t.Fatalf("store.get: %v", err)
	}

	rules := []extractRule{
		{prefix: "caches/", baseDir: gradleUserHome},
		{prefix: "wrapper/", baseDir: gradleUserHome},
		{prefix: "configuration-cache/", baseDir: filepath.Join(projectDir, ".gradle")},
	}
	if err := extractBundleZstd(ctx, body, rules, projectDir); err != nil {
		t.Fatalf("extractBundleZstd: %v", err)
	}
	_ = body.Close()

	if _, err := os.Stat(filepath.Join(gradleUserHome, "caches")); err != nil {
		t.Fatalf("expected caches dir after restore: %v", err)
	}

	// Check if configuration-cache was restored.
	ccRestored := filepath.Join(projectDir, ".gradle", "configuration-cache")
	if _, err := os.Stat(ccRestored); err != nil {
		t.Log("  configuration-cache dir was NOT restored (may not have been in the bundle)")
	} else {
		t.Log("  configuration-cache dir restored")
	}

	// ── Step 5: Rebuild and verify cache hits ────────────────────────────────
	t.Log("Step 5: Rebuilding to verify cache hits...")
	output := gradleRun(t, ctx, gradlew, projectDir, gradleUserHome, "build")

	// Check for configuration cache reuse.
	if strings.Contains(output, "Reusing configuration cache") {
		t.Log("  Configuration cache: reused")
	} else {
		ccLine := extractLine(output, "configuration cache")
		t.Logf("  Configuration cache: %s", ccLine)
		if strings.Contains(ccLine, "stored") {
			t.Error("expected configuration cache to be reused, but it was stored fresh")
		}
	}

	// Check for build cache hits (FROM-CACHE) on compilation tasks.
	fromCacheCount := strings.Count(output, "FROM-CACHE")
	upToDateCount := strings.Count(output, "UP-TO-DATE")
	t.Logf("  Task results: %d FROM-CACHE, %d UP-TO-DATE", fromCacheCount, upToDateCount)

	if fromCacheCount == 0 && upToDateCount == 0 {
		t.Error("expected at least some tasks to be FROM-CACHE or UP-TO-DATE after restore")
	}

	t.Log("Integration test passed")
}

// gradleRun executes a Gradle build and returns the combined output.
func gradleRun(t *testing.T, ctx context.Context, gradlew, projectDir, gradleUserHome string, tasks ...string) string {
	t.Helper()
	args := append(tasks, "--no-daemon", "--console=plain")
	cmd := exec.CommandContext(ctx, gradlew, args...)
	cmd.Dir = projectDir
	cmd.Env = gradleEnv(gradleUserHome)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("gradle %v failed: %v\n%s", tasks, err, out)
	}
	return string(out)
}

func gradleEnv(gradleUserHome string) []string {
	env := os.Environ()
	filtered := make([]string, 0, len(env)+2)
	for _, e := range env {
		if !strings.HasPrefix(e, "GRADLE_USER_HOME=") &&
			!strings.HasPrefix(e, "GRADLE_ENCRYPTION_KEY=") {
			filtered = append(filtered, e)
		}
	}
	return append(filtered,
		"GRADLE_USER_HOME="+gradleUserHome,
		// Fixed encryption key so the configuration cache keystore is stable
		// across Gradle invocations and survives save/restore cycles.
		"GRADLE_ENCRYPTION_KEY=7FmG8IW20OSZFPEUD6OWjP847SYQz07Oe/4iAN6dpo0=",
	)
}

func extractLine(output, substr string) string {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(strings.ToLower(line), strings.ToLower(substr)) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}
