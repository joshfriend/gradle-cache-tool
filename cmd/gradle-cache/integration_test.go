//nolint:gosec // test file: all paths and subprocess args are controlled inputs
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCachew is a minimal file-backed implementation of the cachew object API
// used to exercise the real CLI binary without needing S3 or a remote server.
// Blobs are written to disk to handle large bundles without blowing up memory.
type fakeCachew struct {
	dir string // storage directory
}

func newFakeCachew(dir string) *fakeCachew {
	return &fakeCachew{dir: dir}
}

func (f *fakeCachew) blobPath(key string) string {
	// key is "cacheKey/commit" — flatten slashes to avoid nested dirs
	return filepath.Join(f.dir, strings.ReplaceAll(key, "/", "_"))
}

func (f *fakeCachew) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Expected path: /api/v1/object/{cacheKey}/{commit}
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/object/")
	path := f.blobPath(key)

	switch r.Method {
	case http.MethodHead:
		if _, err := os.Stat(path); err != nil {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		file, err := os.Open(path)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer func() { _ = file.Close() }()
		w.Header().Set("Content-Type", "application/zstd")
		_, _ = io.Copy(w, file)
	case http.MethodPost:
		file, err := os.Create(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, cpErr := io.Copy(file, r.Body)
		_ = file.Close()
		if cpErr != nil {
			http.Error(w, cpErr.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
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

// TestIntegrationGradleBuildCycle exercises the full save/restore cycle using
// the compiled CLI binary as a subprocess. This tests the complete code path
// including kong CLI parsing, metrics binding, and backend communication.
//
// A fake cachew HTTP server stands in for real storage.
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

	// ── Build the CLI binary ─────────────────────────────────────────────────
	binaryPath := filepath.Join(t.TempDir(), "gradle-cache")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	buildCmd.Dir = "."
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}

	// ── Start fake cachew server ─────────────────────────────────────────────
	server := httptest.NewServer(newFakeCachew(t.TempDir()))
	defer server.Close()

	// ── Copy the fixture project ─────────────────────────────────────────────
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

	cacheKey := "cache-test:build"

	// Helper to run the gradle-cache CLI.
	runTool := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(binaryPath, args...)
		cmd.Dir = projectDir
		cmd.Env = gradleEnv(gradleUserHome)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("gradle-cache %v: %v\n%s", args, err, out)
		}
		return string(out)
	}

	// ── Initialize git repo ──────────────────────────────────────────────────
	gitRun := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", projectDir}, args...)...)
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
	gradleRun(t, projectDir, gradlew, gradleUserHome, "build")

	classesDir := filepath.Join(projectDir, "build", "classes")
	if _, err := os.Stat(classesDir); err != nil {
		t.Fatalf("expected compiled classes: %v", err)
	}

	// ── Step 2: Save the cache via CLI ───────────────────────────────────────
	t.Log("Step 2: Saving cache via CLI...")
	runTool("--log-level", "debug", "save",
		"--cachew-url", server.URL,
		"--cache-key", cacheKey,
		"--commit", commitSHA,
		"--gradle-user-home", gradleUserHome,
	)

	// ── Step 3: Clear all Gradle state ───────────────────────────────────────
	t.Log("Step 3: Clearing Gradle state...")
	must(t, os.RemoveAll(gradleUserHome))
	must(t, os.MkdirAll(gradleUserHome, 0o755))
	must(t, os.RemoveAll(filepath.Join(projectDir, ".gradle")))
	must(t, os.RemoveAll(filepath.Join(projectDir, "build")))

	if _, err := os.Stat(filepath.Join(gradleUserHome, "caches")); err == nil {
		t.Fatal("expected caches dir to be gone after cleanup")
	}

	// ── Step 4: Restore the cache via CLI ────────────────────────────────────
	t.Log("Step 4: Restoring cache via CLI...")
	runTool("--log-level", "debug", "restore",
		"--cachew-url", server.URL,
		"--cache-key", cacheKey,
		"--ref", commitSHA,
		"--git-dir", projectDir,
		"--gradle-user-home", gradleUserHome,
	)

	if _, err := os.Stat(filepath.Join(gradleUserHome, "caches")); err != nil {
		t.Fatalf("expected caches dir after restore: %v", err)
	}

	if _, err := os.Stat(filepath.Join(gradleUserHome, "wrapper")); err != nil {
		t.Fatalf("expected wrapper dir after restore: %v", err)
	}

	ccRestored := filepath.Join(projectDir, ".gradle", "configuration-cache")
	if _, err := os.Stat(ccRestored); err != nil {
		t.Log("  configuration-cache dir was NOT restored")
	} else {
		t.Log("  configuration-cache dir restored")
	}

	// ── Step 5: Rebuild and verify cache hits ────────────────────────────────
	t.Log("Step 5: Rebuilding to verify cache hits...")
	output := gradleRun(t, projectDir, gradlew, gradleUserHome, "build")

	// Verify the wrapper was NOT re-downloaded — if it was, the bundle didn't
	// include the wrapper directory (or the .ok marker file was missing).
	if strings.Contains(output, "Downloading") {
		t.Error("Gradle re-downloaded the wrapper distribution after restore; wrapper/ should be cached in the bundle")
	}

	if strings.Contains(output, "Reusing configuration cache") {
		t.Log("  Configuration cache: reused")
	} else {
		ccLine := extractLine(output, "configuration cache")
		t.Logf("  Configuration cache: %s", ccLine)
		if strings.Contains(ccLine, "stored") {
			t.Error("expected configuration cache to be reused, but it was stored fresh")
		}
	}

	fromCacheCount := strings.Count(output, "FROM-CACHE")
	upToDateCount := strings.Count(output, "UP-TO-DATE")
	t.Logf("  Task results: %d FROM-CACHE, %d UP-TO-DATE", fromCacheCount, upToDateCount)

	if fromCacheCount == 0 && upToDateCount == 0 {
		t.Error("expected at least some tasks to be FROM-CACHE or UP-TO-DATE after restore")
	}

	t.Log("Integration test passed")
}

// gradleRun executes a Gradle build and returns the combined output.
func gradleRun(t *testing.T, projectDir, gradlew, gradleUserHome string, tasks ...string) string {
	t.Helper()
	args := append(tasks, "--no-daemon", "--console=plain")
	cmd := exec.Command(gradlew, args...)
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
