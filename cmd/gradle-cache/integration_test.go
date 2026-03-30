//nolint:gosec // test file: all paths and subprocess args are controlled inputs
package main

import (
	"io"
	"io/fs"
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

// backendArgs returns the CLI flags for the storage backend under test.
// Set GRADLE_CACHE_BACKEND=github-actions to test against the real GitHub
// Actions cache (requires ACTIONS_CACHE_URL and ACTIONS_RUNTIME_TOKEN).
// Otherwise a local fake cachew server is started.
func backendArgs(t *testing.T) (args []string, cleanup func()) {
	t.Helper()
	if os.Getenv("GRADLE_CACHE_BACKEND") == "github-actions" {
		if os.Getenv("ACTIONS_CACHE_URL") == "" || os.Getenv("ACTIONS_RUNTIME_TOKEN") == "" {
			t.Skip("ACTIONS_CACHE_URL / ACTIONS_RUNTIME_TOKEN not set")
		}
		return []string{"--github-actions"}, func() {}
	}
	server := httptest.NewServer(newFakeCachew(t.TempDir()))
	return []string{"--cachew-url", server.URL}, func() { server.Close() }
}

// TestIntegrationGradleBuildCycle exercises the full save/restore cycle using
// the compiled CLI binary as a subprocess. This tests the complete code path
// including kong CLI parsing, metrics binding, and backend communication.
//
// A fake cachew HTTP server stands in for real storage (or the real GitHub
// Actions cache when GRADLE_CACHE_BACKEND=github-actions).
//
// Requirements: Java on PATH, internet access (first run downloads Gradle wrapper).
// Skipped automatically if Java is not available or in -short mode.
func TestIntegrationGradleBuildCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	for _, tool := range []string{"java", "tar"} {
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

	// ── Backend selection ────────────────────────────────────────────────────
	backend, cleanupBackend := backendArgs(t)
	defer cleanupBackend()

	ctx := integrationContext(t)

	// ── Build + Save ─────────────────────────────────────────────────────────
	t.Log("Step 1: Building and saving cache...")
	runGradleBuild(t, ctx)

	commitSHA := gitRevParse(t, ctx.projectDir)

	saveArgs := append([]string{"--log-level", "debug", "save"}, backend...)
	saveArgs = append(saveArgs,
		"--cache-key", ctx.cacheKey,
		"--commit", commitSHA,
		"--gradle-user-home", ctx.gradleUserHome,
	)
	runCLI(t, binaryPath, ctx, saveArgs...)

	// ── Clear + Restore + Verify ─────────────────────────────────────────────
	t.Log("Step 2: Clearing state...")
	clearGradleState(t, ctx)

	t.Log("Step 3: Restoring cache...")
	restoreArgs := append([]string{"--log-level", "debug", "restore"}, backend...)
	restoreArgs = append(restoreArgs,
		"--cache-key", ctx.cacheKey,
		"--ref", commitSHA,
		"--git-dir", ctx.projectDir,
		"--gradle-user-home", ctx.gradleUserHome,
	)
	runCLI(t, binaryPath, ctx, restoreArgs...)

	t.Log("Step 4: Verifying restore...")
	verifyRestore(t, ctx)
}

// TestIntegrationGradleBuild runs only the Gradle build step against the
// fixture project. Use this from CI when the GitHub Action handles
// restore/save and the test only needs to populate GRADLE_USER_HOME.
//
// Set GRADLE_USER_HOME and INTEGRATION_PROJECT_DIR to point at the
// pre-configured project directory.
func TestIntegrationGradleBuild(t *testing.T) {
	if os.Getenv("INTEGRATION_PROJECT_DIR") == "" {
		t.Skip("INTEGRATION_PROJECT_DIR not set")
	}
	for _, tool := range []string{"java", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}

	ctx := externalContext(t)
	runGradleBuild(t, ctx)
}

// TestIntegrationVerifyRestore verifies that a previously restored cache
// produces cache hits on rebuild. Use this from CI when the GitHub Action
// has already restored GRADLE_USER_HOME.
//
// Set GRADLE_USER_HOME and INTEGRATION_PROJECT_DIR to point at the
// pre-configured project directory.
func TestIntegrationVerifyRestore(t *testing.T) {
	if os.Getenv("INTEGRATION_PROJECT_DIR") == "" {
		t.Skip("INTEGRATION_PROJECT_DIR not set")
	}
	for _, tool := range []string{"java", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}

	ctx := externalContext(t)
	verifyRestore(t, ctx)
}

// ─── Shared integration helpers ─────────────────────────────────────────────

// integrationCtx holds paths and settings shared across integration test steps.
type integrationCtx struct {
	projectDir     string
	gradleUserHome string
	gradlew        string
	cacheKey       string
}

// integrationContext sets up a fresh fixture project + gradle home for a
// self-contained integration test.
func integrationContext(t *testing.T) integrationCtx {
	t.Helper()

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

	// Initialize git repo so save can resolve HEAD.
	gitInit(t, projectDir)

	return integrationCtx{
		projectDir:     projectDir,
		gradleUserHome: gradleUserHome,
		gradlew:        gradlew,
		cacheKey:       "cache-test:build",
	}
}

// externalContext builds an integrationCtx from environment variables,
// for use when the GitHub Action manages the project and gradle home.
func externalContext(t *testing.T) integrationCtx {
	t.Helper()

	projectDir := os.Getenv("INTEGRATION_PROJECT_DIR")
	if projectDir == "" {
		t.Fatal("INTEGRATION_PROJECT_DIR must be set")
	}

	gradleUserHome := os.Getenv("GRADLE_USER_HOME")
	if gradleUserHome == "" {
		home, err := os.UserHomeDir()
		must(t, err)
		gradleUserHome = filepath.Join(home, ".gradle")
	}

	gradlew := filepath.Join(projectDir, "gradlew")

	return integrationCtx{
		projectDir:     projectDir,
		gradleUserHome: gradleUserHome,
		gradlew:        gradlew,
		cacheKey:       "cache-test:build",
	}
}

func runCLI(t *testing.T, binaryPath string, ctx integrationCtx, args ...string) string {
	t.Helper()
	cmd := exec.Command(binaryPath, args...)
	cmd.Dir = ctx.projectDir
	cmd.Env = gradleEnv(ctx.gradleUserHome)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("gradle-cache %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func gitInit(t *testing.T, projectDir string) {
	t.Helper()
	for _, args := range [][]string{
		{"init"},
		{"add", "."},
		{"commit", "-m", "initial"},
	} {
		cmd := exec.Command("git", append([]string{"-C", projectDir}, args...)...)
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
}

func gitRevParse(t *testing.T, projectDir string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", projectDir, "rev-parse", "HEAD")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git rev-parse: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func runGradleBuild(t *testing.T, ctx integrationCtx) {
	t.Helper()
	gradleRun(t, ctx.projectDir, ctx.gradlew, ctx.gradleUserHome, "build")

	classesDir := filepath.Join(ctx.projectDir, "build", "classes")
	if _, err := os.Stat(classesDir); err != nil {
		t.Fatalf("expected compiled classes: %v", err)
	}
}

func clearGradleState(t *testing.T, ctx integrationCtx) {
	t.Helper()
	must(t, os.RemoveAll(ctx.gradleUserHome))
	must(t, os.MkdirAll(ctx.gradleUserHome, 0o755))
	must(t, os.RemoveAll(filepath.Join(ctx.projectDir, ".gradle")))
	// Remove build/ dirs from all projects and included builds recursively.
	must(t, filepath.WalkDir(ctx.projectDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "build" {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			return filepath.SkipDir
		}
		return nil
	}))

	if _, err := os.Stat(filepath.Join(ctx.gradleUserHome, "caches")); err == nil {
		t.Fatal("expected caches dir to be gone after cleanup")
	}
}

func verifyRestore(t *testing.T, ctx integrationCtx) {
	t.Helper()

	if _, err := os.Stat(filepath.Join(ctx.gradleUserHome, "caches")); err != nil {
		t.Fatalf("expected caches dir after restore: %v", err)
	}

	if _, err := os.Stat(filepath.Join(ctx.gradleUserHome, "wrapper")); err != nil {
		t.Fatalf("expected wrapper dir after restore: %v", err)
	}

	ccRestored := filepath.Join(ctx.projectDir, ".gradle", "configuration-cache")
	if _, err := os.Stat(ccRestored); err != nil {
		t.Log("  configuration-cache dir was NOT restored")
	} else {
		t.Log("  configuration-cache dir restored")
	}

	output := gradleRun(t, ctx.projectDir, ctx.gradlew, ctx.gradleUserHome, "build")

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

	t.Log("Verify restore passed")
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

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func extractLine(output, substr string) string {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(strings.ToLower(line), strings.ToLower(substr)) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}
