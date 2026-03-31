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
	"time"
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

// TestIntegrationDeltaWorkspaceIntegrity verifies that delta bundles capture
// complete immutable workspaces even when only some files have new mtimes.
//
// In CI, the base bundle provides workspace output files (old mtime) while the
// delta overwrites metadata files (new mtime). Without ImmutableWorkspaceParents,
// save-delta only captures the metadata → partial workspace. When that delta is
// applied to a base that lacks the workspace, Gradle crashes with:
//
//	Could not read workspace metadata from .../metadata.bin (No such file or directory)
//
// This test reproduces the mtime skew by backdating output files in workspaces
// created by a real Gradle build, then verifying the delta is complete.
func TestIntegrationDeltaWorkspaceIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	for _, tool := range []string{"java", "tar"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}

	binaryPath := filepath.Join(t.TempDir(), "gradle-cache")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	buildCmd.Dir = "."
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}

	for _, tt := range []struct {
		name        string
		fixture     string
		dslCacheID  string
		newBuild    string
		newSettings string
	}{
		{
			name:       "groovy-dsl",
			fixture:    "groovy-project",
			dslCacheID: "groovy-dsl",
			newBuild: `plugins { id 'java' }
repositories { mavenCentral() }
dependencies { implementation 'com.google.guava:guava:33.4.0-jre' }
`,
			newSettings: "\ninclude ':sub'\n",
		},
		{
			name:       "kotlin-dsl",
			fixture:    "gradle-project",
			dslCacheID: "kotlin-dsl",
			newBuild: `plugins { kotlin("jvm") version "2.3.20" }
repositories { mavenCentral() }
dependencies { implementation("com.google.guava:guava:33.4.0-jre") }
`,
			newSettings: "\ninclude(\":sub\")\n",
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			backend, cleanupBackend := backendArgs(t)
			defer cleanupBackend()

			ctx := integrationContextFrom(t, tt.fixture)

			buildFile := "build.gradle"
			settingsFile := "settings.gradle"
			if tt.dslCacheID == "kotlin-dsl" {
				buildFile = "build.gradle.kts"
				settingsFile = "settings.gradle.kts"
			}

			restoreArgs := append([]string{"--log-level", "debug", "restore"}, backend...)
			saveDeltaArgs := append([]string{"--log-level", "debug", "save-delta"}, backend...)
			_ = append([]string{"--log-level", "debug", "restore-delta"}, backend...)

			// ── Step 1: Build and save base bundle ──────────────────────
			t.Log("Step 1: Assembling and saving base bundle...")
			gradleRun(t, ctx.projectDir, ctx.gradlew, ctx.gradleUserHome, "assemble")
			commitSHA := gitRevParse(t, ctx.projectDir)

			saveArgs := append([]string{"--log-level", "debug", "save"}, backend...)
			saveArgs = append(saveArgs,
				"--cache-key", ctx.cacheKey,
				"--commit", commitSHA,
				"--gradle-user-home", ctx.gradleUserHome,
			)
			runCLI(t, binaryPath, ctx, saveArgs...)

			// ── Step 2: Restore base, add subproject, build ─────────────
			t.Log("Step 2: Restore base, add subproject, build...")
			clearGradleState(t, ctx)

			restoreArgs = append(restoreArgs,
				"--cache-key", ctx.cacheKey,
				"--ref", commitSHA,
				"--git-dir", ctx.projectDir,
				"--gradle-user-home", ctx.gradleUserHome,
			)
			runCLI(t, binaryPath, ctx, restoreArgs...)

			subDir := filepath.Join(ctx.projectDir, "sub")
			must(t, os.MkdirAll(filepath.Join(subDir, "src", "main", "java"), 0o755))
			must(t, os.WriteFile(filepath.Join(subDir, buildFile), []byte(tt.newBuild), 0o644))

			settingsPath := filepath.Join(ctx.projectDir, settingsFile)
			f, err := os.OpenFile(settingsPath, os.O_APPEND|os.O_WRONLY, 0o644)
			must(t, err)
			_, err = f.WriteString(tt.newSettings)
			must(t, err)
			must(t, f.Close())

			gradleRun(t, ctx.projectDir, ctx.gradlew, ctx.gradleUserHome, "assemble")

			// ── Step 3: Simulate mtime skew, save delta ─────────────────
			// Snapshot workspace file counts before backdating (reference).
			refWorkspaceDirs := map[string]string{}
			for _, dirID := range []string{tt.dslCacheID, "transforms"} {
				dir := findDslCacheDir(t, ctx.gradleUserHome, dirID)
				if dir != "" {
					refWorkspaceDirs[dirID] = dir
				}
			}

			// Backdate output files to simulate base-provided outputs with
			// old mtimes while metadata has new mtimes from delta extraction.
			t.Log("Step 3: Simulating mtime skew in workspaces, saving delta...")
			backdated := backdateWorkspaceOutputs(t, ctx.gradleUserHome, tt.dslCacheID)
			t.Logf("  Backdated output files in %d workspace(s)", backdated)

			saveDeltaArgs = append(saveDeltaArgs,
				"--cache-key", ctx.cacheKey,
				"--branch", "test-branch",
				"--gradle-user-home", ctx.gradleUserHome,
			)
			runCLI(t, binaryPath, ctx, saveDeltaArgs...)

			// ── Step 4: Apply delta to empty cache, check completeness ──
			// Extract the delta into a fresh directory (no base) so that
			// partial workspaces aren't masked by base-provided files.
			t.Log("Step 4: Applying delta to fresh cache, checking workspace completeness...")
			freshHome := filepath.Join(t.TempDir(), "fresh-gradle-home")
			must(t, os.MkdirAll(filepath.Join(freshHome, "caches"), 0o755))

			freshRestoreDelta := append([]string{"--log-level", "debug", "restore-delta"}, backend...)
			freshRestoreDelta = append(freshRestoreDelta,
				"--cache-key", ctx.cacheKey,
				"--branch", "test-branch",
				"--gradle-user-home", freshHome,
			)
			runCLI(t, binaryPath, ctx, freshRestoreDelta...)

			// Check for partial workspaces by comparing delta vs reference.
			var corruptCount int
			for _, dirID := range []string{tt.dslCacheID, "transforms"} {
				refDir := refWorkspaceDirs[dirID]
				deltaDir := findDslCacheDir(t, freshHome, dirID)
				corruptCount += checkPartialWorkspaces(t, refDir, deltaDir)
			}
			if corruptCount > 0 {
				t.Errorf("found %d partial workspace(s) — ImmutableWorkspaceParents not working", corruptCount)
			}

			// Also restore base + delta normally and verify Gradle works.
			clearGradleState(t, ctx)
			runCLI(t, binaryPath, ctx, restoreArgs...)

			fullRestoreDelta := append([]string{"--log-level", "debug", "restore-delta"}, backend...)
			fullRestoreDelta = append(fullRestoreDelta,
				"--cache-key", ctx.cacheKey,
				"--branch", "test-branch",
				"--gradle-user-home", ctx.gradleUserHome,
			)
			runCLI(t, binaryPath, ctx, fullRestoreDelta...)

			output, err := gradleRunMayFail(ctx.projectDir, ctx.gradlew, ctx.gradleUserHome, "assemble")
			if err != nil {
				if strings.Contains(output, "metadata.bin") || strings.Contains(output, "workspace metadata") {
					t.Fatalf("workspace corruption after delta restore:\n%s", output)
				}
				t.Fatalf("Gradle failed after delta restore: %v\n%s", err, output)
			}
			t.Log("  Gradle assembled successfully")
		})
	}
}

// backdateWorkspaceOutputs walks immutable workspace directories (transforms/,
// groovy-dsl/, kotlin-dsl/) and backdates all non-metadata files to simulate the
// mtime skew from base+delta extraction. Returns the number of workspaces affected.
func backdateWorkspaceOutputs(t *testing.T, gradleUserHome, dslCacheID string) int {
	t.Helper()
	oldTime := time.Now().Add(-1 * time.Hour)
	affected := 0

	for _, dirID := range []string{dslCacheID, "transforms"} {
		wsParent := findDslCacheDir(t, gradleUserHome, dirID)
		if wsParent == "" {
			continue
		}
		entries, _ := os.ReadDir(wsParent)
		for _, entry := range entries {
			if !entry.IsDir() || entry.Name() == ".internal" {
				continue
			}
			wsDir := filepath.Join(wsParent, entry.Name())
			backdatedAny := false
			_ = filepath.WalkDir(wsDir, func(path string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				name := d.Name()
				// Keep metadata.bin and results.bin with current (new) mtime.
				// Backdate everything else to simulate base-provided output files.
				if name != "metadata.bin" && name != "results.bin" {
					_ = os.Chtimes(path, oldTime, oldTime)
					backdatedAny = true
				}
				return nil
			})
			if backdatedAny {
				affected++
			}
		}
	}
	return affected
}

// integrationContextFrom creates an integration context from a named fixture directory.
func integrationContextFrom(t *testing.T, fixture string) integrationCtx {
	t.Helper()

	fixtureDir := filepath.Join("testdata", fixture)
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

	gitInit(t, projectDir)

	return integrationCtx{
		projectDir:     projectDir,
		gradleUserHome: gradleUserHome,
		gradlew:        gradlew,
		cacheKey:       "delta-test:" + fixture,
	}
}

// findDslCacheDir searches for a DSL cache directory (e.g. "groovy-dsl" or
// "kotlin-dsl") inside GRADLE_USER_HOME/caches/<version>/.
func findDslCacheDir(t *testing.T, gradleUserHome, dslID string) string {
	t.Helper()
	cachesDir := filepath.Join(gradleUserHome, "caches")
	var found string
	_ = filepath.Walk(cachesDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() && info.Name() == dslID {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	return found
}

// checkPartialWorkspaces compares workspace directories between a reference
// (complete) cache and a delta-only cache. A workspace is partial if the delta
// has it but with fewer files than the reference. Returns the count of partial
// workspaces found.
func checkPartialWorkspaces(t *testing.T, refDir, deltaDir string) int {
	t.Helper()
	if refDir == "" || deltaDir == "" {
		return 0
	}

	refCounts := workspaceFileCounts(refDir)
	deltaCounts := workspaceFileCounts(deltaDir)

	var count int
	for hash, deltaFiles := range deltaCounts {
		refFiles, inRef := refCounts[hash]
		if !inRef {
			// Workspace only in delta — can't compare, assume OK.
			continue
		}
		if deltaFiles < refFiles {
			t.Logf("  PARTIAL workspace %s: delta has %d files, reference has %d", hash, deltaFiles, refFiles)
			count++
		}
	}
	return count
}

// workspaceFileCounts returns a map of workspace hash → file count for all
// hex-hash workspace directories under dir (recursing one level for containers).
func workspaceFileCounts(dir string) map[string]int {
	counts := make(map[string]int)
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == ".internal" {
			continue
		}
		name := entry.Name()
		if isHexHash(name) {
			n := 0
			_ = filepath.WalkDir(filepath.Join(dir, name), func(_ string, d os.DirEntry, _ error) error {
				if d != nil && !d.IsDir() {
					n++
				}
				return nil
			})
			counts[name] = n
		} else {
			// Recurse for containers like kotlin-dsl/accessors/
			for k, v := range workspaceFileCounts(filepath.Join(dir, name)) {
				counts[k] = v
			}
		}
	}
	return counts
}

// isHexHash returns true if s looks like a 32-char hex hash (Gradle workspace ID).
func isHexHash(s string) bool {
	if len(s) < 16 {
		return false
	}
	for _, c := range s {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || c == '-'
		if !isHex {
			return false
		}
	}
	return true
}

// gradleRunMayFail runs Gradle and returns the output + error instead of calling t.Fatal.
func gradleRunMayFail(projectDir, gradlew, gradleUserHome string, tasks ...string) (string, error) {
	args := append(tasks, "--no-daemon", "--console=plain")
	cmd := exec.Command(gradlew, args...)
	cmd.Dir = projectDir
	cmd.Env = gradleEnv(gradleUserHome)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func extractLine(output, substr string) string {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(strings.ToLower(line), strings.ToLower(substr)) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}
