//nolint:gosec // test file: all paths and subprocess args are controlled inputs
package gradlecache

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestHistoryCommits(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	ctx := context.Background()
	repo := t.TempDir()

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

func TestMergeBase(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	ctx := context.Background()
	repo := t.TempDir()

	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repo}, args...)...)
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

	commit := func(msg string) string {
		t.Helper()
		run("commit", "--allow-empty", "-m", msg)
		return run("rev-parse", "HEAD")
	}

	run("init")
	run("config", "user.email", "test@test.com")
	run("config", "user.name", "Test")
	run("branch", "-m", "main")

	base := commit("base")
	commit("main 1")
	run("checkout", "-b", "feature", base)
	commit("feature 1")
	commit("feature 2")

	t.Run("returns common ancestor for divergent branches", func(t *testing.T) {
		got, err := mergeBase(ctx, repo, "main", "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		if got != base {
			t.Fatalf("mergeBase(main, HEAD) = %q, want %q", got, base)
		}
	})

	t.Run("falls back to base ref when merge-base fails", func(t *testing.T) {
		got, err := mergeBase(ctx, repo, "refs/heads/does-not-exist", "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		if got != "refs/heads/does-not-exist" {
			t.Fatalf("mergeBase fallback = %q, want base ref", got)
		}
	})
}
