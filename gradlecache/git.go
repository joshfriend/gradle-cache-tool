package gradlecache

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"
)

// mergeBase returns the merge-base between two refs in the given git repo.
// If the merge-base cannot be determined (e.g. shallow clone), it returns
// the baseRef unchanged and a nil error.
func mergeBase(ctx context.Context, gitDir, baseRef, headRef string) (string, error) {
	//nolint:gosec
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "merge-base", baseRef, headRef)
	out, err := cmd.Output()
	if err != nil {
		// merge-base can fail in shallow clones or if refs don't share history;
		// fall back to baseRef so we don't break the restore.
		return baseRef, nil
	}
	sha := strings.TrimSpace(string(out))
	if sha == "" {
		return baseRef, nil
	}
	return sha, nil
}

func historyCommits(ctx context.Context, gitDir, ref string, maxBlocks int) ([]string, error) {
	rawCount := maxBlocks * 10
	//nolint:gosec
	cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "log", "--first-parent",
		fmt.Sprintf("-n%d", rawCount), "--format=%H\t%an", ref)
	out, err := cmd.Output()
	if err != nil {
		return nil, errors.Errorf("git log: %w", err)
	}

	var commits []string
	prevAuthor := ""
	blocksSeen := 0

	scanner := bufio.NewScanner(strings.NewReader(string(out)))
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

func branchSlug(branch string) string {
	s := strings.ReplaceAll(branch, "/", "--")
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

func deltaCommit(branch string) string {
	return "branches/" + branchSlug(branch)
}
