# Bundle Cache Tool

A CLI tool for saving and restoring Gradle build cache bundles, with a ready-to-use GitHub Action.

Bundles are stored keyed by commit SHA, so `restore` doesn't need to know
exactly which commit produced a given bundle. Instead, it walks local git
history and tries each commit SHA in order, newest first, until it finds a
bundle that exists. By default it walks from a given ref (default: `HEAD`);
when restoring for a branch or pull request, it first resolves the merge-base
of `HEAD` and the base ref, then walks from that common ancestor. This lets a
feature branch restore a bundle compatible with its base without needing to
know the SHA in advance.

The history walk counts distinct author-change boundaries rather than raw
commit count, so a long run of commits by the same author only consumes one
step of the search budget. The default search depth is 20 such boundaries.

## GitHub Action

The easiest way to use gradle-cache in CI is with the GitHub Action. It stores
cache bundles in the GitHub Actions cache (no S3 required) and handles
restore/save automatically. Place the action **before** your Gradle build step:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # full history needed for cache lookup

      - uses: joshfriend/bundle-cache@v1  # restores cache here

      - run: ./gradlew assembleDebug            # your build

      # cache is saved automatically when the job finishes
```

> **Note:** `fetch-depth: 0` is required so the git history walk can search
> past commits for cache bundles. Without it, GitHub's default shallow clone
> (depth 1) means only the current commit is checked.

The action has three phases that run at different points in the job lifecycle:

1. **Pre-step** (before your workflow steps) — installs the `gradle-cache` binary
2. **Main step** (where you place `uses:`) — restores the most recent cache bundle
3. **Post-step** (automatic, after all steps finish) — saves an updated cache bundle

The post-step runs automatically during job cleanup — you don't need to add a
separate save step. It runs even if your build fails, so partial caches are
still preserved for the next run.

On **pull requests**, the action automatically uses delta caches: it restores
the base bundle by walking history from the **merge-base** of the PR branch and
the default branch, applies any existing branch delta on top, and after the
build only saves the files that changed. On **pushes to the default branch**,
it saves a full bundle. This is all auto-detected from the GitHub event context
— no configuration needed.

### Inputs

| Input | Default | Description |
|-------|---------|-------------|
| `cache-key` | `github.job` | Cache key identifying the bundle. Each job gets its own cache by default. |
| `ref` | repo default branch | Git ref used to search for a base bundle. On PR restores, history walks from the merge-base of `HEAD` and this ref. |
| `branch` | auto-detected on PRs | Branch name for delta cache support. |
| `project-dir` | `.` | Path to the Gradle project root. |
| `gradle-user-home` | `~/.gradle` | Path to GRADLE_USER_HOME. |
| `included-build` | | Comma-separated included build paths (e.g. `buildSrc,build-logic`). |
| `bucket` | | S3 bucket name. When set, uses S3 instead of the GitHub Actions cache. |
| `region` | `us-west-2` | AWS region (only used with `bucket`). |
| `save` | `true` | Set to `false` to skip saving after the build. |
| `version` | `latest` | Version of gradle-cache to install. |
| `log-level` | `info` | Log level: `debug`, `info`, `warn`, or `error`. |

## CLI

The action wraps the `gradle-cache` CLI, which can also be used standalone
with S3 or a cachew server as the storage backend.

## Installation

```sh
curl -fsSL https://raw.githubusercontent.com/joshfriend/gradle-cache-tool/main/scripts/install.sh | sh
```

This installs the latest release to `~/.local/bin`. Set `INSTALL_DIR` to override the destination, or `VERSION` to pin a specific release tag.

## Usage

### Base cache (main branch)

```
gradle-cache restore --bucket <bucket> --cache-key <key> [--ref main]
gradle-cache save    --bucket <bucket> --cache-key <key>
```

`--ref` controls where the history walk starts (default `HEAD`). When running
on a feature branch you typically want to pass `--ref main` so the walk
searches commits that CI has actually built cache bundles for.

`--included-build` (repeatable) controls which included build output directories
are archived alongside `$GRADLE_USER_HOME/caches`. Accepts a direct path
(`buildSrc`, `build-logic`) or a glob (`plugins/*`) to include all
subdirectories. Defaults to `buildSrc`.

### Branch delta cache (PR branches)

For PR builds, pass `--branch` to `restore` to apply a branch delta in the same invocation. The delta bundle is downloaded concurrently with the base extraction so it adds no extra latency:

```sh
# Restore phase (single invocation)
gradle-cache restore     --bucket <bucket> --cache-key <key> --ref main --branch $BRANCH_NAME

# ... run the Gradle build ...

# Save phase
gradle-cache save-delta  --bucket <bucket> --cache-key <key> --branch $BRANCH_NAME
```

After the build, `save-delta` scans for files created since the restore marker and uploads a cumulative delta bundle keyed by branch name — so it survives rebases and force-pushes without any extra bookkeeping.

If you need to apply a delta separately (e.g. the base was already restored by another step), `restore-delta` is still available as a standalone subcommand.

### Credentials

S3 credentials are resolved via the standard AWS credential chain (environment variables, IRSA, instance profiles, etc.).

## License

Apache 2.0
