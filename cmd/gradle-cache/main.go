// gradle-cache is a CLI for restoring and saving Gradle build cache bundles.
// It delegates all logic to the gradlecache library package.
package main

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/kong"

	"github.com/joshfriend/gradle-cache-tool/gradlecache"
)

const gradleUserHomeEnv = "GRADLE_USER_HOME"

// version is set at build time via -ldflags.
var version = "dev"

type CLI struct {
	Version       kong.VersionFlag `help:"Print version and exit."`
	LogLevel      string           `help:"Log level." default:"info" enum:"debug,info,warn,error"`
	Restore       RestoreCmd       `cmd:"" help:"Find the newest cached bundle in history and restore it to GRADLE_USER_HOME."`
	RestoreDelta  RestoreDeltaCmd  `cmd:"" help:"Apply a branch delta bundle on top of an already-restored base cache."`
	Save          SaveCmd          `cmd:"" help:"Bundle GRADLE_USER_HOME/caches and upload to S3 tagged with a commit SHA."`
	SaveDelta     SaveDeltaCmd     `cmd:"" help:"Pack files added since the last restore and upload as a branch delta bundle."`
	StatsdAddr    string           `help:"DogStatsD address (host:port) for emitting metrics. Auto-detected from DD_AGENT_HOST if not set."`
	DatadogAPIKey string           `help:"DataDog API key for direct metric submission (no agent required)." env:"DATADOG_API_KEY"`
	MetricsTags   []string         `help:"Additional metric tags in key:value format. May be repeated." name:"metrics-tag"`
	CPUProfile    string           `help:"Write CPU profile to file." name:"cpuprofile" hidden:""`
}

type backendFlags struct {
	Bucket        string `help:"S3 bucket name."`
	Region        string `help:"AWS region." default:"us-west-2" env:"AWS_REGION"`
	CachewURL     string `help:"Cachew server URL (e.g. http://localhost:8080). Mutually exclusive with --bucket." name:"cachew-url"`
	GithubActions bool   `help:"Use the GitHub Actions Cache backend." name:"github-actions"`
}

func (f *backendFlags) validate() error {
	set := 0
	if f.Bucket != "" {
		set++
	}
	if f.CachewURL != "" {
		set++
	}
	if f.GithubActions {
		set++
	}
	if set == 0 {
		return errors.New("one of --bucket, --cachew-url, or --github-actions is required")
	}
	if set > 1 {
		return errors.New("--bucket, --cachew-url, and --github-actions are mutually exclusive")
	}
	return nil
}

// ── Restore ─────────────────────────────────────────────────────────────────

type RestoreCmd struct {
	backendFlags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	GitDir         string   `help:"Path to the git repository used for history walking." default:"." type:"path"`
	Ref            string   `help:"Git ref used to search for a base bundle. When --branch is set, history walks from the merge-base of HEAD and this ref." default:"HEAD"`
	Commit         string   `help:"Specific commit SHA to try directly, skipping history walk."`
	MaxBlocks      int      `help:"Number of distinct-author commit blocks to search." default:"20"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to restore. May be repeated." name:"included-build"`
	Branch         string   `help:"Branch name to also apply a delta bundle for." optional:""`
}

func (c *RestoreCmd) AfterApply() error { return c.validate() }

func (c *RestoreCmd) Run(ctx context.Context, metrics gradlecache.MetricsClient) error {
	slog.Debug(gradleUserHomeEnv, "path", c.GradleUserHome)
	return gradlecache.Restore(ctx, gradlecache.RestoreConfig{
		Bucket:         c.Bucket,
		Region:         c.Region,
		CachewURL:      c.CachewURL,
		CacheKey:       c.CacheKey,
		GitDir:         c.GitDir,
		Ref:            c.Ref,
		Commit:         c.Commit,
		MaxBlocks:      c.MaxBlocks,
		GradleUserHome: c.GradleUserHome,
		IncludedBuilds: c.IncludedBuilds,
		Branch:         c.Branch,
		Metrics:        metrics,
	})
}

// ── RestoreDelta ────────────────────────────────────────────────────────────

type RestoreDeltaCmd struct {
	backendFlags
	CacheKey       string `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Branch         string `help:"Branch name to look up a delta for." required:""`
	GradleUserHome string `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
}

func (c *RestoreDeltaCmd) AfterApply() error { return c.validate() }

func (c *RestoreDeltaCmd) Run(ctx context.Context, metrics gradlecache.MetricsClient) error {
	slog.Debug(gradleUserHomeEnv, "path", c.GradleUserHome)
	return gradlecache.RestoreDelta(ctx, gradlecache.RestoreDeltaConfig{
		Bucket:         c.Bucket,
		Region:         c.Region,
		CachewURL:      c.CachewURL,
		CacheKey:       c.CacheKey,
		Branch:         c.Branch,
		GradleUserHome: c.GradleUserHome,
		Metrics:        metrics,
	})
}

// ── Save ────────────────────────────────────────────────────────────────────

type SaveCmd struct {
	backendFlags
	CacheKey       string   `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Commit         string   `help:"Commit SHA to tag this bundle with. Defaults to HEAD of --git-dir."`
	GitDir         string   `help:"Path to the git repository." default:"." type:"path"`
	GradleUserHome string   `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
	IncludedBuilds []string `help:"Included build directories whose build/ output to archive. May be repeated." name:"included-build"`
}

func (c *SaveCmd) AfterApply() error { return c.validate() }

func (c *SaveCmd) Run(ctx context.Context, metrics gradlecache.MetricsClient) error {
	slog.Debug(gradleUserHomeEnv, "path", c.GradleUserHome)
	return gradlecache.Save(ctx, gradlecache.SaveConfig{
		Bucket:         c.Bucket,
		Region:         c.Region,
		CachewURL:      c.CachewURL,
		CacheKey:       c.CacheKey,
		Commit:         c.Commit,
		GitDir:         c.GitDir,
		GradleUserHome: c.GradleUserHome,
		IncludedBuilds: c.IncludedBuilds,
		Metrics:        metrics,
	})
}

// ── SaveDelta ───────────────────────────────────────────────────────────────

type SaveDeltaCmd struct {
	backendFlags
	CacheKey       string `help:"Bundle identifier, e.g. 'my-project:assembleRelease'." required:""`
	Branch         string `help:"Branch name to save the delta under." required:""`
	GradleUserHome string `help:"Path to GRADLE_USER_HOME." env:"GRADLE_USER_HOME"`
}

func (c *SaveDeltaCmd) AfterApply() error { return c.validate() }

func (c *SaveDeltaCmd) Run(ctx context.Context, metrics gradlecache.MetricsClient) error {
	slog.Debug(gradleUserHomeEnv, "path", c.GradleUserHome)
	return gradlecache.SaveDelta(ctx, gradlecache.SaveDeltaConfig{
		Bucket:         c.Bucket,
		Region:         c.Region,
		CachewURL:      c.CachewURL,
		CacheKey:       c.CacheKey,
		Branch:         c.Branch,
		GradleUserHome: c.GradleUserHome,
		Metrics:        metrics,
	})
}

// ── main ────────────────────────────────────────────────────────────────────

func main() {
	if runtime.GOMAXPROCS(0) < 16 {
		runtime.GOMAXPROCS(16)
	}

	cli := &CLI{}
	ctx := context.Background()
	kctx := kong.Parse(cli,
		kong.UsageOnError(),
		kong.HelpOptions{Compact: true},
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.Vars{"version": version},
	)
	setupLogger(cli.LogLevel)
	slog.Debug("starting gradle-cache", "version", version)

	if cli.CPUProfile != "" {
		f, err := os.Create(cli.CPUProfile)
		if err != nil {
			kctx.Fatalf("could not create CPU profile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			kctx.Fatalf("could not start CPU profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
			slog.Info("CPU profile written", "path", cli.CPUProfile)
		}()
	}

	mf := &gradlecache.MetricsFlags{
		StatsdAddr:    cli.StatsdAddr,
		DatadogAPIKey: cli.DatadogAPIKey,
		MetricsTags:   cli.MetricsTags,
	}
	metrics := mf.NewMetricsClient()
	defer metrics.Close()

	kctx.BindTo(metrics, (*gradlecache.MetricsClient)(nil))
	kctx.FatalIfErrorf(kctx.Run(ctx))
}

func setupLogger(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: l,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	slog.SetDefault(slog.New(handler))
}
