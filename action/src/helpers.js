const core = require("@actions/core");
const exec = require("@actions/exec");
const tc = require("@actions/tool-cache");
const os = require("os");
const path = require("path");

/**
 * Install the gradle-cache binary. Skips download if it's already on PATH.
 */
async function install() {
  // Check if gradle-cache is already available (e.g. built from source in CI).
  const exitCode = await exec.exec("which", ["gradle-cache"], {
    ignoreReturnCode: true,
    silent: true,
  });
  if (exitCode === 0) {
    core.info("gradle-cache already on PATH, skipping install");
    return;
  }

  const binDir = path.join(os.homedir(), ".local", "bin");
  await exec.exec("mkdir", ["-p", binDir]);

  const version = core.getInput("version") || "latest";
  const platform = os.platform(); // linux or darwin
  const arch = os.arch(); // x64 or arm64

  let suffix;
  if (platform === "darwin") {
    suffix = "darwin-universal";
  } else if (arch === "arm64") {
    suffix = `${platform}-arm64`;
  } else {
    suffix = `${platform}-amd64`;
  }

  let url;
  if (version === "latest") {
    url = `https://github.com/joshfriend/gradle-cache-tool/releases/latest/download/gradle-cache-${suffix}`;
  } else {
    const tag = version.startsWith("v") ? version : `v${version}`;
    url = `https://github.com/joshfriend/gradle-cache-tool/releases/download/${tag}/gradle-cache-${suffix}`;
  }

  core.info(`Downloading gradle-cache from ${url}`);
  const downloaded = await tc.downloadTool(url);

  await exec.exec("chmod", ["+x", downloaded]);

  const dest = path.join(binDir, "gradle-cache");
  await exec.exec("mv", [downloaded, dest]);

  core.addPath(binDir);
  core.info("gradle-cache installed successfully");
}

/**
 * Build common CLI args from action inputs.
 */
function backendArgs() {
  const args = [];
  const bucket = core.getInput("bucket");
  const region = core.getInput("region");

  if (bucket) {
    args.push("--bucket", bucket);
    if (region) args.push("--region", region);
  } else {
    // Default to GitHub Actions cache when no explicit backend is configured.
    args.push("--github-actions");
  }

  return args;
}

function commonArgs() {
  const cacheKey = core.getInput("cache-key") || "gradle";
  const args = ["--cache-key", cacheKey];
  const logLevel = core.getInput("log-level");
  if (logLevel) args.push("--log-level", logLevel);
  return args;
}

function gradleHomeArgs() {
  const home = core.getInput("gradle-user-home");
  return home ? ["--gradle-user-home", home] : [];
}

function includedBuildArgs() {
  const input = core.getInput("included-build");
  if (!input) return [];
  return input
    .split(",")
    .map((b) => b.trim())
    .filter(Boolean)
    .flatMap((b) => ["--included-build", b]);
}

/**
 * Returns --git-dir args when project-dir differs from the workspace root.
 * The git repo lives at GITHUB_WORKSPACE; the Gradle project may be elsewhere.
 */
function gitDirArgs() {
  const workspace = process.env.GITHUB_WORKSPACE || process.cwd();
  return ["--git-dir", workspace];
}

/**
 * Returns exec options with cwd set to the project directory.
 */
function execOptions(extra) {
  const projectDir = core.getInput("project-dir") || ".";
  return { cwd: path.resolve(projectDir), ...extra };
}

/**
 * Resolve the branch name for delta cache support.
 *
 * Explicit `branch` input takes priority. Otherwise, on pull_request events
 * we automatically use GITHUB_HEAD_REF (the PR source branch) so that delta
 * caches work out of the box without any configuration.
 *
 * Returns empty string on push events (default branch builds save full bundles).
 */
function resolveBranch() {
  const explicit = core.getInput("branch");
  if (explicit) return explicit;

  const event = process.env.GITHUB_EVENT_NAME || "";
  if (event === "pull_request" || event === "pull_request_target") {
    return process.env.GITHUB_HEAD_REF || "";
  }
  return "";
}

module.exports = {
  install,
  backendArgs,
  commonArgs,
  gradleHomeArgs,
  includedBuildArgs,
  gitDirArgs,
  execOptions,
  resolveBranch,
};
