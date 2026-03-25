const fs = require("fs");
const core = require("@actions/core");
const exec = require("@actions/exec");
const {
  backendArgs,
  commonArgs,
  gradleHomeArgs,
  includedBuildArgs,
  gitDirArgs,
  execOptions,
  resolveBranch,
} = require("./helpers");

function defaultBranch() {
  try {
    const eventPath = process.env.GITHUB_EVENT_PATH;
    if (eventPath) {
      const event = JSON.parse(fs.readFileSync(eventPath, "utf8"));
      if (event.repository && event.repository.default_branch) {
        return event.repository.default_branch;
      }
    }
  } catch {
    // ignore
  }
  return "HEAD";
}

async function run() {
  try {
    const args = [
      "restore",
      ...commonArgs(),
      ...backendArgs(),
      ...gradleHomeArgs(),
      ...includedBuildArgs(),
      ...gitDirArgs(),
      "--ref",
      core.getInput("ref") || defaultBranch(),
    ];

    const branch = resolveBranch();
    if (branch) {
      args.push("--branch", branch);
      core.info(`Delta cache enabled for branch: ${branch}`);
    }

    const exitCode = await exec.exec("gradle-cache", args, execOptions({
      ignoreReturnCode: true,
    }));
    if (exitCode !== 0) {
      core.warning("Cache restore failed; proceeding without cache");
    }

    // Save inputs for the post step
    core.saveState("cache-key", core.getInput("cache-key"));
    core.saveState("save", core.getInput("save"));
  } catch (error) {
    core.warning(`Cache restore failed: ${error.message}`);
  }
}

run();
