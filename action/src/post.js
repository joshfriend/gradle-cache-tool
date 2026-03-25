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

async function run() {
  try {
    if (core.getInput("save") === "false") {
      core.info("Cache save skipped (save: false)");
      return;
    }

    const branch = resolveBranch();

    if (branch) {
      // save-delta will fall back to full save if no restore marker exists
      const args = [
        "save-delta",
        ...commonArgs(),
        ...backendArgs(),
        ...gradleHomeArgs(),
        ...gitDirArgs(),
        "--branch",
        branch,
      ];
      await exec.exec("gradle-cache", args, execOptions());
    } else {
      const args = [
        "save",
        ...commonArgs(),
        ...backendArgs(),
        ...gradleHomeArgs(),
        ...includedBuildArgs(),
        ...gitDirArgs(),
      ];
      await exec.exec("gradle-cache", args, execOptions());
    }
  } catch (error) {
    core.warning(`Cache save failed: ${error.message}`);
  }
}

run();
