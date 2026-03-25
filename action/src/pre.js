const core = require("@actions/core");
const { install } = require("./helpers");

async function run() {
  try {
    await install();
  } catch (error) {
    core.setFailed(`Failed to install gradle-cache: ${error.message}`);
  }
}

run();
