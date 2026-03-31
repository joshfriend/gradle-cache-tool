description = "Gradle build cache restore/save tool backed by S3"
homepage    = "https://github.com/block/bundle-cache"

binaries = ["gradle-cache"]
test     = "gradle-cache --help"

# macOS: one universal binary for both Intel and Apple Silicon.
darwin {
  source = "https://github.com/block/bundle-cache/releases/download/${version}/gradle-cache-darwin-universal"
  rename = {"gradle-cache-darwin-universal": "gradle-cache"}
}

# Linux amd64
linux {
  arch   = "amd64"
  source = "https://github.com/block/bundle-cache/releases/download/${version}/gradle-cache-linux-amd64"
  rename = {"gradle-cache-linux-amd64": "gradle-cache"}
}

# Linux arm64
linux {
  arch   = "arm64"
  source = "https://github.com/block/bundle-cache/releases/download/${version}/gradle-cache-linux-arm64"
  rename = {"gradle-cache-linux-arm64": "gradle-cache"}
}

channel "latest" {
  update  = "24h"
  version = "*"

  auto-version {
    github-release        = "block/bundle-cache"
    version-pattern       = "v(.*)"
  }
}

version "0.1.0" {}
