#!/bin/sh
# Install gradle-cache. Usage:
#   curl -fsSL https://raw.githubusercontent.com/joshfriend/bundle-cache/main/scripts/install.sh | sh
#
# Environment variables:
#   VERSION            - specific release tag to install (default: latest)
#   INSTALL_DIR        - installation directory (default: ~/.local/bin)
set -e

REPO="joshfriend/bundle-cache"
BINARY="gradle-cache"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# ---------------------------------------------------------------------------
# Detect OS
# ---------------------------------------------------------------------------
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$OS" in
    linux|darwin) ;;
    *) echo "error: unsupported OS: $OS" >&2; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# Detect architecture
# On macOS always use the universal binary so one download works for both
# Intel and Apple Silicon machines.
# ---------------------------------------------------------------------------
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)         ARCH=amd64 ;;
    aarch64|arm64)  ARCH=arm64 ;;
    *) echo "error: unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

if [ "$OS" = "darwin" ]; then
    PLATFORM="darwin-universal"
else
    PLATFORM="${OS}-${ARCH}"
fi

# ---------------------------------------------------------------------------
# Resolve version
# ---------------------------------------------------------------------------
if [ -z "$VERSION" ]; then
    VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep '"tag_name"' \
        | sed 's/.*"tag_name": *"//;s/".*//')
fi

if [ -z "$VERSION" ]; then
    echo "error: could not determine latest version (try setting VERSION=vX.Y.Z)" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Download and install
# ---------------------------------------------------------------------------
URL="https://github.com/${REPO}/releases/download/${VERSION}/${BINARY}-${PLATFORM}"

echo "Installing ${BINARY} ${VERSION} (${PLATFORM})..."
mkdir -p "$INSTALL_DIR"
curl -fsSL "$URL" -o "$INSTALL_DIR/$BINARY"
chmod +x "$INSTALL_DIR/$BINARY"
echo "Installed: $INSTALL_DIR/$BINARY"

# Remind the user if INSTALL_DIR is not on PATH.
case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *)
        echo ""
        echo "  $INSTALL_DIR is not in your PATH."
        echo "  Add the following to your shell profile:"
        echo ""
        echo "    export PATH=\"\$PATH:$INSTALL_DIR\""
        echo ""
        ;;
esac
