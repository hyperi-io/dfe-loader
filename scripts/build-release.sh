#!/bin/bash
# Build release binaries for all target platforms
#
# Output: target/release/
#   - dfe-loader-x86_64-linux
#   - dfe-loader-aarch64-linux (requires cross-compilation toolchain)
#
# Usage:
#   ./scripts/build-release.sh           # Build for current arch only
#   ./scripts/build-release.sh --all     # Build for all architectures (requires cross)

set -euo pipefail

VERSION=$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
RELEASE_DIR="target/release"

echo "Building dfe-loader v${VERSION}"

# Detect current architecture
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  CURRENT_TARGET="x86_64-unknown-linux-gnu" ;;
    aarch64) CURRENT_TARGET="aarch64-unknown-linux-gnu" ;;
    *)       echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

build_target() {
    local target=$1
    local suffix=$2

    echo "Building for ${target}..."

    if [[ "$target" == "$CURRENT_TARGET" ]]; then
        cargo build --release --target "$target"
    else
        # Cross-compilation requires: rustup target add $target
        # And appropriate linker (gcc-aarch64-linux-gnu or gcc-x86-64-linux-gnu)
        cargo build --release --target "$target"
    fi

    local src="target/${target}/release/dfe-loader"
    local dst="${RELEASE_DIR}/dfe-loader-${suffix}"

    if [[ -f "$src" ]]; then
        cp "$src" "$dst"
        echo "  -> ${dst} ($(du -h "$dst" | cut -f1))"
    else
        echo "  ERROR: $src not found"
        return 1
    fi
}

# Ensure release directory exists
mkdir -p "$RELEASE_DIR"

if [[ "${1:-}" == "--all" ]]; then
    # Build for all targets
    build_target "x86_64-unknown-linux-gnu" "x86_64-linux"
    build_target "aarch64-unknown-linux-gnu" "aarch64-linux"
else
    # Build for current architecture only
    case "$CURRENT_TARGET" in
        x86_64-unknown-linux-gnu)
            build_target "$CURRENT_TARGET" "x86_64-linux"
            ;;
        aarch64-unknown-linux-gnu)
            build_target "$CURRENT_TARGET" "aarch64-linux"
            ;;
    esac
fi

echo ""
echo "Release binaries:"
ls -lh "${RELEASE_DIR}"/dfe-loader-* 2>/dev/null || echo "  (none)"
