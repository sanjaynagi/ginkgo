#!/bin/sh
# Ginkgo installer — installs the `ginkgo` CLI as an isolated uv tool.
#
# Usage:
#   curl -LsSf https://raw.githubusercontent.com/sanjaynagi/ginkgo/main/install.sh | sh
#
# Requires `uv` to be installed and on PATH. This installer does not bootstrap
# uv; install it first if missing:
#   https://docs.astral.sh/uv/getting-started/installation/

set -eu

REPO_URL="git+https://github.com/sanjaynagi/ginkgo.git@main"

info() { printf '\033[1;32m==>\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33mwarning:\033[0m %s\n' "$1" >&2; }
err()  { printf '\033[1;31merror:\033[0m %s\n' "$1" >&2; }

# Require uv — this installer does not bootstrap it.
if ! command -v uv >/dev/null 2>&1; then
    err "uv is required but was not found on PATH."
    err "Install uv first, then re-run this script:"
    err "  https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

# Install (or reinstall) the ginkgo CLI into its own isolated environment.
info "Installing ginkgo from ${REPO_URL}"
uv tool install --force "$REPO_URL"

# Pixi is needed at runtime for pixi-backed task environments; warn if absent.
if ! command -v pixi >/dev/null 2>&1; then
    warn "pixi was not found on PATH."
    warn "Workflows that use pixi-backed task environments need pixi installed:"
    warn "  https://pixi.sh/"
fi

# Confirm the CLI is reachable on PATH.
if command -v ginkgo >/dev/null 2>&1; then
    info "Installed: $(ginkgo --version 2>/dev/null || echo ginkgo)"
else
    warn "ginkgo was installed but is not on PATH."
    warn "Add uv's tool bin directory to PATH, then restart your shell:"
    warn "  uv tool update-shell"
fi
