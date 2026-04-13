#!/usr/bin/env bash
#
# Build and push a custom ginkgo worker image containing this project's
# Python dependencies, auto-detected from pyproject.toml.
#
# Usage:
#   GINKGO_REGISTRY=<registry-path> scripts/build-worker.sh
#
# Reads [project].dependencies from pyproject.toml, drops anything
# already in the ginkgo base image (ginkgo, pytest), and installs the
# rest on top of the ginkgo worker base.
#
# Environment variables:
#   GINKGO_REGISTRY    OCI registry path (required). Examples:
#                        europe-west2-docker.pkg.dev/my-project/ginkgo
#                        ghcr.io/myuser
#                        123456789.dkr.ecr.us-east-1.amazonaws.com/ginkgo
#   GINKGO_BASE_IMAGE  Override the base ginkgo worker image
#                        (default: ${GINKGO_REGISTRY}/worker:v2)
#   GINKGO_REPO_NAME   Target image repo name (default: <project>-worker)
#
# Requires: docker (with buildx), python3 (3.11+), registry auth.

set -euo pipefail

if [[ -z "${GINKGO_REGISTRY:-}" ]]; then
  echo "error: GINKGO_REGISTRY must be set (e.g. ghcr.io/myuser or" >&2
  echo "  europe-west2-docker.pkg.dev/my-project/ginkgo)" >&2
  exit 1
fi

REGISTRY="${GINKGO_REGISTRY}"
BASE_IMAGE="${GINKGO_BASE_IMAGE:-${REGISTRY}/worker:v2}"

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYPROJECT="${PROJECT_ROOT}/pyproject.toml"
REPO_NAME="${GINKGO_REPO_NAME:-$(basename "$PROJECT_ROOT")-worker}"

if [[ ! -f "$PYPROJECT" ]]; then
  echo "error: $PYPROJECT not found." >&2
  exit 1
fi

# Extract [project].dependencies, stripping the base-image packages.
REQUIREMENTS=$(python3 - "$PYPROJECT" <<'PY'
import sys, tomllib

EXCLUDE = {"ginkgo", "pytest"}

with open(sys.argv[1], "rb") as f:
    data = tomllib.load(f)

deps = data.get("project", {}).get("dependencies", [])
kept = []
for spec in deps:
    name = spec.split(">")[0].split("<")[0].split("=")[0].split("[")[0].strip()
    if name.lower() in EXCLUDE:
        continue
    kept.append(spec)

print("\n".join(kept))
PY
)

if [[ -z "$REQUIREMENTS" ]]; then
  echo "error: no installable dependencies found in $PYPROJECT" >&2
  exit 1
fi

echo "→ Detected dependencies:"
echo "$REQUIREMENTS" | sed 's/^/    /'

# Content-addressed tag: rebuild only when deps change.
DEPS_HASH=$(printf '%s' "$REQUIREMENTS" | shasum -a 256 | cut -c1-12)
IMAGE_REF="${REGISTRY}/${REPO_NAME}:${DEPS_HASH}"

echo "→ Base image: $BASE_IMAGE"
echo "→ Target:     $IMAGE_REF"

# Skip build if image already exists in the registry. docker manifest
# inspect works against any OCI-compliant registry.
if docker manifest inspect "$IMAGE_REF" >/dev/null 2>&1; then
  echo "✓ Image already exists in registry, skipping build."
  echo
  echo "Use this in ginkgo.toml:"
  echo "  image = \"${IMAGE_REF}\""
  exit 0
fi

BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT

printf '%s\n' "$REQUIREMENTS" >"$BUILD_DIR/requirements.txt"
cat >"$BUILD_DIR/Dockerfile" <<EOF
FROM ${BASE_IMAGE}
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
EOF

echo "→ Building (linux/amd64) and pushing..."
docker buildx build \
  --platform linux/amd64 \
  -t "$IMAGE_REF" \
  --push \
  "$BUILD_DIR"

echo
echo "✓ Pushed ${IMAGE_REF}"
echo
echo "Use this in ginkgo.toml:"
echo "  image = \"${IMAGE_REF}\""
