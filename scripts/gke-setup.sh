#!/usr/bin/env bash
#
# Set up a minimal GKE Autopilot cluster for ginkgo remote execution.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - A GCP project with billing enabled
#   - Docker installed (for building the worker image)
#
# Usage:
#   bash scripts/gke-setup.sh
#
# What this creates:
#   1. Artifact Registry repository (to store the worker Docker image)
#   2. GKE Autopilot cluster (scales to zero when idle — you only pay for pods)
#   3. Builds and pushes the ginkgo worker image
#   4. Creates a K8s namespace for ginkgo workloads
#
# To tear everything down later:
#   bash scripts/gke-teardown.sh

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION="europe-west2"
CLUSTER_NAME="ginkgo"
NAMESPACE="ginkgo"
REPO_NAME="ginkgo"
IMAGE_NAME="worker"
IMAGE_TAG="latest"

# Derived values.
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}"
FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "=== Ginkgo GKE Setup ==="
echo ""
echo "  Project:   ${PROJECT_ID}"
echo "  Region:    ${REGION}"
echo "  Cluster:   ${CLUSTER_NAME}"
echo "  Namespace: ${NAMESPACE}"
echo "  Image:     ${FULL_IMAGE}"
echo ""
read -rp "Continue? [y/N] " confirm
if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

# ── Step 1: Enable required APIs ─────────────────────────────────────────────

echo ""
echo ">>> Enabling GCP APIs..."
gcloud services enable \
    container.googleapis.com \
    artifactregistry.googleapis.com \
    --project="${PROJECT_ID}"

# ── Step 2: Create Artifact Registry repository ──────────────────────────────

echo ""
echo ">>> Creating Artifact Registry repository..."
if gcloud artifacts repositories describe "${REPO_NAME}" \
    --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    Already exists, skipping."
else
    gcloud artifacts repositories create "${REPO_NAME}" \
        --repository-format=docker \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --description="Ginkgo worker images"
fi

# ── Step 2b: Grant GKE nodes access to Artifact Registry ────────────────────
#
# GKE Autopilot nodes use the default compute service account. It needs
# permission to pull images from Artifact Registry.

echo ""
echo ">>> Granting Artifact Registry Reader to GKE nodes..."
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/artifactregistry.reader" \
    --condition=None \
    --quiet > /dev/null

# ── Step 3: Create GKE Autopilot cluster ─────────────────────────────────────
#
# Autopilot manages node pools automatically. You only pay for the resources
# your pods actually request. When no pods are running, cost is near zero.

echo ""
echo ">>> Creating GKE Autopilot cluster (this takes 5-10 minutes)..."
if gcloud container clusters describe "${CLUSTER_NAME}" \
    --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    Already exists, skipping."
else
    gcloud container clusters create-auto "${CLUSTER_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --release-channel=regular
fi

# ── Step 4: Configure kubectl ────────────────────────────────────────────────

echo ""
echo ">>> Configuring kubectl credentials..."
gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}"

# ── Step 5: Create K8s namespace ─────────────────────────────────────────────

echo ""
echo ">>> Creating namespace '${NAMESPACE}'..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# ── Step 6: Build and push worker image ──────────────────────────────────────

echo ""
echo ">>> Configuring Docker for Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo ""
echo ">>> Building worker image (linux/amd64 for GKE)..."
docker buildx build --platform linux/amd64 -t "${FULL_IMAGE}" --push .

# ── Done ─────────────────────────────────────────────────────────────────────

echo ""
echo "=== Setup complete ==="
echo ""
echo "Add this to your ginkgo.toml to use remote execution:"
echo ""
echo "  [remote.k8s]"
echo "  image = \"${FULL_IMAGE}\""
echo "  namespace = \"${NAMESPACE}\""
echo ""
echo "Then run a workflow with:"
echo ""
echo "  ginkgo run --executor k8s <workflow>"
echo ""
