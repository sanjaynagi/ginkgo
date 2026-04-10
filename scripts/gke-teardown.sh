#!/usr/bin/env bash
#
# Tear down the GKE Autopilot cluster and Artifact Registry created by gke-setup.sh.
#
# This deletes:
#   1. The GKE cluster (and all workloads in it)
#   2. The Artifact Registry repository (and all images in it)
#
# Usage:
#   bash scripts/gke-teardown.sh

set -euo pipefail

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION="europe-west2"
CLUSTER_NAME="ginkgo"
REPO_NAME="ginkgo"

echo "=== Ginkgo GKE Teardown ==="
echo ""
echo "  Project: ${PROJECT_ID}"
echo "  Cluster: ${CLUSTER_NAME}"
echo "  Registry: ${REPO_NAME}"
echo ""
echo "  This will permanently delete the cluster and all images."
echo ""
read -rp "Continue? [y/N] " confirm
if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo ">>> Deleting GKE cluster (this takes a few minutes)..."
gcloud container clusters delete "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet || echo "    Cluster not found or already deleted."

echo ""
echo ">>> Deleting Artifact Registry repository..."
gcloud artifacts repositories delete "${REPO_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet || echo "    Repository not found or already deleted."

echo ""
echo "=== Teardown complete ==="
