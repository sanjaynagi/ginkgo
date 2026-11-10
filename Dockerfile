# Ginkgo remote worker image, which can also run the driver.
#
# Build:
#   docker build -t ginkgo-worker .
#
# By default it runs a worker: it reads a task payload from the
# GINKGO_WORKER_PAYLOAD env var, executes it, and prints a result line to
# stdout. Run with the command `ginkgo run <workflow>` instead, it is a driver
# that dispatches tasks to Kubernetes (see deploy/oke/).

FROM python:3.12-slim

# Prevent Python from writing .pyc files and enable unbuffered output
# so pod logs appear immediately.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Upgraded so the image carries Debian's security fixes, not only the base's.
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# pixi, which installs a task's Pixi env= from the lock in the code bundle.
COPY --from=ghcr.io/prefix-dev/pixi:0.82.1 /usr/local/bin/pixi /usr/local/bin/pixi

WORKDIR /app

# Install ginkgo, the object-store plugins (s3://, gs://, oci://), Pixi envs,
# notebook tasks (with marimo), and the Kubernetes executor, which only a
# driver uses. ginkgo-gcp brings the Batch client libraries along with the
# gs:// store.
COPY pyproject.toml README.md ./
COPY src/ginkgo/ src/ginkgo/
COPY plugins/ginkgo-aws/ plugins/ginkgo-aws/
COPY plugins/ginkgo-gcp/ plugins/ginkgo-gcp/
COPY plugins/ginkgo-oci/ plugins/ginkgo-oci/
COPY plugins/ginkgo-pixi/ plugins/ginkgo-pixi/
COPY plugins/ginkgo-notebook/ plugins/ginkgo-notebook/
COPY plugins/ginkgo-k8s/ plugins/ginkgo-k8s/
RUN pip install --no-cache-dir . ./plugins/ginkgo-aws ./plugins/ginkgo-gcp ./plugins/ginkgo-oci \
    ./plugins/ginkgo-pixi ./plugins/ginkgo-notebook ./plugins/ginkgo-k8s

ENTRYPOINT ["python", "-m", "ginkgo.remote.worker"]
