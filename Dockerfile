# Ginkgo remote worker image.
#
# Build:
#   docker build -t ginkgo-worker .
#
# The worker reads a task payload from the GINKGO_WORKER_PAYLOAD env var,
# executes it, and prints a JSON result line to stdout.

FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output
# so pod logs appear immediately.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install ginkgo and the object-store plugins (s3://, gs://, oci://). A worker
# runs tasks, so it needs no executor plugin; ginkgo-gcp brings the Batch
# client libraries along with the gs:// store.
COPY pyproject.toml .
COPY src/ginkgo/ src/ginkgo/
COPY plugins/ginkgo-aws/ plugins/ginkgo-aws/
COPY plugins/ginkgo-gcp/ plugins/ginkgo-gcp/
COPY plugins/ginkgo-oci/ plugins/ginkgo-oci/
RUN pip install --no-cache-dir . ./plugins/ginkgo-aws ./plugins/ginkgo-gcp ./plugins/ginkgo-oci

ENTRYPOINT ["python", "-m", "ginkgo.remote.worker"]
