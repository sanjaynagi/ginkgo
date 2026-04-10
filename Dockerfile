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

# Install ginkgo with cloud dependencies (kubernetes, gcsfs, google-auth).
COPY pyproject.toml .
COPY ginkgo/ ginkgo/
RUN pip install --no-cache-dir ".[cloud]"

ENTRYPOINT ["python", "-m", "ginkgo.remote.worker"]
