# A workflow's image, built on the ginkgo base image (the repo's Dockerfile).
# The driver and its workers run this one image, so the workflow's code is at
# the same path in both and workers load tasks from it directly.
#
# Build from the workflow project's root:
#   docker build -f <path>/Dockerfile -t <region>.ocir.io/<os-namespace>/ginkgo/<workflow>:<tag> .

FROM <region>.ocir.io/<os-namespace>/ginkgo/worker:<tag>

# The workflow's own Python dependencies. Delete these two lines if it has none.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The project: ginkgo.toml and the workflow package. The driver runs here.
COPY . /project
WORKDIR /project
