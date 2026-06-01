#!/usr/bin/env bash
# Generates Go code from .proto files using a containerized protoc + Go plugins.
# The image is built once and cached locally; subsequent runs reuse it.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="pubsub-event-processor-proto:1"
DOCKERFILE="${REPO_ROOT}/scripts/proto/Dockerfile"

PROTO_FILE="proto/salesforce/pubsub_api.proto"

if ! docker image inspect "${IMAGE_TAG}" >/dev/null 2>&1; then
    echo "building proto-gen image ${IMAGE_TAG}..."
    docker build -t "${IMAGE_TAG}" -f "${DOCKERFILE}" "${REPO_ROOT}/scripts/proto"
fi

docker run --rm \
    -v "${REPO_ROOT}:/workspace" \
    -w /workspace \
    "${IMAGE_TAG}" \
    --proto_path=. \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    "${PROTO_FILE}"

echo "generated:"
ls -1 proto/salesforce/*.pb.go
