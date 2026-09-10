# syntax=docker/dockerfile:1

# --- build stage ---
FROM golang:1.26 AS build
WORKDIR /src

# Dependencies first so source edits do not invalidate the download layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
# CGO off produces a fully static binary, required by the distroless runtime.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/processor ./cmd/processor

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev
LABEL org.opencontainers.image.title="pubsub-event-processor" \
      org.opencontainers.image.description="Go microservice consuming Salesforce Platform Events over the gRPC Pub/Sub API" \
      org.opencontainers.image.source="https://github.com/AndreyZubov/pubsub-event-processor" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

WORKDIR /
COPY --from=build /out/processor /processor
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/processor"]
