# Salesforce Pub/Sub Event Processor

> Go microservice that subscribes to Salesforce Platform Events and Change Data Capture channels over the gRPC Pub/Sub API, decodes Avro payloads, and reliably persists events to PostgreSQL with at-least-once delivery and resumable replay.

**Status:** work in progress. Authentication and the gRPC client are functional; event subscription, Avro decoding, persistence, and sink forwarding are on the roadmap below.

---

## Why this project

The Salesforce Pub/Sub API is a modern gRPC + HTTP/2 service that uses Avro for payload serialization. 

The service connects to a Salesforce org as an OAuth client, subscribes to one or more Platform Event topics, decodes events, and pushes them downstream.

---

## Architecture

```
            Salesforce Pub/Sub API (gRPC, TLS, HTTP/2)
                          |
                 [ Subscriber ]   <- stream, replay IDs, flow control
                          |
                    events channel
                          |
                 [ Worker pool ]   <- N goroutines, bounded
                      /        \
              [ Decoder ]   (Avro schema cache)
                      |
               [ Processor ]      <- idempotent handler
                   /     \
          [ Postgres ]   [ Sink: webhook / channel ]
                          |
              [ Replay store ]    <- persists last replay ID
```

Cross-cutting components: OAuth token provider with cached refresh, structured zap logger, Prometheus metrics registry, environment-based config loader, graceful shutdown across all goroutines.

---

## Current functionality

What works today (in `make run`):

- **OAuth client-credentials flow** against a Salesforce org with cached, refresh-ahead-of-expiry token management and concurrency-safe deduplication of refresh requests.
- **gRPC client** to `api.pubsub.salesforce.com` with TLS 1.2+, HTTP/2 keepalive, per-RPC credentials that inject the three required Salesforce metadata headers (`accesstoken`, `instanceurl`, `tenantid`) automatically.
- **Unary interceptor** that adds Prometheus metrics, structured logging, and exponential-backoff retries with jitter on transient `UNAVAILABLE` / `DEADLINE_EXCEEDED` errors.
- **Typed wrappers** for `GetTopic` and `GetSchema` with sentinel errors for `NotFound`.
- **Bidirectional subscribe stream** — per-topic Subscriber opens a Pub/Sub `Subscribe` stream, drives pull-based flow control via `FetchRequest`, reconnects with exponential backoff and jitter on stream errors, and recovers replay capacity through downstream acknowledgements.
- **Avro schema cache** — fetched schemas are parsed once and reused; concurrent cold-start lookups for the same `schema_id` are deduplicated via `singleflight`.
- **Avro decoder** — payload bytes are decoded into a generic `map[string]any` keyed by field name, ready for storage and downstream sinks.
- **End-to-end event pipeline** — subscribers fan-in into a single events channel that a consumer goroutine drains, decoding each event and emitting a structured log line per event.
- **Admin HTTP server** exposing `/healthz`, `/readyz` (aggregates per-subsystem checks), and `/metrics`.
- **Startup topic discovery** — for each configured topic, the service queries Salesforce for its metadata and Avro schema and logs the result.
- **Graceful shutdown** on `SIGINT` / `SIGTERM` with bounded drain timeouts coordinated through `errgroup`.

Not yet implemented (see [Roadmap](#roadmap)).

---

## Quick start

### Prerequisites

- Go 1.24 or newer (the project currently builds against Go 1.26).
- Docker (used for golangci-lint, proto generation, golang-migrate, and the local Postgres container).
- A Salesforce Developer Edition org (or sandbox) with an External Client App configured for the OAuth Client Credentials flow.

### Configure a Salesforce External Client App

External Client Apps replace the legacy Connected Apps for new integrations.

1. **Setup → External Client App Manager → New External Client App**.
2. Fill in the basic information (name, contact email, distribution state = `Local`).
3. In **API (Enable OAuth Settings)**:
   - Enable OAuth.
   - Set any callback URL such as `http://localhost/oauth/callback` (unused for client credentials but required).
   - Select OAuth scopes: `api`, `refresh_token`, `offline_access`.
4. Save the app.
5. Open the new app and go to the **Policies** tab → **Edit**:
   - Enable **Client Credentials Flow**.
   - Choose a **Run As** user — the API will execute under that user's profile and permissions.
6. Save. Wait a few minutes for the new credentials to propagate.
7. Open the **Settings** tab → **Consumer Key and Secret** → reveal and copy:
   - **Consumer Key** → `SF_CLIENT_ID`
   - **Consumer Secret** → `SF_CLIENT_SECRET`
8. Optionally, create a Platform Event (Setup → Platform Events → New) to subscribe to, for example `Order_Event` (the API name becomes `/event/Order_Event__e`).

### Configure the service

Copy `.env.example` to `.env` and fill in the Salesforce credentials. `.env` is git-ignored.

```bash
cp .env.example .env
# edit .env with your Connected App credentials and topic list
```

Required values:

| Variable | Purpose |
|----------|---------|
| `SF_CLIENT_ID` | Connected App Consumer Key |
| `SF_CLIENT_SECRET` | Connected App Consumer Secret |
| `SF_LOGIN_URL` | `https://login.salesforce.com` (prod) or `https://test.salesforce.com` (sandbox) |
| `SF_TOPICS` | Comma-separated topic list, e.g. `/event/Order_Event__e` |
| `PUBSUB_ENDPOINT` | `api.pubsub.salesforce.com:7443` |
| `DATABASE_URL` | Postgres DSN (validated at startup; used in later milestones) |

Optional knobs:

| Variable | Default |
|----------|---------|
| `WORKER_COUNT` | `8` |
| `FLOW_BATCH_SIZE` | `100` |
| `HTTP_ADDR` | `:8080` |
| `LOG_LEVEL` | `info` |
| `SINK_WEBHOOK_URL` | _empty_ |

### Run the service

```bash
# Start the local Postgres container.
make up

# Build and run.
make run
```

The service binds the admin HTTP server (default `:8080`) and starts topic discovery. With valid credentials and a configured topic, the logs will show entries like:

```json
{"level":"info","msg":"topic discovered","topic":"/event/Order_Event__e","schema_id":"abc123","can_subscribe":true}
{"level":"info","msg":"schema fetched","schema_id":"abc123","schema_json_bytes":847}
```

Verify the admin endpoints:

```bash
curl localhost:8080/healthz   # liveness
curl localhost:8080/readyz    # readiness (200 if auth check passes, 503 otherwise)
curl localhost:8080/metrics   # Prometheus metrics
```

Stop with `Ctrl+C` for a graceful shutdown.

---

## Roadmap

| Milestone | Scope | Status |
|-----------|-------|--------|
| 1 — Skeleton | Config, logging, health endpoints, Docker, base lifecycle | Done |
| 2 — Auth + gRPC | OAuth token provider, gRPC client, GetTopic / GetSchema, readiness probe | Done |
| 3 — Subscribe + decode | Subscribe stream client, Avro schema cache, Avro decoder | Done |
| 4 — Process + persist | Worker pool, Postgres writes, idempotency on event UUID | Planned |
| 5 — Reliability | Replay ID persistence, reconnect with resume, graceful drain | Planned |
| 6 — Sink + observability | Webhook sink with retries, full metrics dashboard | Planned |
| 7 — Polish | Integration tests with testcontainers, documentation, demo | Planned |

---

## Project layout

```
cmd/processor/             entry point
internal/
  app/                     subsystem wiring and lifecycle
  auth/                    OAuth token provider with cache
  config/                  env-based configuration loader
  event/                   shared decoded-event model
  health/                  Checker interface, /healthz, /readyz
  httpserver/              chi-based admin HTTP server
  log/                     zap logger constructor
  pubsub/                  Salesforce Pub/Sub gRPC client and Subscriber
  schema/                  Avro schema cache and decoder
proto/salesforce/          Salesforce .proto and generated Go code
scripts/                   dev scripts (proto generation, git hooks)
deploy/docker/             docker-compose, service Dockerfile
migrations/                SQL migrations (used in milestone 4)
```

---

## Development

The Makefile is the single entry point for common tasks.

```bash
make help          # list all targets

make build         # compile the binary into bin/processor
make test          # go test -race -count=1 ./...
make cover         # tests with coverage report
make lint          # golangci-lint via Docker
make fmt           # apply gofumpt + gci import ordering

make proto         # regenerate Go code from .proto (Docker-based)
make proto-check   # CI guard: fails if generated code is out of date

make up            # start local Postgres
make down          # stop it (preserves volume)
make down-clean    # stop and delete the volume

make migrate-up    # apply database migrations (requires DATABASE_URL)
make migrate-down  # revert the last migration

make install-hooks # install the project's pre-commit hook
make clean         # remove build artifacts
```

### Pre-commit hook

`make install-hooks` symlinks a git pre-commit hook that runs `make lint` and `make test` on commits that touch Go code or lint configuration. Bypass with `git commit --no-verify` if needed; the same checks run in CI regardless.

### Code style

- Standard Go formatting via `gofumpt`.
- Imports grouped by `gci`: standard library, third-party, then this module.
- `golangci-lint` configuration in `.golangci.yml` enables `errcheck`, `govet`, `gosec`, `revive`, `staticcheck`, `loggercheck` (zap-aware), and several others.

### Regenerating proto code

`make proto` builds a small Docker image with `protoc` plus `protoc-gen-go` and `protoc-gen-go-grpc`, then generates Go code from `proto/salesforce/pubsub_api.proto`. The image is cached after the first build. The proto file itself is a lightly modified copy of the upstream Salesforce schema published at [developerforce/pub-sub-api](https://github.com/developerforce/pub-sub-api) — only the `go_package` option is customized to land the generated code in this module.

---

## Observability

Prometheus metrics exposed at `/metrics` (Go runtime metrics included by default):

| Metric | Type | Labels |
|--------|------|--------|
| `auth_token_refresh_total` | counter | `result` |
| `auth_token_expiry_seconds` | gauge | |
| `pubsub_grpc_rpc_total` | counter | `method`, `code` |
| `pubsub_grpc_rpc_duration_seconds` | histogram | `method` |
| `pubsub_grpc_rpc_retries_total` | counter | `method`, `code` |
| `pubsub_events_received_total` | counter | `topic` |
| `pubsub_reconnects_total` | counter | `topic`, `reason` |
| `pubsub_stream_open` | gauge | `topic` |
| `schema_cache_hits_total` | counter | |
| `schema_cache_misses_total` | counter | |

Structured JSON logs via zap, written to stdout. Every log line includes the `service` and `version` fields (the version is injected at build time from `git describe`).

---

## License

Released under the [MIT License](LICENSE). You are free to use, modify, and distribute this code in both open-source and commercial work; the only obligation is to keep the copyright notice in any substantial copy.

---

## Contributing

This is currently a personal portfolio project. Issues and discussion are welcome via GitHub Issues. Conventional Commits are used for the history; see existing commits for examples.
