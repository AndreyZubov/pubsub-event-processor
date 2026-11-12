SHELL := /usr/bin/env bash
MODULE := github.com/AndreyZubov/pubsub-event-processor
BIN_DIR := bin
BIN := $(BIN_DIR)/processor
PKG := ./...

GO ?= go
DOCKER ?= docker

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-X main.version=$(VERSION) -s -w"

GOLANGCI_LINT_VERSION := v2.13.2
MIGRATE_IMAGE := migrate/migrate:v4.18.1
GOVULNCHECK_VERSION := v1.9.0
PROMETHEUS_IMAGE ?= prom/prometheus:v3.5.0
PROMTOOL := $(DOCKER) run --rm -v "$(CURDIR)/deploy/prometheus":/rules -w /rules --entrypoint promtool $(PROMETHEUS_IMAGE)
COMPOSE_FILE := deploy/docker/docker-compose.yml

CHART_DIR := deploy/helm/pubsub-event-processor
KIND_CLUSTER ?= dev
K8S_NAMESPACE ?= pubsub-demo
HELM_RELEASE ?= demo
HELM_VALUES ?= deploy/helm/values-kind.yaml
# Label selector addressing the processor pods only, not the bundled Postgres.
K8S_SELECTOR := app.kubernetes.io/component=processor

# Host port of the local Postgres container; change it if 5433 is taken.
POSTGRES_PORT ?= 5433
export POSTGRES_PORT
DATABASE_URL ?= postgres://postgres:postgres@localhost:$(POSTGRES_PORT)/pubsub?sslmode=disable

# The demo keeps its events in a database of its own, so replay cursors of the
# fake org never mix with those of a real one.
DEMO_DATABASE_URL := postgres://postgres:postgres@localhost:$(POSTGRES_PORT)/pubsub_demo?sslmode=disable
DEMO_CA_FILE := $(BIN_DIR)/fakeorg-ca.pem
DEMO_RATE ?= 20
MIGRATIONS_DIR := migrations

HAS_GOLANGCI_LINT := $(shell command -v golangci-lint 2> /dev/null)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build the processor binary into bin/
	@mkdir -p $(BIN_DIR)
	$(GO) build $(LDFLAGS) -o $(BIN) ./cmd/processor

.PHONY: run
run: build ## Build and run the processor locally
	./$(BIN)

.PHONY: test
test: ## Run unit tests with race detector
	$(GO) test -race -count=1 $(PKG)

.PHONY: integration-test
integration-test: ## Run integration tests (requires Docker; uses testcontainers)
	$(GO) test -race -count=1 -tags=integration -timeout=15m ./internal/app/... ./internal/storage/... ./internal/kafka/... ./internal/rediscache/...

.PHONY: bench
bench: ## Benchmark duplicate handling with and without the dedup cache
	$(GO) test -tags=integration -bench=BenchmarkDuplicates -benchtime=2000x \
	  -run='^$$' -timeout=15m ./internal/handler/

.PHONY: cover
cover: ## Run tests with coverage, write coverage.out and coverage.html
	$(GO) test -race -coverprofile=coverage.out $(PKG)
	@$(GO) tool cover -func=coverage.out | tail -1
	@$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "coverage report: coverage.html"

.PHONY: lint
lint: ## Run golangci-lint (local if installed, else Docker)
ifdef HAS_GOLANGCI_LINT
	golangci-lint run
else
	$(DOCKER) run --rm -v $$PWD:/app -w /app golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) golangci-lint run
endif

.PHONY: fmt
fmt: ## Format code via golangci-lint formatters (gofumpt, gci)
ifdef HAS_GOLANGCI_LINT
	golangci-lint fmt
else
	$(DOCKER) run --rm -v $$PWD:/app -w /app golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) golangci-lint fmt
endif

.PHONY: tidy
tidy: ## Run go mod tidy
	$(GO) mod tidy

.PHONY: vuln
vuln: ## Scan dependencies and the standard library with govulncheck
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

.PHONY: check-alerts
check-alerts: ## Validate the Prometheus alerting rules and run their unit tests (via Docker)
	$(PROMTOOL) check rules alerts.yml
	$(PROMTOOL) test rules alerts_test.yml

.PHONY: proto
proto: ## Generate Go code from .proto files (Docker-based)
	./scripts/gen_proto.sh

.PHONY: proto-check
proto-check: ## Regenerate and fail if generated code differs from committed (CI guard)
	./scripts/gen_proto.sh
	@if ! git diff --quiet -- proto/; then \
	  echo "generated proto code differs from committed code; run make proto and commit"; \
	  git --no-pager diff --stat -- proto/; \
	  exit 1; \
	fi

.PHONY: migrate-up
migrate-up: ## Apply all pending migrations
	$(DOCKER) run --rm \
	  -v $$PWD/$(MIGRATIONS_DIR):/migrations \
	  --network host \
	  $(MIGRATE_IMAGE) -path=/migrations -database "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Roll back the most recent migration
	$(DOCKER) run --rm \
	  -v $$PWD/$(MIGRATIONS_DIR):/migrations \
	  --network host \
	  $(MIGRATE_IMAGE) -path=/migrations -database "$(DATABASE_URL)" down 1

.PHONY: migrate-create
migrate-create: ## Create a new migration: make migrate-create name=add_foo
	@test -n "$(name)" || (echo "usage: make migrate-create name=<migration_name>"; exit 1)
	$(DOCKER) run --rm \
	  -v $$PWD/$(MIGRATIONS_DIR):/migrations \
	  $(MIGRATE_IMAGE) create -ext sql -dir /migrations -seq $(name)

.PHONY: up
up: ## Start local infrastructure (Postgres) and wait for healthchecks
	$(DOCKER) compose -f $(COMPOSE_FILE) up -d --wait

.PHONY: up-observability
up-observability: ## Start Postgres plus Prometheus and Grafana with the bundled dashboard
	$(DOCKER) compose -f $(COMPOSE_FILE) --profile observability up -d --wait

.PHONY: demo-org
demo-org: ## Demo, terminal 1: infrastructure, dashboard, and a fake Salesforce org emitting events
	$(DOCKER) compose -f $(COMPOSE_FILE) --profile observability up -d --wait
	$(DOCKER) exec pubsub-postgres dropdb -U postgres --if-exists --force pubsub_demo
	$(DOCKER) exec pubsub-postgres createdb -U postgres pubsub_demo
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/fakeorg ./cmd/fakeorg
	./$(BIN_DIR)/fakeorg -ca-out $(DEMO_CA_FILE) -rate $(DEMO_RATE)

.PHONY: demo
demo: build ## Demo, terminal 2: the processor, subscribed to the fake org
	@test -s $(DEMO_CA_FILE) || (echo "start 'make demo-org' in another terminal first"; exit 1)
	SF_CLIENT_ID=demo SF_CLIENT_SECRET=demo \
	SF_LOGIN_URL=http://127.0.0.1:8090 \
	SF_TOPICS=/event/Order_Event__e \
	PUBSUB_ENDPOINT=127.0.0.1:7443 \
	PUBSUB_CA_FILE=$(DEMO_CA_FILE) \
	DATABASE_URL="$(DEMO_DATABASE_URL)" \
	HTTP_ADDR=:8080 \
	  ./$(BIN)

.PHONY: down
down: ## Stop local infrastructure (keep volumes)
	$(DOCKER) compose -f $(COMPOSE_FILE) --profile observability down

.PHONY: down-clean
down-clean: ## Stop local infrastructure and delete volumes
	$(DOCKER) compose -f $(COMPOSE_FILE) --profile observability down -v

.PHONY: logs
logs: ## Tail logs from local infrastructure
	$(DOCKER) compose -f $(COMPOSE_FILE) logs -f

.PHONY: docker-build
docker-build: ## Build the service Docker image
	$(DOCKER) build \
	  -t pubsub-event-processor:$(VERSION) \
	  --build-arg VERSION=$(VERSION) \
	  -f deploy/docker/Dockerfile .

.PHONY: install-hooks
install-hooks: ## Install git pre-commit hook
	@chmod +x scripts/hooks/pre-commit
	@ln -sf ../../scripts/hooks/pre-commit .git/hooks/pre-commit
	@echo "pre-commit hook installed"

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(BIN_DIR) coverage.out coverage.html

# ---------------------------------------------------------------------------
# Kubernetes (local kind cluster)
# ---------------------------------------------------------------------------

.PHONY: helm-lint
helm-lint: ## Lint the Helm chart
	helm lint $(CHART_DIR) -f $(HELM_VALUES)

.PHONY: helm-template
helm-template: ## Render the chart to stdout
	helm template $(HELM_RELEASE) $(CHART_DIR) -f $(HELM_VALUES)

.PHONY: helm-validate
helm-validate: ## Validate rendered manifests against the live cluster API
	helm template $(HELM_RELEASE) $(CHART_DIR) -f $(HELM_VALUES) \
	  | kubectl apply --dry-run=server -f -

.PHONY: kind-up
kind-up: ## Create the local kind cluster if it does not exist
	@kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)" \
	  && echo "kind cluster '$(KIND_CLUSTER)' already exists" \
	  || kind create cluster --name $(KIND_CLUSTER)

.PHONY: kind-load
kind-load: docker-build ## Build the image and side-load it into kind
	$(DOCKER) tag pubsub-event-processor:$(VERSION) pubsub-event-processor:dev
	kind load docker-image pubsub-event-processor:dev --name $(KIND_CLUSTER)

.PHONY: k8s-deploy
k8s-deploy: kind-load ## Deploy the chart into the kind cluster
	helm upgrade --install $(HELM_RELEASE) $(CHART_DIR) \
	  -f $(HELM_VALUES) \
	  -n $(K8S_NAMESPACE) --create-namespace
	@echo
	@echo "Deployed. With placeholder Salesforce credentials the pod stays Running"
	@echo "and readiness reports 503 — that is expected. Run 'make k8s-status'."

.PHONY: k8s-status
k8s-status: ## Show pods, endpoints, and probe state
	@echo "== pods =="
	@kubectl -n $(K8S_NAMESPACE) get pods -L app.kubernetes.io/component
	@echo
	@echo "== service endpoints (empty until readiness passes) =="
	@kubectl -n $(K8S_NAMESPACE) get endpointslice \
	  -l kubernetes.io/service-name=$(HELM_RELEASE)-pubsub-event-processor \
	  -o jsonpath='{range .items[*].endpoints[*]}{.addresses[0]}{"  ready="}{.conditions.ready}{"\n"}{end}'

.PHONY: k8s-logs
k8s-logs: ## Tail processor logs
	kubectl -n $(K8S_NAMESPACE) logs -l $(K8S_SELECTOR) -c processor -f --tail=50

.PHONY: k8s-forward
k8s-forward: ## Forward the admin port to localhost:8080
	@echo "curl localhost:8080/healthz | /readyz | /metrics"
	kubectl -n $(K8S_NAMESPACE) port-forward svc/$(HELM_RELEASE)-pubsub-event-processor 8080:8080

.PHONY: k8s-test
k8s-test: ## Run the chart's Helm test hook (requires a ready release)
	helm test $(HELM_RELEASE) -n $(K8S_NAMESPACE) --logs

.PHONY: k8s-undeploy
k8s-undeploy: ## Remove the release and its namespace
	-helm uninstall $(HELM_RELEASE) -n $(K8S_NAMESPACE)
	-kubectl delete namespace $(K8S_NAMESPACE) --wait=false
