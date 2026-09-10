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
COMPOSE_FILE := deploy/docker/docker-compose.yml

CHART_DIR := deploy/helm/pubsub-event-processor
KIND_CLUSTER ?= dev
K8S_NAMESPACE ?= pubsub-demo
HELM_RELEASE ?= demo
HELM_VALUES ?= deploy/helm/values-kind.yaml
# Label selector addressing the processor pods only, not the bundled Postgres.
K8S_SELECTOR := app.kubernetes.io/component=processor

DATABASE_URL ?= postgres://postgres:postgres@localhost:5433/pubsub?sslmode=disable
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
	$(GO) test -race -count=1 -tags=integration -timeout=5m ./internal/storage/...

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

.PHONY: down
down: ## Stop local infrastructure (keep volumes)
	$(DOCKER) compose -f $(COMPOSE_FILE) down

.PHONY: down-clean
down-clean: ## Stop local infrastructure and delete volumes
	$(DOCKER) compose -f $(COMPOSE_FILE) down -v

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
