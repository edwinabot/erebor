.PHONY: build test cover cover-html lint fmt migrate db-up db-down db-logs db-wait db-reset run dev up down logs clean

BIN        := bin/erebor-ingest
MAIN       := ./cmd/erebor-ingest
MIGRATION  := engine/migrations/001_initial_schema.sql
COMPOSE    := docker compose
DB_SERVICE := timescaledb
LOCAL_DSN  := postgres://erebor:erebor_dev@localhost:5432/erebor?sslmode=disable
CONFIG     ?= engine/config.example.yaml
ENGINE_DIR := engine
SERVER_IP  ?= 192.168.1.111

# ---------- Build / test / lint ----------

build:
	mkdir -p bin
	cd $(ENGINE_DIR) && go build -o ../$(BIN) $(MAIN)

test:
	cd $(ENGINE_DIR) && go test -race ./...

cover:
	cd $(ENGINE_DIR) && go test -race -covermode=atomic -coverprofile=coverage.out ./...
	cd $(ENGINE_DIR) && go tool cover -func=coverage.out | tail -n 1

cover-html: cover
	cd $(ENGINE_DIR) && go tool cover -html=coverage.out

fmt:
	cd $(ENGINE_DIR) && gofmt -w .

lint:
	cd $(ENGINE_DIR) && golangci-lint run

qlty:
	qlty check --all

# ---------- Database lifecycle (local dev only) ----------

db-up:
	$(COMPOSE) up -d $(DB_SERVICE)
	@$(MAKE) --no-print-directory db-wait

db-down:
	$(COMPOSE) down

db-reset:
	$(COMPOSE) down -v
	@$(MAKE) --no-print-directory db-up
	@$(MAKE) --no-print-directory migrate

db-logs:
	$(COMPOSE) logs -f $(DB_SERVICE)

db-wait:
	@echo "waiting for TimescaleDB to be ready..."
	@for i in $$(seq 1 30); do \
	    if $(COMPOSE) exec -T $(DB_SERVICE) pg_isready -U erebor -d erebor >/dev/null 2>&1; then \
	        echo "TimescaleDB ready"; \
	        exit 0; \
	    fi; \
	    sleep 1; \
	done; \
	echo "TimescaleDB did not become ready in time"; exit 1

migrate:
	@DSN="$${DATABASE_DSN:-$(LOCAL_DSN)}"; \
	if [ -z "$$DSN" ]; then \
	    echo "DATABASE_DSN is required"; exit 1; \
	fi; \
	if psql --version >/dev/null 2>&1; then \
	    echo "applying $(MIGRATION) via host psql to $$DSN"; \
	    psql "$$DSN" -v ON_ERROR_STOP=1 -f $(MIGRATION); \
	else \
	    echo "host psql not found — applying $(MIGRATION) via docker compose exec"; \
	    $(COMPOSE) exec -T $(DB_SERVICE) psql "$$DSN" -v ON_ERROR_STOP=1 -f - < $(MIGRATION); \
	fi

psql:
	@DSN="$${DATABASE_DSN:-$(LOCAL_DSN)}"; \
	if [ -z "$$DSN" ]; then \
	    echo "DATABASE_DSN is required"; exit 1; \
	fi; \
	if psql --version >/dev/null 2>&1; then \
	    echo "launching host psql to $$DSN"; \
	    psql "$$DSN"; \
	else \
	    echo "host psql not found — launching docker compose exec psql"; \
	    $(COMPOSE) exec -it $(DB_SERVICE) psql "$$DSN"; \
	fi

# ---------- End-to-end local run ----------

# `make run` brings up the database, applies the migration, and runs the
# CLI against the local TimescaleDB. Requires BINANCE_API_KEY and
# BINANCE_API_SECRET to be set in the environment.
run: build db-up migrate
	@if [ -z "$$BINANCE_API_KEY" ] || [ -z "$$BINANCE_API_SECRET" ]; then \
	    echo "BINANCE_API_KEY and BINANCE_API_SECRET must be set"; exit 1; \
	fi
	DATABASE_DSN="$${DATABASE_DSN:-$(LOCAL_DSN)}" $(BIN) --config $(CONFIG)

# `make dev` is a shorthand for build + db-up + migrate without launching
# the CLI, useful when iterating with `go run`.
dev: db-up migrate

# ---------- Full stack (Docker Compose) ----------

up:
	$(COMPOSE) up --build -d
	@$(MAKE) --no-print-directory stack-wait
	@echo "stack ready"
	@echo "  ingest    http://$(SERVER_IP):8080"
	@echo "  grafana   http://$(SERVER_IP):3000"
	@echo "  dashboard http://$(SERVER_IP):3001"

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

stack-wait:
	@echo "waiting for ingest health probe..."
	@for i in $$(seq 1 60); do \
	    if curl -s http://localhost:8080/healthz >/dev/null 2>&1; then \
	        echo "ingest healthy"; exit 0; \
	    fi; \
	    sleep 2; \
	done; \
	echo "ingest did not become healthy in time"; exit 1

# ---------- Clean ----------

clean:
	rm -rf bin
