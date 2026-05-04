.PHONY: build test lint migrate db-up db-down db-logs db-wait db-reset run dev clean

BIN        := bin/erebor-ingest
MAIN       := ./cmd/erebor-ingest
MIGRATION  := migrations/001_initial_schema.sql
COMPOSE    := docker compose
DB_SERVICE := timescaledb
LOCAL_DSN  := postgres://erebor:erebor_dev@localhost:5432/erebor?sslmode=disable
CONFIG     ?= config.example.yaml

# ---------- Build / test / lint ----------

build:
	mkdir -p bin
	go build -o $(BIN) $(MAIN)

test:
	go test -race ./...

lint:
	golangci-lint run

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
	echo "applying $(MIGRATION) to $$DSN"; \
	psql "$$DSN" -v ON_ERROR_STOP=1 -f $(MIGRATION)

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

clean:
	rm -rf bin
