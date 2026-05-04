.PHONY: build test lint migrate

BIN := bin/erebor-ingest
MAIN := ./cmd/erebor-ingest
MIGRATION := migrations/001_initial_schema.sql

build:
	mkdir -p bin
	go build -o $(BIN) $(MAIN)

test:
	go test -race ./...

lint:
	golangci-lint run

migrate:
	@if [ -z "$$DATABASE_DSN" ]; then \
	    echo "DATABASE_DSN environment variable is required"; \
	    exit 1; \
	fi
	psql "$$DATABASE_DSN" -v ON_ERROR_STOP=1 -f $(MIGRATION)
