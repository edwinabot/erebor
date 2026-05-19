module github.com/edwinabot/erebor/backtest

go 1.22.0

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/edwinabot/erebor/fillmath v0.0.0-00010101000000-000000000000
	github.com/edwinabot/erebor/ingest v0.0.0
	github.com/edwinabot/erebor/risk v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.9.2
	github.com/redis/go-redis/v9 v9.7.3
	github.com/shopspring/decimal v1.4.0
	github.com/stretchr/testify v1.11.1
	go.uber.org/zap v1.28.0
	golang.org/x/sync v0.17.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/edwinabot/erebor/ingest => ../

replace github.com/edwinabot/erebor/risk => ../risk

replace github.com/edwinabot/erebor/fillmath => ../fillmath
