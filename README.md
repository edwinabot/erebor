# erebor

[![CI](https://github.com/edwinabot/erebor/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/edwinabot/erebor/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/edwinabot/erebor/branch/main/graph/badge.svg)](https://codecov.io/gh/edwinabot/erebor)

## Continuous Integration

CI runs on every push and pull request to `main` via GitHub Actions
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) and is split into three jobs:

- **format** — verifies `gofmt` cleanliness and that `go.mod` / `go.sum` are tidy.
- **lint** — runs `golangci-lint` (version pinned to match `.qlty/qlty.toml`).
- **test** — runs `go test -race -covermode=atomic -coverprofile=coverage.out ./...`,
  uploads `coverage.out` as a build artifact, and publishes the report to
  [Codecov](https://codecov.io/gh/edwinabot/erebor).

### Test coverage

Generate a coverage report locally with:

```sh
go test -race -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out      # textual summary
go tool cover -html=coverage.out      # browser report
```

To enable the Codecov badge, add a `CODECOV_TOKEN` repository secret in
GitHub. Public repositories may also use Codecov tokenless uploads.
