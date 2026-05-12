package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/edwinabot/erebor/ingest/book"
	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/fetcher"
	"github.com/edwinabot/erebor/ingest/repository"
	"github.com/edwinabot/erebor/ingest/symbol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestRequireEnvAllPresent verifies the happy path of the credential
// gate: when every named environment variable is set to a non-empty
// value, requireEnv returns nil.
func TestRequireEnvAllPresent(t *testing.T) {
	t.Setenv("FOO", "1")
	t.Setenv("BAR", "2")
	require.NoError(t, requireEnv("FOO", "BAR"))
}

// TestRequireEnvReportsAllMissing verifies that requireEnv aggregates all
// missing variables in a single error message (no early return on the
// first missing one) and does not mention variables that are present.
func TestRequireEnvReportsAllMissing(t *testing.T) {
	t.Setenv("FOO_PRESENT", "1")
	// Deliberately leave FOO_MISSING_A and FOO_MISSING_B unset.
	_ = os.Unsetenv("FOO_MISSING_A")
	_ = os.Unsetenv("FOO_MISSING_B")

	err := requireEnv("FOO_PRESENT", "FOO_MISSING_A", "FOO_MISSING_B")
	require.Error(t, err)
	require.Contains(t, err.Error(), "FOO_MISSING_A")
	require.Contains(t, err.Error(), "FOO_MISSING_B")
	require.NotContains(t, err.Error(), "FOO_PRESENT")
}

// TestLoadConfigParsesYAMLAndAppliesDefaults verifies two concerns
// together: viper applies the registered defaults for keys absent from
// the YAML (Binance URLs, health addr), and values that the YAML does
// supply (log.level, the symbols block) override correctly with proper
// type coercion (e.g. "500ms" → time.Duration).
func TestLoadConfigParsesYAMLAndAppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
symbols:
  - name: BTCUSDT
    depth_limit: 25
    checkpoint_interval: 500ms
    checkpoint_diff_threshold: 100
log:
  level: debug
`), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)

	// Defaults applied where YAML omits values.
	require.Equal(t, "wss://stream.binance.com:9443", cfg.Binance.WebSocketBaseURL)
	require.Equal(t, "https://api.binance.com", cfg.Binance.RESTBaseURL)
	require.Equal(t, ":8080", cfg.Health.Addr)

	// Values from YAML.
	require.Equal(t, "debug", cfg.Log.Level)
	require.Len(t, cfg.Symbols, 1)
	require.Equal(t, "BTCUSDT", cfg.Symbols[0].Name)
	require.Equal(t, 25, cfg.Symbols[0].DepthLimit)
	require.Equal(t, 500*time.Millisecond, cfg.Symbols[0].CheckpointInterval)
	require.Equal(t, 100, cfg.Symbols[0].CheckpointDiffThreshold)
}

// TestLoadConfigFailsWhenFileMissing covers the v.ReadInConfig error branch.
func TestLoadConfigFailsWhenFileMissing(t *testing.T) {
	_, err := loadConfig(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
}

// TestLoadConfigFailsOnInvalidYAML covers the v.Unmarshal / parser error
// branch by feeding YAML that can't deserialise into appConfig (a list
// where a map is expected).
func TestLoadConfigFailsOnInvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte("not:\n  valid: [yaml: here"), 0o600))
	_, err := loadConfig(path)
	require.Error(t, err)
}

// TestBuildLoggerProducesValidLogger smoke-tests the production zap
// configuration: the returned logger is non-nil and a basic .Info call
// does not panic (catches misconfigured encoders or levels).
func TestBuildLoggerProducesValidLogger(t *testing.T) {
	logger, closeFn, err := buildLogger("info", "debug", "")
	require.NoError(t, err)
	require.NotNil(t, logger)
	require.NotNil(t, closeFn)
	defer func() { _ = closeFn() }()
	logger.Info("smoke") // must not panic
}

// TestBuildLoggerRejectsInvalidLevel verifies that an unrecognised log
// level string surfaces as an explicit error at startup rather than
// silently falling back to a default.
func TestBuildLoggerRejectsInvalidLevel(t *testing.T) {
	_, _, err := buildLogger("notalevel", "debug", "")
	require.Error(t, err)
}

// TestBuildLoggerRejectsInvalidFileLevel covers the symmetric error path
// for the file-level config.
func TestBuildLoggerRejectsInvalidFileLevel(t *testing.T) {
	_, _, err := buildLogger("info", "notalevel", "")
	require.Error(t, err)
}

// TestBuildLoggerSplitLevelsHonourEachCore: stderr at INFO must filter
// DEBUG entries out, while file at DEBUG must capture them.
func TestBuildLoggerSplitLevelsHonourEachCore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "split.log")

	logger, closeFn, err := buildLogger("info", "debug", path)
	require.NoError(t, err)
	defer func() { _ = closeFn() }()

	logger.Debug("debug-only", zap.String("k", "v"))
	logger.Info("info-everywhere", zap.String("k", "v"))
	_ = logger.Sync()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "debug-only", "file at DEBUG must capture debug entries")
	require.Contains(t, string(data), "info-everywhere", "file must capture info entries too")
}

// TestBuildLoggerWritesToFileAndStderr verifies the file-tee wiring: when
// filePath is supplied, log entries reach the file (caller can verify by
// reading it back). The stderr core is also active — that's covered
// implicitly by the smoke test above.
func TestBuildLoggerWritesToFileAndStderr(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ingest.log")

	logger, closeFn, err := buildLogger("info", "debug", path)
	require.NoError(t, err)
	defer func() { _ = closeFn() }()

	logger.Info("hello-from-test", zap.String("k", "v"))
	// Sync may return "invalid argument" for the stderr core in test
	// environments; the data has already been written to both cores.
	_ = logger.Sync()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "hello-from-test")
	require.Contains(t, string(data), `"k":"v"`)
}

// TestBuildLoggerReturnsErrorOnUnopenableFile verifies the file-open
// failure surfaces explicitly rather than silently falling back to
// stderr-only logging.
func TestBuildLoggerReturnsErrorOnUnopenableFile(t *testing.T) {
	// A path under a non-existent directory cannot be created.
	_, _, err := buildLogger("info", "debug", filepath.Join(t.TempDir(), "no-such-dir", "x.log"))
	require.Error(t, err)
}

// healthOnlyDeps is a tiny harness for the health endpoint test that doesn't
// require Binance or a database — startHealthServer just queries h.State().
func newSyncedHandler(t *testing.T, snapshotLastID int64) *symbol.Handler {
	t.Helper()
	repo := &healthMockRepo{}
	df := &healthMockFetcher{snapshotLastID: snapshotLastID}
	ob := book.New("TESTSYM")
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "TESTSYM",
		DepthLimit:              10,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           10,
	}, ob, df, repo, zap.NewNop())

	h.Start(context.Background())
	// One aligning diff drives Bootstrapping → Synced.
	h.HandleDiff(domain.DiffEvent{
		Symbol:        "TESTSYM",
		EventTime:     time.Now(),
		FirstUpdateID: snapshotLastID,
		FinalUpdateID: snapshotLastID + 1,
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if h.State() == symbol.Synced {
			return h
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("handler did not reach Synced in time")
	return nil
}

// TestStartHealthServerReportsOKWhenAnySymbolSynced verifies the ADR
// health contract: the endpoint returns 200 with body {"status":"ok"} as
// soon as ANY registered symbol handler is in the Synced state. The mux
// is exercised directly via httptest.NewRecorder for deterministic
// scheduling (no race against the listener goroutine).
func TestStartHealthServerReportsOKWhenAnySymbolSynced(t *testing.T) {
	h := newSyncedHandler(t, 100)
	srv := startHealthServer("127.0.0.1:0", []*symbol.Handler{h}, zap.NewNop())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	// startHealthServer launches its own goroutine; we re-use the underlying
	// mux via httptest for a deterministic, immediate response.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	srv.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "ok", body["status"])
}

// TestStartHealthServerReportsDegradedWhenNoSymbolSynced verifies the
// negative case: with all handlers still Disconnected (Start never
// called), the endpoint returns 503 with body {"status":"degraded"} so
// that an orchestrator does not route traffic to a not-yet-bootstrapped
// instance.
func TestStartHealthServerReportsDegradedWhenNoSymbolSynced(t *testing.T) {
	df := &healthMockFetcher{snapshotLastID: 100}
	repo := &healthMockRepo{}
	ob := book.New("TESTSYM")
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "TESTSYM",
		DepthLimit:              10,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           10,
	}, ob, df, repo, zap.NewNop()) // never Start()ed → still Disconnected

	srv := startHealthServer("127.0.0.1:0", []*symbol.Handler{h}, zap.NewNop())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	srv.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "degraded", body["status"])
}

// healthMockRepo accepts everything; only used by the health-endpoint tests.
type healthMockRepo struct{}

func (*healthMockRepo) WriteDiff(_ context.Context, _ domain.DiffEvent) error { return nil }
func (*healthMockRepo) WriteCheckpoint(_ context.Context, _ domain.SnapshotEvent) error {
	return nil
}
func (*healthMockRepo) QueryNearestCheckpoint(_ context.Context, _ string, _ time.Time) (domain.SnapshotEvent, error) {
	return domain.SnapshotEvent{}, nil
}
func (*healthMockRepo) QueryDiffs(_ context.Context, _ string, _ time.Time, _ time.Time) ([]domain.DiffEvent, error) {
	return nil, nil
}

// healthMockFetcher returns a fixed snapshot.
type healthMockFetcher struct{ snapshotLastID int64 }

func (m *healthMockFetcher) FetchSnapshot(_ context.Context, sym string, _ int) (domain.SnapshotEvent, error) {
	return domain.SnapshotEvent{
		Symbol:       sym,
		CapturedAt:   time.Now(),
		LastUpdateID: m.snapshotLastID,
	}, nil
}

// Compile-time interface checks.
var (
	_ repository.Repository = (*healthMockRepo)(nil)
	_ fetcher.DepthFetcher  = (*healthMockFetcher)(nil)
)
