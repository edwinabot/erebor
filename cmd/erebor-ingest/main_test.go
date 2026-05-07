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

func TestRequireEnvAllPresent(t *testing.T) {
	t.Setenv("FOO", "1")
	t.Setenv("BAR", "2")
	require.NoError(t, requireEnv("FOO", "BAR"))
}

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

func TestBuildLoggerProducesValidLogger(t *testing.T) {
	logger, err := buildLogger("info")
	require.NoError(t, err)
	require.NotNil(t, logger)
	logger.Info("smoke") // must not panic
}

func TestBuildLoggerRejectsInvalidLevel(t *testing.T) {
	_, err := buildLogger("notalevel")
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
