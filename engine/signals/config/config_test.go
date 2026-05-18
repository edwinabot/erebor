package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/edwinabot/erebor/signals/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestLoad_Defaults(t *testing.T) {
	path := writeConfig(t, "symbols:\n  - BTCUSDT\n")
	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, []string{"BTCUSDT"}, cfg.Symbols)
	assert.Equal(t, "erebor:live", cfg.StreamNamespace)
	assert.Equal(t, 10, cfg.SignalDepth)
	assert.Equal(t, "localhost:6379", cfg.Redis.Addr)
	assert.Equal(t, "", cfg.Redis.Password)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "debug", cfg.Log.FileLevel)
	assert.Equal(t, ":8080", cfg.Health.Addr)
}

func TestLoad_FullConfig(t *testing.T) {
	yaml := `
symbols:
  - BTCUSDT
  - ETHUSDT
  - SOLUSDT
stream_namespace: "erebor:backtest:run-abc"
signal_depth: 5
redis:
  addr: "redis-prod:6379"
  password: "secret"
log:
  level: "debug"
  file_level: "debug"
  file_path: "/tmp/signals.log"
health:
  addr: ":9090"
`
	path := writeConfig(t, yaml)
	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}, cfg.Symbols)
	assert.Equal(t, "erebor:backtest:run-abc", cfg.StreamNamespace)
	assert.Equal(t, 5, cfg.SignalDepth)
	assert.Equal(t, "redis-prod:6379", cfg.Redis.Addr)
	assert.Equal(t, "secret", cfg.Redis.Password)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "debug", cfg.Log.FileLevel)
	assert.Equal(t, "/tmp/signals.log", cfg.Log.FilePath)
	assert.Equal(t, ":9090", cfg.Health.Addr)
}

func TestLoad_EnvOverrides(t *testing.T) {
	path := writeConfig(t, `
symbols:
  - BTCUSDT
redis:
  addr: "config-redis:6379"
  password: "config-pass"
stream_namespace: "erebor:live"
`)

	t.Setenv("REDIS_ADDR", "env-redis:6380")
	t.Setenv("REDIS_PASSWORD", "env-pass")
	t.Setenv("STREAM_NAMESPACE", "erebor:backtest:env-run")

	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, "env-redis:6380", cfg.Redis.Addr, "REDIS_ADDR env should override config")
	assert.Equal(t, "env-pass", cfg.Redis.Password, "REDIS_PASSWORD env should override config")
	assert.Equal(t, "erebor:backtest:env-run", cfg.StreamNamespace, "STREAM_NAMESPACE env should override config")
}

func TestLoad_EnvDoesNotOverrideWhenUnset(t *testing.T) {
	// Explicitly unset env vars in case a parent test set them.
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("STREAM_NAMESPACE", "")

	path := writeConfig(t, `
symbols:
  - BTCUSDT
redis:
  addr: "config-redis:6379"
  password: "config-pass"
stream_namespace: "erebor:live"
`)
	cfg, err := config.Load(path)
	require.NoError(t, err)

	// When env is empty, defaults/config should win.
	assert.Equal(t, "config-redis:6379", cfg.Redis.Addr)
}

func TestLoad_MultipleSymbols(t *testing.T) {
	path := writeConfig(t, "symbols:\n  - BTCUSDT\n  - ETHUSDT\n  - BNBUSDT\n  - SOLUSDT\n")
	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Len(t, cfg.Symbols, 4)
}

func TestLoad_MissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	_, err := config.Load(missing)
	require.Error(t, err)
}

func TestLoad_MalformedYAML(t *testing.T) {
	path := writeConfig(t, "symbols: [unclosed")
	_, err := config.Load(path)
	require.Error(t, err)
}

func TestLoad_SignalDepthDefault(t *testing.T) {
	// signal_depth not set → default 10
	path := writeConfig(t, "symbols:\n  - BTCUSDT\n")
	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, 10, cfg.SignalDepth)
}

func TestLoad_SignalDepthOverride(t *testing.T) {
	path := writeConfig(t, "symbols:\n  - BTCUSDT\nsignal_depth: 20\n")
	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, 20, cfg.SignalDepth)
}
