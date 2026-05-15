package consumer

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── decodeL2BookUpdateEvent ────────────────────────────────────────────────────

func TestDecodeL2BookUpdateEvent_Valid(t *testing.T) {
	ts := "2026-05-15T10:00:00.000000000Z"
	values := map[string]any{
		"run_id":         "run-abc",
		"symbol":         "BTCUSDT",
		"event_time":     ts,
		"last_update_id": "4872910001",
		"bids":           `[["94500.00","2.500"],["94499.50","1.200"]]`,
		"asks":           `[["94501.00","0.800"],["94502.50","2.100"]]`,
	}

	ev, err := decodeL2BookUpdateEvent(values)
	require.NoError(t, err)

	assert.Equal(t, "run-abc", ev.RunID)
	assert.Equal(t, "BTCUSDT", ev.Symbol)
	assert.Equal(t, int64(4872910001), ev.LastUpdateID)

	expectedTime, _ := time.Parse(time.RFC3339Nano, ts)
	assert.Equal(t, expectedTime, ev.EventTime)

	require.Len(t, ev.Bids, 2)
	assert.True(t, ev.Bids[0].Price.Equal(decimal.RequireFromString("94500.00")))
	assert.True(t, ev.Bids[0].Quantity.Equal(decimal.RequireFromString("2.500")))
	assert.True(t, ev.Bids[1].Price.Equal(decimal.RequireFromString("94499.50")))

	require.Len(t, ev.Asks, 2)
	assert.True(t, ev.Asks[0].Price.Equal(decimal.RequireFromString("94501.00")))
	assert.True(t, ev.Asks[0].Quantity.Equal(decimal.RequireFromString("0.800")))
}

func TestDecodeL2BookUpdateEvent_EmptyRunID(t *testing.T) {
	values := map[string]any{
		"run_id":     "",
		"symbol":     "ETHUSDT",
		"event_time": "2026-05-15T10:00:00Z",
		"bids":       `[]`,
		"asks":       `[]`,
	}
	ev, err := decodeL2BookUpdateEvent(values)
	require.NoError(t, err)
	assert.Equal(t, "", ev.RunID)
	assert.Equal(t, "ETHUSDT", ev.Symbol)
}

func TestDecodeL2BookUpdateEvent_MissingSymbol(t *testing.T) {
	values := map[string]any{
		"event_time": "2026-05-15T10:00:00Z",
		"bids":       `[]`,
		"asks":       `[]`,
	}
	_, err := decodeL2BookUpdateEvent(values)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing symbol")
}

func TestDecodeL2BookUpdateEvent_MissingEventTime(t *testing.T) {
	values := map[string]any{
		"symbol": "BTCUSDT",
		"bids":   `[]`,
		"asks":   `[]`,
	}
	_, err := decodeL2BookUpdateEvent(values)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing event_time")
}

func TestDecodeL2BookUpdateEvent_InvalidEventTime(t *testing.T) {
	values := map[string]any{
		"symbol":     "BTCUSDT",
		"event_time": "not-a-timestamp",
		"bids":       `[]`,
		"asks":       `[]`,
	}
	_, err := decodeL2BookUpdateEvent(values)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse event_time")
}

func TestDecodeL2BookUpdateEvent_InvalidBidsJSON(t *testing.T) {
	values := map[string]any{
		"symbol":     "BTCUSDT",
		"event_time": "2026-05-15T10:00:00Z",
		"bids":       `{not valid json`,
		"asks":       `[]`,
	}
	_, err := decodeL2BookUpdateEvent(values)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode bids")
}

func TestDecodeL2BookUpdateEvent_InvalidAsksJSON(t *testing.T) {
	values := map[string]any{
		"symbol":     "BTCUSDT",
		"event_time": "2026-05-15T10:00:00Z",
		"bids":       `[]`,
		"asks":       `{not valid json`,
	}
	_, err := decodeL2BookUpdateEvent(values)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode asks")
}

func TestDecodeL2BookUpdateEvent_MissingLastUpdateID(t *testing.T) {
	// last_update_id is optional; missing = 0
	values := map[string]any{
		"symbol":     "BTCUSDT",
		"event_time": "2026-05-15T10:00:00Z",
		"bids":       `[]`,
		"asks":       `[]`,
	}
	ev, err := decodeL2BookUpdateEvent(values)
	require.NoError(t, err)
	assert.Equal(t, int64(0), ev.LastUpdateID)
}

func TestDecodeL2BookUpdateEvent_EmptyBidsAndAsks(t *testing.T) {
	values := map[string]any{
		"symbol":     "BTCUSDT",
		"event_time": "2026-05-15T10:00:00Z",
		"bids":       ``,
		"asks":       ``,
	}
	ev, err := decodeL2BookUpdateEvent(values)
	require.NoError(t, err)
	assert.Empty(t, ev.Bids)
	assert.Empty(t, ev.Asks)
}

func TestDecodeL2BookUpdateEvent_HighPrecisionDecimal(t *testing.T) {
	values := map[string]any{
		"symbol":     "XRPUSDT",
		"event_time": "2026-05-15T10:00:00Z",
		"bids":       `[["0.00001234","10000.000000001"]]`,
		"asks":       `[["0.00001235","9999.999999999"]]`,
	}
	ev, err := decodeL2BookUpdateEvent(values)
	require.NoError(t, err)
	assert.True(t, ev.Bids[0].Price.Equal(decimal.RequireFromString("0.00001234")))
	assert.True(t, ev.Bids[0].Quantity.Equal(decimal.RequireFromString("10000.000000001")))
}

// ── decodePriceLevels ─────────────────────────────────────────────────────────

func TestDecodePriceLevels_Valid(t *testing.T) {
	levels, err := decodePriceLevels(`[["100.50","1.500"],["100.00","2.200"]]`)
	require.NoError(t, err)
	require.Len(t, levels, 2)
	assert.True(t, levels[0].Price.Equal(decimal.RequireFromString("100.50")))
	assert.True(t, levels[0].Quantity.Equal(decimal.RequireFromString("1.500")))
	assert.True(t, levels[1].Price.Equal(decimal.RequireFromString("100.00")))
}

func TestDecodePriceLevels_Empty(t *testing.T) {
	levels, err := decodePriceLevels(`[]`)
	require.NoError(t, err)
	assert.Empty(t, levels)
}

func TestDecodePriceLevels_EmptyString(t *testing.T) {
	levels, err := decodePriceLevels(``)
	require.NoError(t, err)
	assert.Nil(t, levels)
}

func TestDecodePriceLevels_MalformedJSON(t *testing.T) {
	_, err := decodePriceLevels(`{bad`)
	require.Error(t, err)
}

func TestDecodePriceLevels_InvalidPrice(t *testing.T) {
	_, err := decodePriceLevels(`[["not-a-number","1.0"]]`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse price")
}

func TestDecodePriceLevels_InvalidQuantity(t *testing.T) {
	_, err := decodePriceLevels(`[["100.00","not-a-number"]]`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse qty")
}

func TestDecodePriceLevels_SingleLevel(t *testing.T) {
	levels, err := decodePriceLevels(`[["94500.00","2.500"]]`)
	require.NoError(t, err)
	require.Len(t, levels, 1)
	assert.True(t, levels[0].Price.Equal(decimal.RequireFromString("94500.00")))
}

func TestDecodePriceLevels_TenLevels(t *testing.T) {
	pairs := make([][2]string, 10)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("%d.00", 100+i), "1.000"}
	}
	raw := `[`
	for i, p := range pairs {
		if i > 0 {
			raw += ","
		}
		raw += fmt.Sprintf(`["%s","%s"]`, p[0], p[1])
	}
	raw += `]`

	levels, err := decodePriceLevels(raw)
	require.NoError(t, err)
	assert.Len(t, levels, 10)
}

// ── isAlreadyExists ───────────────────────────────────────────────────────────

func TestIsAlreadyExists_BusygroupError(t *testing.T) {
	err := errors.New("BUSYGROUP Consumer Group name already exists")
	assert.True(t, isAlreadyExists(err))
}

func TestIsAlreadyExists_OtherError(t *testing.T) {
	err := errors.New("ERR no such key")
	assert.False(t, isAlreadyExists(err))
}

func TestIsAlreadyExists_Nil(t *testing.T) {
	assert.False(t, isAlreadyExists(nil))
}

func TestIsAlreadyExists_PartialMatch(t *testing.T) {
	err := errors.New("connection refused")
	assert.False(t, isAlreadyExists(err))
}

// ── inputKey ──────────────────────────────────────────────────────────────────

func TestConsumer_InputKey(t *testing.T) {
	tests := []struct {
		namespace string
		symbol    string
		want      string
	}{
		{"erebor:live", "BTCUSDT", "erebor:live:l2:BTCUSDT"},
		{"erebor:live", "btcusdt", "erebor:live:l2:BTCUSDT"}, // lowercased symbol normalized
		{"erebor:backtest:run-123", "ETHUSDT", "erebor:backtest:run-123:l2:ETHUSDT"},
		{"erebor:test:abc", "SOLUSDT", "erebor:test:abc:l2:SOLUSDT"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			c := &Consumer{namespace: tt.namespace}
			assert.Equal(t, tt.want, c.inputKey(tt.symbol))
		})
	}
}
