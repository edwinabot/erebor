package publisher_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/internal/testutil"
	"github.com/edwinabot/erebor/backtest/publisher"
	ingestdomain "github.com/edwinabot/erebor/ingest/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func nopLogger() *zap.Logger { return zap.NewNop() }

// ── L2Publisher ───────────────────────────────────────────────────────────────

func TestL2Publisher_PublishesCorrectWireFormat(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	pub := publisher.NewL2Publisher(client, ns, nopLogger())

	eventTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	bids := []ingestdomain.PriceLevel{
		{Price: decimal.RequireFromString("50000.00"), Quantity: decimal.RequireFromString("1.5")},
	}
	asks := []ingestdomain.PriceLevel{
		{Price: decimal.RequireFromString("50001.00"), Quantity: decimal.RequireFromString("1.0")},
	}

	err := pub.Publish(context.Background(), "run-001", "BTCUSDT", eventTime, 12345, bids, asks)
	require.NoError(t, err)

	msgs := testutil.ReadAllStream(t, client, ns+":l2:BTCUSDT")
	require.Len(t, msgs, 1)

	vals := msgs[0].Values
	assert.Equal(t, "run-001", vals["run_id"])
	assert.Equal(t, "BTCUSDT", vals["symbol"])
	assert.Equal(t, eventTime.UTC().Format(time.RFC3339Nano), vals["event_time"])
	assert.Equal(t, "12345", vals["last_update_id"])

	// bids/asks are JSON [][2]string pairs.
	var bidsDecoded [][2]string
	require.NoError(t, json.Unmarshal([]byte(vals["bids"].(string)), &bidsDecoded))
	require.Len(t, bidsDecoded, 1)
	assert.Equal(t, "50000", bidsDecoded[0][0])
	assert.Equal(t, "1.5", bidsDecoded[0][1])
}

func TestL2Publisher_SymbolIsUppercased(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	pub := publisher.NewL2Publisher(client, ns, nopLogger())
	err := pub.Publish(context.Background(), "run-001", "btcusdt", time.Now(), 1, nil, nil)
	require.NoError(t, err)

	msgs := testutil.ReadAllStream(t, client, ns+":l2:BTCUSDT")
	require.Len(t, msgs, 1, "stream key must use uppercase symbol")
}

func TestL2Publisher_EmptyLevelsPublishEmptyArrays(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	pub := publisher.NewL2Publisher(client, ns, nopLogger())
	err := pub.Publish(context.Background(), "", "ETHUSDT", time.Now(), 0, nil, nil)
	require.NoError(t, err)

	msgs := testutil.ReadAllStream(t, client, ns+":l2:ETHUSDT")
	require.Len(t, msgs, 1)

	var bids [][2]string
	require.NoError(t, json.Unmarshal([]byte(msgs[0].Values["bids"].(string)), &bids))
	assert.Empty(t, bids)
}

func TestL2Publisher_MultiplePublishesGoToSameStream(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	pub := publisher.NewL2Publisher(client, ns, nopLogger())

	for i := range 5 {
		err := pub.Publish(context.Background(), "run-x", "BTCUSDT", time.Now().Add(time.Duration(i)*time.Second), int64(i), nil, nil)
		require.NoError(t, err)
	}

	msgs := testutil.ReadAllStream(t, client, ns+":l2:BTCUSDT")
	assert.Len(t, msgs, 5)
}

func TestL2Publisher_LiveRunIDIsEmptyString(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	pub := publisher.NewL2Publisher(client, ns, nopLogger())

	// empty run_id = live event
	err := pub.Publish(context.Background(), "", "BTCUSDT", time.Now(), 1, nil, nil)
	require.NoError(t, err)

	msgs := testutil.ReadAllStream(t, client, ns+":l2:BTCUSDT")
	require.Len(t, msgs, 1)
	assert.Equal(t, "", msgs[0].Values["run_id"], "live event must have empty run_id")
}

// ── ControlPublisher ──────────────────────────────────────────────────────────

func TestControlPublisher_PublishesReplayStart(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	pub := publisher.NewControlPublisher(client, ns, nopLogger())
	err := pub.Publish(context.Background(), domain.ControlEvent{
		RunID:   "run-abc",
		Type:    domain.ControlReplayStart,
		Payload: map[string]string{"symbols": "BTCUSDT,ETHUSDT"},
	})
	require.NoError(t, err)

	msgs := testutil.ReadAllStream(t, client, ns+":control")
	require.Len(t, msgs, 1)
	assert.Equal(t, "run-abc", msgs[0].Values["run_id"])
	assert.Equal(t, string(domain.ControlReplayStart), msgs[0].Values["type"])

	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &payload))
	assert.Equal(t, "BTCUSDT,ETHUSDT", payload["symbols"])
}

func TestControlPublisher_AllEventTypes(t *testing.T) {
	types := []domain.ControlEventType{
		domain.ControlReplayStart,
		domain.ControlReplayComplete,
		domain.ControlDataGap,
		domain.ControlCancelled,
	}

	for _, evType := range types {
		evType := evType
		t.Run(string(evType), func(t *testing.T) {
			_, client := testutil.NewMiniredis(t)
			ns := testutil.UniqueNamespace(t)
			pub := publisher.NewControlPublisher(client, ns, nopLogger())

			err := pub.Publish(context.Background(), domain.ControlEvent{
				RunID:   "run-test",
				Type:    evType,
				Payload: map[string]string{},
			})
			require.NoError(t, err)

			msgs := testutil.ReadAllStream(t, client, ns+":control")
			require.Len(t, msgs, 1)
			assert.Equal(t, string(evType), msgs[0].Values["type"])
		})
	}
}

func TestControlPublisher_NilPayloadSerialises(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	pub := publisher.NewControlPublisher(client, ns, nopLogger())

	err := pub.Publish(context.Background(), domain.ControlEvent{
		RunID:   "r",
		Type:    domain.ControlReplayComplete,
		Payload: nil,
	})
	require.NoError(t, err)

	msgs := testutil.ReadAllStream(t, client, ns+":control")
	require.Len(t, msgs, 1)
	// nil map marshals to "null" — must not crash the publisher.
	assert.NotEmpty(t, msgs[0].Values["payload"])
}
