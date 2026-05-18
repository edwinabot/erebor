package order_test

import (
	"context"
	"testing"
	"time"

	backtestdomain "github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/execution/internal/testutil"
	"github.com/edwinabot/erebor/execution/order"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func makeOrder(sessionID string) backtestdomain.OrderEvent {
	return backtestdomain.OrderEvent{
		RunID:      sessionID,
		Symbol:     "BTCUSDT",
		EventTime:  time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
		OrderID:    "order-abc",
		Side:       backtestdomain.SideBuy,
		Type:       backtestdomain.OrderTypeMarket,
		Price:      decimal.Zero,
		Quantity:   decimal.RequireFromString("0.001"),
		Status:     backtestdomain.OrderStatusFilled,
		FillPrice:  decimal.RequireFromString("50001"),
		FillQty:    decimal.RequireFromString("0.001"),
		Fee:        decimal.RequireFromString("0.05"),
		SignalName: "book_imbalance",
	}
}

func TestOrderPublisherWritesAllFields(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	pub := order.NewPublisher(client, ns, zap.NewNop())

	err := pub.Publish(context.Background(), makeOrder("sess-123"))
	require.NoError(t, err)

	msgs := testutil.ReadAllStream(t, client, ns+":orders")
	require.Len(t, msgs, 1)
	v := msgs[0].Values
	assert.Equal(t, "sess-123", v["run_id"])
	assert.Equal(t, "BTCUSDT", v["symbol"])
	assert.Equal(t, "order-abc", v["order_id"])
	assert.Equal(t, "Buy", v["side"])
	assert.Equal(t, "Market", v["type"])
	assert.Equal(t, "Filled", v["status"])
	assert.Equal(t, "50001", v["fill_price"])
	assert.Equal(t, "0.001", v["fill_qty"])
	assert.Equal(t, "0.05", v["fee"])
	assert.Equal(t, "book_imbalance", v["signal_name"])
}

func TestOrderPublisherUsesCorrectStreamKey(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := "erebor:live"
	pub := order.NewPublisher(client, ns, zap.NewNop())

	require.NoError(t, pub.Publish(context.Background(), makeOrder("s1")))

	msgs := testutil.ReadAllStream(t, client, "erebor:live:orders")
	assert.Len(t, msgs, 1)
}

func TestOrderPublisherMultipleOrders(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	pub := order.NewPublisher(client, ns, zap.NewNop())

	for i := 0; i < 3; i++ {
		require.NoError(t, pub.Publish(context.Background(), makeOrder("sess")))
	}

	msgs := testutil.ReadAllStream(t, client, ns+":orders")
	assert.Len(t, msgs, 3)
}
