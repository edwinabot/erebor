// Package testutil provides shared test helpers for the erebor-backtest module.
package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	ingestdomain "github.com/edwinabot/erebor/ingest/domain"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

const (
	testBidPrice = "50000.00"
	testAskPrice = "50001.00"
)

// NewMiniredis starts an in-process Redis server and returns a connected client.
// Both are cleaned up via t.Cleanup.
func NewMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// RealRedisClient returns a client connected to EREBOR_TEST_REDIS_ADDR (DB 1),
// or skips the test if the variable is unset.
func RealRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("EREBOR_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("EREBOR_TEST_REDIS_ADDR not set; skipping real-Redis test")
	}
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("EREBOR_TEST_REDIS_PASSWORD"),
		DB:       1,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// UniqueNamespace returns a collision-free test namespace.
func UniqueNamespace(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("erebor:test:%d", time.Now().UnixNano())
}

// MakeSnapshot creates a SnapshotEvent with predictable test data.
func MakeSnapshot(symbol string, lastUpdateID int64, capturedAt time.Time) ingestdomain.SnapshotEvent {
	return ingestdomain.SnapshotEvent{
		Symbol:       symbol,
		CapturedAt:   capturedAt,
		LastUpdateID: lastUpdateID,
		Bids: []ingestdomain.PriceLevel{
			{Price: decimal.RequireFromString(testBidPrice), Quantity: decimal.RequireFromString("1.5")},
			{Price: decimal.RequireFromString("49999.00"), Quantity: decimal.RequireFromString("2.0")},
		},
		Asks: []ingestdomain.PriceLevel{
			{Price: decimal.RequireFromString(testAskPrice), Quantity: decimal.RequireFromString("1.0")},
			{Price: decimal.RequireFromString("50002.00"), Quantity: decimal.RequireFromString("0.5")},
		},
	}
}

// MakeDiff creates a DiffEvent that applies cleanly after prevFinalUpdateID.
func MakeDiff(symbol string, prevFinalUpdateID int64, eventTime time.Time) ingestdomain.DiffEvent {
	return ingestdomain.DiffEvent{
		Symbol:        symbol,
		EventTime:     eventTime,
		FirstUpdateID: prevFinalUpdateID + 1,
		FinalUpdateID: prevFinalUpdateID + 1,
		Bids: []ingestdomain.PriceLevel{
			{Price: decimal.RequireFromString(testBidPrice), Quantity: decimal.RequireFromString("1.6")},
		},
		Asks: []ingestdomain.PriceLevel{
			{Price: decimal.RequireFromString(testAskPrice), Quantity: decimal.RequireFromString("0.9")},
		},
	}
}

// MakeDiffSeq returns n sequential diffs starting from firstUpdateID.
func MakeDiffSeq(symbol string, firstUpdateID int64, baseTime time.Time, n int) []ingestdomain.DiffEvent {
	diffs := make([]ingestdomain.DiffEvent, n)
	for i := range diffs {
		id := firstUpdateID + int64(i)
		diffs[i] = ingestdomain.DiffEvent{
			Symbol:        symbol,
			EventTime:     baseTime.Add(time.Duration(i) * time.Second),
			FirstUpdateID: id,
			FinalUpdateID: id,
			Bids: []ingestdomain.PriceLevel{
				{Price: decimal.RequireFromString(testBidPrice), Quantity: decimal.RequireFromString("1.0")},
			},
			Asks: []ingestdomain.PriceLevel{
				{Price: decimal.RequireFromString(testAskPrice), Quantity: decimal.RequireFromString("1.0")},
			},
		}
	}
	return diffs
}

// ReadAllStream reads every message from a stream key using XRANGE.
func ReadAllStream(t *testing.T, client *redis.Client, streamKey string) []redis.XMessage {
	t.Helper()
	msgs, err := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, err)
	return msgs
}
