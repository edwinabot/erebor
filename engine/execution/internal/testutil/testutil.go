// Package testutil provides shared test helpers for the erebor-execution module.
package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	signalsdomain "github.com/edwinabot/erebor/signals/domain"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
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

// SeedSignal appends one SignalEvent to streamKey and returns the stream message ID.
func SeedSignal(t *testing.T, client *redis.Client, streamKey string, sig signalsdomain.SignalEvent) string {
	t.Helper()
	params, _ := json.Marshal(sig.Params)
	id, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"run_id":     sig.RunID,
			"symbol":     sig.Symbol,
			"event_time": sig.EventTime.UTC().Format("2006-01-02T15:04:05.999999999Z"),
			"name":       sig.Name,
			"version":    sig.Version,
			"value":      sig.Value.String(),
			"params":     string(params),
		},
	}).Result()
	require.NoError(t, err)
	return id
}

// MakeSignal returns a SignalEvent with predictable test values.
func MakeSignal(symbol, name, value string) signalsdomain.SignalEvent {
	return signalsdomain.SignalEvent{
		Symbol:    symbol,
		Name:      name,
		Value:     decimal.RequireFromString(value),
		EventTime: time.Now().UTC(),
		Version:   "1",
	}
}

// ReadAllStream reads every message from streamKey using XRANGE.
func ReadAllStream(t *testing.T, client *redis.Client, streamKey string) []redis.XMessage {
	t.Helper()
	msgs, err := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, err)
	return msgs
}
