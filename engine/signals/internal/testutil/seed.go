// Package testutil provides seed data and helpers for erebor-signals tests.
package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// SeedEntry is a single L2 book snapshot in the testdata JSON format.
// Bids are sorted descending (best bid first); Asks ascending (best ask first).
type SeedEntry struct {
	RunID        string      `json:"run_id"`
	Symbol       string      `json:"symbol"`
	EventTime    time.Time   `json:"event_time"`
	LastUpdateID int64       `json:"last_update_id"`
	Bids         [][2]string `json:"bids"` // [price, qty] string pairs
	Asks         [][2]string `json:"asks"`
}

// ToRedisValues converts a SeedEntry to the map[string]any wire format
// written to Redis Streams by publisher.Publisher and read by consumer.Consumer.
func ToRedisValues(e SeedEntry) map[string]any {
	bidsJSON, _ := json.Marshal(e.Bids)
	asksJSON, _ := json.Marshal(e.Asks)
	return map[string]any{
		"run_id":         e.RunID,
		"symbol":         e.Symbol,
		"event_time":     e.EventTime.UTC().Format(time.RFC3339Nano),
		"last_update_id": fmt.Sprintf("%d", e.LastUpdateID),
		"bids":           string(bidsJSON),
		"asks":           string(asksJSON),
	}
}

// LoadBtcusdtEvents reads the canonical BTCUSDT seed file from testdata/.
// Each call returns a fresh copy; mutating the slice does not affect subsequent calls.
func LoadBtcusdtEvents(t *testing.T) []SeedEntry {
	t.Helper()
	data, err := os.ReadFile(testdataPath("btcusdt_l2_events.json"))
	require.NoError(t, err, "read btcusdt_l2_events.json")
	var entries []SeedEntry
	require.NoError(t, json.Unmarshal(data, &entries), "parse btcusdt_l2_events.json")
	return entries
}

// SeedStream writes entries to a Redis stream in the L2BookUpdateEvent wire format.
// It calls t.Fatal if any XADD fails.
func SeedStream(ctx context.Context, t *testing.T, client *redis.Client, streamKey string, entries []SeedEntry) {
	t.Helper()
	for i, e := range entries {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: ToRedisValues(e),
		}).Err()
		require.NoError(t, err, "seed event %d into stream %s", i, streamKey)
	}
}

// UniqueNamespace returns a stream namespace that is unique per test, preventing
// key collisions when tests run concurrently or in parallel.
// Format: erebor:test:{unix_nano}
func UniqueNamespace(t *testing.T) string {
	return fmt.Sprintf("erebor:test:%s:%d", sanitize(t.Name()), time.Now().UnixNano())
}

// NewMiniredis starts an in-process Redis server and returns a connected client.
// Both the server and client are cleaned up via t.Cleanup.
func NewMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// RealRedisClient returns a client connected to the Redis instance at
// EREBOR_TEST_REDIS_ADDR, or skips the test if the variable is not set.
// Each call selects DB 1 (reserved for tests) and uses a unique key prefix
// to prevent collisions across concurrent test runs.
func RealRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("EREBOR_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("EREBOR_TEST_REDIS_ADDR not set; skipping real-Redis integration test")
	}
	password := os.Getenv("EREBOR_TEST_REDIS_PASSWORD")
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       1, // DB 1 is reserved for tests; never collides with live data on DB 0
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// testdataPath resolves the path to a file in the module's testdata/ directory,
// regardless of which package's test is calling this function.
func testdataPath(name string) string {
	_, thisFile, _, _ := runtime.Caller(0)
	// thisFile = .../engine/signals/internal/testutil/seed.go
	// testdata  = .../engine/signals/testdata/
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "testdata")
	return filepath.Join(root, name)
}

// sanitize replaces characters that are invalid in Redis key names.
func sanitize(s string) string {
	out := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if c == '/' || c == ' ' {
			out[i] = '_'
		} else {
			out[i] = c
		}
	}
	return string(out)
}
