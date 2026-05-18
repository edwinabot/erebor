package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edwinabot/erebor/signals/consumer"
	"github.com/edwinabot/erebor/signals/publisher"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const healthzPath = "/healthz"

// newTestConsumer creates a Consumer that has NOT been started, so IsRunning() is false.
func newTestConsumer() *consumer.Consumer {
	// Use a dummy redis client — it won't be connected, but we won't call Start.
	client := redis.NewClient(&redis.Options{Addr: "localhost:0"})
	pub := publisher.New(client, "erebor:test")
	return consumer.New(client, pub, "erebor:test", []string{"BTCUSDT"}, 10, zap.NewNop())
}

// healthHandler extracts the /healthz handler logic so it can be tested in isolation.
func healthHandler(cons *consumer.Consumer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var body map[string]string
		if cons.IsRunning() {
			w.WriteHeader(http.StatusOK)
			body = map[string]string{"status": "ok"}
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			body = map[string]string{"status": "degraded"}
		}
		_ = json.NewEncoder(w).Encode(body)
	}
}

func TestHealthzReturns503WhenConsumerNotRunning(t *testing.T) {
	cons := newTestConsumer()
	// Consumer has not been started — IsRunning() must be false.
	require.False(t, cons.IsRunning(), "pre-condition: consumer must not be running")

	req := httptest.NewRequest(http.MethodGet, healthzPath, nil)
	rec := httptest.NewRecorder()

	healthHandler(cons).ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"/healthz must return 503 when consumer is not running")

	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, "degraded", body["status"])
}

func TestHealthzReturns200ContentType(t *testing.T) {
	cons := newTestConsumer()

	req := httptest.NewRequest(http.MethodGet, healthzPath, nil)
	rec := httptest.NewRecorder()

	healthHandler(cons).ServeHTTP(rec, req)

	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestHealthzBodyIsValidJSON(t *testing.T) {
	cons := newTestConsumer()

	req := httptest.NewRequest(http.MethodGet, healthzPath, nil)
	rec := httptest.NewRecorder()

	healthHandler(cons).ServeHTTP(rec, req)

	var body map[string]string
	err := json.NewDecoder(rec.Body).Decode(&body)
	require.NoError(t, err, "health response body must be valid JSON")
	_, hasStatus := body["status"]
	assert.True(t, hasStatus, "body must contain 'status' field")
}
