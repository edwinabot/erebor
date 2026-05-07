package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func levels(pairs ...[2]string) []domain.PriceLevel {
	out := make([]domain.PriceLevel, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, domain.PriceLevel{
			Price:    decimal.RequireFromString(p[0]),
			Quantity: decimal.RequireFromString(p[1]),
		})
	}
	return out
}

const (
	priceLevelA = "100.50"
	priceLevelB = "100.40"
	priceLevelC = "100.60"
)

func TestLevelsJSONRoundTrip(t *testing.T) {
	in := levels(
		[2]string{priceLevelA, "1.5"},
		[2]string{priceLevelB, "2.0"},
		[2]string{"0.00001234", "10000"},
	)

	data, err := levelsToJSON(in)
	require.NoError(t, err)

	out, err := jsonToLevels(data)
	require.NoError(t, err)

	require.Len(t, out, len(in))
	for i := range in {
		require.True(t, in[i].Price.Equal(out[i].Price), "price mismatch idx=%d in=%s out=%s", i, in[i].Price, out[i].Price)
		require.True(t, in[i].Quantity.Equal(out[i].Quantity), "qty mismatch idx=%d", i)
	}
}

// TestJSONToLevelsRejectsInvalidPrice covers the price decimal-parse error
// branch. Such corruption can only arise from manual SQL surgery — but the
// guard exists.
func TestJSONToLevelsRejectsInvalidPrice(t *testing.T) {
	_, err := jsonToLevels([]byte(`[["abc", "1.0"]]`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse price")
}

// TestJSONToLevelsRejectsInvalidQuantity covers the quantity-parse branch.
func TestJSONToLevelsRejectsInvalidQuantity(t *testing.T) {
	_, err := jsonToLevels([]byte(`[["100.0", "xyz"]]`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse qty")
}

// TestJSONToLevelsRejectsMalformedJSON covers the json.Unmarshal error
// branch.
func TestJSONToLevelsRejectsMalformedJSON(t *testing.T) {
	_, err := jsonToLevels([]byte(`{not valid json`))
	require.Error(t, err)
}

func TestLevelsJSONHandlesEmptyInput(t *testing.T) {
	out, err := jsonToLevels(nil)
	require.NoError(t, err)
	require.Nil(t, out)

	data, err := levelsToJSON(nil)
	require.NoError(t, err)
	require.Equal(t, "[]", string(data))
}

// TestRepositoryRoundTripIntegration exercises WriteDiff, WriteCheckpoint,
// QueryNearestCheckpoint and QueryDiffs against a real TimescaleDB. It is
// skipped unless EREBOR_TEST_DSN is set in the environment.
//
// Bring up the database with `make db-up && make migrate`, then:
//
//	EREBOR_TEST_DSN="postgres://erebor:erebor_dev@localhost:5432/erebor?sslmode=disable" \
//	    go test -race ./repository/...
func TestRepositoryRoundTripIntegration(t *testing.T) {
	dsn := os.Getenv("EREBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("EREBOR_TEST_DSN not set; skipping repository integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	const sym = "TESTSYM"
	_, err = pool.Exec(ctx, "DELETE FROM order_book_diffs WHERE symbol=$1", sym)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "DELETE FROM order_book_snapshots WHERE symbol=$1", sym)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM order_book_diffs WHERE symbol=$1", sym)
		_, _ = pool.Exec(ctx, "DELETE FROM order_book_snapshots WHERE symbol=$1", sym)
	})

	repo := New(pool)
	base := time.Now().UTC().Truncate(time.Microsecond)

	diff := domain.DiffEvent{
		Symbol:        sym,
		EventTime:     base,
		FirstUpdateID: 100,
		FinalUpdateID: 110,
		Bids:          levels([2]string{priceLevelA, "1.5"}, [2]string{priceLevelB, "2.0"}),
		Asks:          levels([2]string{priceLevelC, "0.7"}),
	}
	require.NoError(t, repo.WriteDiff(ctx, diff))

	// Idempotent: repeat insert is silently absorbed by ON CONFLICT.
	require.NoError(t, repo.WriteDiff(ctx, diff))

	snap := domain.SnapshotEvent{
		Symbol:       sym,
		CapturedAt:   base.Add(time.Second),
		LastUpdateID: 110,
		Bids:         levels([2]string{priceLevelA, "1.5"}),
		Asks:         levels([2]string{priceLevelC, "0.7"}),
	}
	require.NoError(t, repo.WriteCheckpoint(ctx, snap))

	gotSnap, err := repo.QueryNearestCheckpoint(ctx, sym, base.Add(2*time.Second))
	require.NoError(t, err)
	require.Equal(t, sym, gotSnap.Symbol)
	require.Equal(t, int64(110), gotSnap.LastUpdateID)
	require.Len(t, gotSnap.Bids, 1)
	require.True(t, gotSnap.Bids[0].Price.Equal(decimal.RequireFromString(priceLevelA)))

	gotDiffs, err := repo.QueryDiffs(ctx, sym, base.Add(-time.Second), base.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, gotDiffs, 1)
	require.Equal(t, int64(100), gotDiffs[0].FirstUpdateID)
	require.Equal(t, int64(110), gotDiffs[0].FinalUpdateID)
	require.Len(t, gotDiffs[0].Bids, 2)
	require.True(t, gotDiffs[0].Bids[1].Price.Equal(decimal.RequireFromString(priceLevelB)))
}
