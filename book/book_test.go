package book_test

import (
	"testing"
	"time"

	"github.com/edwinabot/erebor/ingest/book"
	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func mustDec(t *testing.T, s string) decimal.Decimal {
	t.Helper()
	d, err := decimal.NewFromString(s)
	require.NoError(t, err)
	return d
}

func lvl(t *testing.T, price, qty string) domain.PriceLevel {
	return domain.PriceLevel{Price: mustDec(t, price), Quantity: mustDec(t, qty)}
}

// TestOrderBookApplyAddNewLevel verifies that applying a diff with
// previously-unseen price levels inserts them on the correct side and that
// LastUpdateID is advanced to the diff's FinalUpdateID.
func TestOrderBookApplyAddNewLevel(t *testing.T) {
	b := book.New("BTCUSDT")
	err := b.Apply(domain.DiffEvent{
		Symbol:        "BTCUSDT",
		EventTime:     time.Now(),
		FirstUpdateID: 1,
		FinalUpdateID: 2,
		Bids:          []domain.PriceLevel{lvl(t, "100.0", "1.5")},
		Asks:          []domain.PriceLevel{lvl(t, "101.0", "0.7")},
	})
	require.NoError(t, err)

	snap := b.Snapshot(10)
	require.Len(t, snap.Bids, 1)
	require.Len(t, snap.Asks, 1)
	require.True(t, snap.Bids[0].Price.Equal(mustDec(t, "100.0")))
	require.True(t, snap.Bids[0].Quantity.Equal(mustDec(t, "1.5")))
	require.Equal(t, int64(2), b.LastUpdateID())
}

// TestOrderBookApplyUpdateExistingLevel verifies that a diff carrying a
// price level already present in the book replaces its quantity rather
// than creating a duplicate entry.
func TestOrderBookApplyUpdateExistingLevel(t *testing.T) {
	b := book.New("BTCUSDT")
	require.NoError(t, b.Apply(domain.DiffEvent{
		FinalUpdateID: 1,
		Bids:          []domain.PriceLevel{lvl(t, "100.0", "1.5")},
	}))
	require.NoError(t, b.Apply(domain.DiffEvent{
		FirstUpdateID: 2,
		FinalUpdateID: 2,
		Bids:          []domain.PriceLevel{lvl(t, "100.0", "2.5")},
	}))

	snap := b.Snapshot(10)
	require.Len(t, snap.Bids, 1)
	require.True(t, snap.Bids[0].Quantity.Equal(mustDec(t, "2.5")))
}

// TestOrderBookApplyRemoveLevelOnZeroQuantity verifies the Binance delete
// convention: a diff entry with quantity "0" removes the price level from
// the book entirely (other levels untouched).
func TestOrderBookApplyRemoveLevelOnZeroQuantity(t *testing.T) {
	b := book.New("BTCUSDT")
	require.NoError(t, b.Apply(domain.DiffEvent{
		FinalUpdateID: 1,
		Bids: []domain.PriceLevel{
			lvl(t, "100.0", "1.5"),
			lvl(t, "99.0", "2.0"),
		},
	}))
	require.NoError(t, b.Apply(domain.DiffEvent{
		FinalUpdateID: 2,
		Bids:          []domain.PriceLevel{lvl(t, "100.0", "0")},
	}))

	snap := b.Snapshot(10)
	require.Len(t, snap.Bids, 1)
	require.True(t, snap.Bids[0].Price.Equal(mustDec(t, "99.0")))
}

// TestOrderBookApplyMultipleBidsAndAsksSimultaneously verifies that a
// single Apply call can carry multiple bid and ask updates and that the
// resulting Snapshot orders bids descending and asks ascending.
func TestOrderBookApplyMultipleBidsAndAsksSimultaneously(t *testing.T) {
	b := book.New("BTCUSDT")
	require.NoError(t, b.Apply(domain.DiffEvent{
		FinalUpdateID: 1,
		Bids: []domain.PriceLevel{
			lvl(t, "100.0", "1.0"),
			lvl(t, "99.5", "2.0"),
			lvl(t, "99.0", "3.0"),
		},
		Asks: []domain.PriceLevel{
			lvl(t, "101.0", "1.0"),
			lvl(t, "101.5", "2.0"),
			lvl(t, "102.0", "3.0"),
		},
	}))

	snap := b.Snapshot(10)
	require.Len(t, snap.Bids, 3)
	require.Len(t, snap.Asks, 3)
	// Bids sorted descending.
	require.True(t, snap.Bids[0].Price.Equal(mustDec(t, "100.0")))
	require.True(t, snap.Bids[1].Price.Equal(mustDec(t, "99.5")))
	require.True(t, snap.Bids[2].Price.Equal(mustDec(t, "99.0")))
	// Asks sorted ascending.
	require.True(t, snap.Asks[0].Price.Equal(mustDec(t, "101.0")))
	require.True(t, snap.Asks[1].Price.Equal(mustDec(t, "101.5")))
	require.True(t, snap.Asks[2].Price.Equal(mustDec(t, "102.0")))
}

// TestOrderBookSnapshotLimitsToTopN verifies the depth-truncation contract
// of Snapshot(N): only the top-N price levels per side are emitted, with
// the correct ordering (best-bid first, best-ask first).
func TestOrderBookSnapshotLimitsToTopN(t *testing.T) {
	cases := []struct {
		name        string
		insertBids  []domain.PriceLevel
		insertAsks  []domain.PriceLevel
		depth       int
		wantTopBid  string
		wantTopAsk  string
		wantBidsLen int
		wantAsksLen int
	}{
		{
			name: "depth 2 of 4",
			insertBids: []domain.PriceLevel{
				{Price: decimal.RequireFromString("100"), Quantity: decimal.RequireFromString("1")},
				{Price: decimal.RequireFromString("99"), Quantity: decimal.RequireFromString("1")},
				{Price: decimal.RequireFromString("98"), Quantity: decimal.RequireFromString("1")},
				{Price: decimal.RequireFromString("97"), Quantity: decimal.RequireFromString("1")},
			},
			insertAsks: []domain.PriceLevel{
				{Price: decimal.RequireFromString("101"), Quantity: decimal.RequireFromString("1")},
				{Price: decimal.RequireFromString("102"), Quantity: decimal.RequireFromString("1")},
				{Price: decimal.RequireFromString("103"), Quantity: decimal.RequireFromString("1")},
				{Price: decimal.RequireFromString("104"), Quantity: decimal.RequireFromString("1")},
			},
			depth:       2,
			wantTopBid:  "100",
			wantTopAsk:  "101",
			wantBidsLen: 2,
			wantAsksLen: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := book.New("BTCUSDT")
			require.NoError(t, b.Apply(domain.DiffEvent{
				FinalUpdateID: 1,
				Bids:          tc.insertBids,
				Asks:          tc.insertAsks,
			}))
			snap := b.Snapshot(tc.depth)
			require.Len(t, snap.Bids, tc.wantBidsLen)
			require.Len(t, snap.Asks, tc.wantAsksLen)
			require.True(t, snap.Bids[0].Price.Equal(mustDec(t, tc.wantTopBid)))
			require.True(t, snap.Asks[0].Price.Equal(mustDec(t, tc.wantTopAsk)))
		})
	}
}

// TestOrderBookLoadSnapshotReplacesState verifies the bootstrap entry
// point: LoadSnapshot wipes any prior book state, populates bids and asks
// from the supplied snapshot, sets LastUpdateID to the snapshot's value,
// and silently drops any zero-quantity levels in the input (defence
// against a malformed snapshot from upstream).
func TestOrderBookLoadSnapshotReplacesState(t *testing.T) {
	b := book.New("BTCUSDT")
	require.NoError(t, b.Apply(domain.DiffEvent{
		FinalUpdateID: 1,
		Bids:          []domain.PriceLevel{lvl(t, "50", "5")},
		Asks:          []domain.PriceLevel{lvl(t, "60", "6")},
	}))

	b.LoadSnapshot(domain.SnapshotEvent{
		Symbol:       "BTCUSDT",
		LastUpdateID: 100,
		Bids: []domain.PriceLevel{
			lvl(t, "100", "1"),
			lvl(t, "99", "2"),
			lvl(t, "98.5", "0"), // zero qty levels are dropped on load
		},
		Asks: []domain.PriceLevel{
			lvl(t, "101", "1"),
		},
	})

	require.Equal(t, int64(100), b.LastUpdateID())
	snap := b.Snapshot(10)
	require.Len(t, snap.Bids, 2)
	require.True(t, snap.Bids[0].Price.Equal(mustDec(t, "100")))
	require.True(t, snap.Bids[1].Price.Equal(mustDec(t, "99")))
	require.Len(t, snap.Asks, 1)
	require.True(t, snap.Asks[0].Price.Equal(mustDec(t, "101")))
}

// TestOrderBookReset verifies that Reset clears all bid/ask state and
// zeroes LastUpdateID, leaving the book ready for a fresh bootstrap.
func TestOrderBookReset(t *testing.T) {
	b := book.New("BTCUSDT")
	require.NoError(t, b.Apply(domain.DiffEvent{
		FinalUpdateID: 5,
		Bids:          []domain.PriceLevel{lvl(t, "100", "1")},
		Asks:          []domain.PriceLevel{lvl(t, "101", "1")},
	}))
	b.Reset()
	require.Equal(t, int64(0), b.LastUpdateID())
	snap := b.Snapshot(10)
	require.Empty(t, snap.Bids)
	require.Empty(t, snap.Asks)
}
