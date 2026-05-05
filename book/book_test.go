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
