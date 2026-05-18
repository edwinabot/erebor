package compute

import (
	"testing"
	"time"

	"github.com/edwinabot/erebor/signals/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

var testTime = time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)

func d(s string) decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return v
}

func levels(pairs ...string) []domain.PriceLevel {
	lvls := make([]domain.PriceLevel, 0, len(pairs)/2)
	for i := 0; i < len(pairs)-1; i += 2 {
		lvls = append(lvls, domain.PriceLevel{Price: d(pairs[i]), Quantity: d(pairs[i+1])})
	}
	return lvls
}

func event(bids, asks []domain.PriceLevel) domain.L2BookUpdateEvent {
	return domain.L2BookUpdateEvent{
		Symbol:    "BTCUSDT",
		EventTime: testTime,
		Bids:      bids,
		Asks:      asks,
	}
}

func TestBookImbalance(t *testing.T) {
	tests := []struct {
		name     string
		bids     []domain.PriceLevel
		asks     []domain.PriceLevel
		depth    int
		expected string
	}{
		{
			name:     "equal quantities",
			bids:     levels("100", "5"),
			asks:     levels("101", "5"),
			depth:    10,
			expected: "0",
		},
		{
			name:     "all bids",
			bids:     levels("100", "10"),
			asks:     levels("101", "0"),
			depth:    10,
			expected: "1",
		},
		{
			name:     "all asks",
			bids:     levels("100", "0"),
			asks:     levels("101", "10"),
			depth:    10,
			expected: "-1",
		},
		{
			name:     "bid heavy",
			bids:     levels("100", "3"),
			asks:     levels("101", "1"),
			depth:    10,
			expected: "0.5",
		},
		{
			name:     "ask heavy",
			bids:     levels("100", "1"),
			asks:     levels("101", "3"),
			depth:    10,
			expected: "-0.5",
		},
		{
			name:     "empty book returns zero",
			bids:     nil,
			asks:     nil,
			depth:    10,
			expected: "0",
		},
		{
			name:     "zero total qty returns zero",
			bids:     levels("100", "0"),
			asks:     levels("101", "0"),
			depth:    10,
			expected: "0",
		},
		{
			name: "depth truncation",
			// 3 bid levels, depth=2: only top 2 counted
			bids:     levels("100", "2", "99", "2", "98", "100"),
			asks:     levels("101", "4"),
			depth:    2,
			expected: "0",
		},
		{
			name:     "depth=0 means unlimited",
			bids:     levels("100", "2", "99", "2"),
			asks:     levels("101", "1"),
			depth:    0,
			expected: "0.6", // (4-1)/(4+1)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig := bookImbalance(event(tt.bids, tt.asks), tt.depth)
			assert.Equal(t, "book_imbalance", sig.Name)
			assert.Equal(t, "1", sig.Version)
			assert.Equal(t, testTime, sig.EventTime)
			got, _ := sig.Value.Float64()
			exp, _ := d(tt.expected).Float64()
			assert.InDelta(t, exp, got, 1e-9)
		})
	}
}

func TestSpreadBps(t *testing.T) {
	tests := []struct {
		name     string
		bids     []domain.PriceLevel
		asks     []domain.PriceLevel
		expected string
	}{
		{
			name:     "1 tick spread on 100",
			bids:     levels("100", "1"),
			asks:     levels("101", "1"),
			expected: "99.50248756218905", // 1/100.5*10000
		},
		{
			name:     "empty bids returns zero",
			bids:     nil,
			asks:     levels("101", "1"),
			expected: "0",
		},
		{
			name:     "empty asks returns zero",
			bids:     levels("100", "1"),
			asks:     nil,
			expected: "0",
		},
		{
			name:     "zero spread returns zero",
			bids:     levels("100", "1"),
			asks:     levels("100", "1"),
			expected: "0",
		},
		{
			name:     "zero mid price returns zero",
			bids:     levels("0", "1"),
			asks:     levels("0", "1"),
			expected: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig := spreadBps(event(tt.bids, tt.asks))
			assert.Equal(t, "spread_bps", sig.Name)
			assert.Equal(t, "1", sig.Version)
			assert.Equal(t, testTime, sig.EventTime)
			got, _ := sig.Value.Float64()
			exp, _ := d(tt.expected).Float64()
			assert.InDelta(t, exp, got, 1e-6)
		})
	}
}

func TestMidPrice(t *testing.T) {
	tests := []struct {
		name     string
		bids     []domain.PriceLevel
		asks     []domain.PriceLevel
		expected string
	}{
		{
			name:     "simple mid",
			bids:     levels("100", "1"),
			asks:     levels("102", "1"),
			expected: "101",
		},
		{
			name:     "fractional mid",
			bids:     levels("100", "1"),
			asks:     levels("101", "1"),
			expected: "100.5",
		},
		{
			name:     "empty book returns zero",
			bids:     nil,
			asks:     nil,
			expected: "0",
		},
		{
			name:     "only bids returns zero",
			bids:     levels("100", "1"),
			asks:     nil,
			expected: "0",
		},
		{
			name:     "only asks returns zero",
			bids:     nil,
			asks:     levels("101", "1"),
			expected: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig := midPrice(event(tt.bids, tt.asks))
			assert.Equal(t, "mid_price", sig.Name)
			assert.Equal(t, "1", sig.Version)
			assert.Equal(t, testTime, sig.EventTime)
			got, _ := sig.Value.Float64()
			exp, _ := d(tt.expected).Float64()
			assert.InDelta(t, exp, got, 1e-9)
		})
	}
}

func TestAllReturnsThreeSignals(t *testing.T) {
	e := event(levels("100", "5"), levels("101", "3"))
	sigs := All(e, 10)
	assert.Len(t, sigs, 3)
	names := make([]string, len(sigs))
	for i, s := range sigs {
		names[i] = s.Name
	}
	assert.Contains(t, names, "book_imbalance")
	assert.Contains(t, names, "spread_bps")
	assert.Contains(t, names, "mid_price")
}

func TestEventTimePropagatesToSignals(t *testing.T) {
	e := event(levels("100", "1"), levels("101", "1"))
	for _, sig := range All(e, 10) {
		assert.Equal(t, testTime, sig.EventTime, "signal %q must propagate EventTime", sig.Name)
	}
}
