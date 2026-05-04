package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/shopspring/decimal"
)

type DepthFetcher interface {
	FetchSnapshot(ctx context.Context, symbol string, limit int) (domain.SnapshotEvent, error)
}

type HTTPFetcher struct {
	baseURL string
	client  *http.Client
}

func New(baseURL string) *HTTPFetcher {
	return &HTTPFetcher{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

type rawDepthResponse struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

func (f *HTTPFetcher) FetchSnapshot(ctx context.Context, symbol string, limit int) (domain.SnapshotEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	u := fmt.Sprintf("%s/api/v3/depth?symbol=%s&limit=%d",
		f.baseURL, url.QueryEscape(strings.ToUpper(symbol)), limit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("build request: %w", err)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return domain.SnapshotEvent{}, fmt.Errorf("depth endpoint status %d: %s", resp.StatusCode, string(body))
	}

	var raw rawDepthResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("decode depth: %w", err)
	}

	bids, err := parseLevels(raw.Bids)
	if err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("parse bids: %w", err)
	}
	asks, err := parseLevels(raw.Asks)
	if err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("parse asks: %w", err)
	}

	return domain.SnapshotEvent{
		Symbol:       strings.ToUpper(symbol),
		CapturedAt:   time.Now().UTC(),
		LastUpdateID: raw.LastUpdateID,
		Bids:         bids,
		Asks:         asks,
	}, nil
}

func parseLevels(in [][]string) ([]domain.PriceLevel, error) {
	out := make([]domain.PriceLevel, 0, len(in))
	for i, pair := range in {
		if len(pair) < 2 {
			return nil, fmt.Errorf("level %d malformed: expected [price, qty]", i)
		}
		price, err := decimal.NewFromString(pair[0])
		if err != nil {
			return nil, fmt.Errorf("level %d price %q: %w", i, pair[0], err)
		}
		qty, err := decimal.NewFromString(pair[1])
		if err != nil {
			return nil, fmt.Errorf("level %d qty %q: %w", i, pair[1], err)
		}
		out = append(out, domain.PriceLevel{Price: price, Quantity: qty})
	}
	return out, nil
}

