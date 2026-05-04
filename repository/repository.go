package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type Repository interface {
	WriteDiff(ctx context.Context, event domain.DiffEvent) error
	WriteCheckpoint(ctx context.Context, snapshot domain.SnapshotEvent) error
	QueryNearestCheckpoint(ctx context.Context, symbol string, at time.Time) (domain.SnapshotEvent, error)
	QueryDiffs(ctx context.Context, symbol string, from time.Time, to time.Time) ([]domain.DiffEvent, error)
}

type PGRepository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *PGRepository {
	return &PGRepository{pool: pool}
}

func (r *PGRepository) WriteDiff(ctx context.Context, event domain.DiffEvent) error {
	bidsJSON, err := levelsToJSON(event.Bids)
	if err != nil {
		return fmt.Errorf("encode bids: %w", err)
	}
	asksJSON, err := levelsToJSON(event.Asks)
	if err != nil {
		return fmt.Errorf("encode asks: %w", err)
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO order_book_diffs
		    (event_time, symbol, first_update_id, final_update_id, bids, asks, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (symbol, final_update_id) DO NOTHING
	`,
		event.EventTime, event.Symbol, event.FirstUpdateID, event.FinalUpdateID,
		bidsJSON, asksJSON, time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert diff: %w", err)
	}
	return nil
}

func (r *PGRepository) WriteCheckpoint(ctx context.Context, snapshot domain.SnapshotEvent) error {
	bidsJSON, err := levelsToJSON(snapshot.Bids)
	if err != nil {
		return fmt.Errorf("encode bids: %w", err)
	}
	asksJSON, err := levelsToJSON(snapshot.Asks)
	if err != nil {
		return fmt.Errorf("encode asks: %w", err)
	}
	depth := len(snapshot.Bids)
	if len(snapshot.Asks) > depth {
		depth = len(snapshot.Asks)
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO order_book_snapshots
		    (snapshot_time, symbol, last_update_id, depth, bids, asks)
		VALUES ($1, $2, $3, $4, $5, $6)
	`,
		snapshot.CapturedAt, snapshot.Symbol, snapshot.LastUpdateID,
		depth, bidsJSON, asksJSON,
	)
	if err != nil {
		return fmt.Errorf("insert checkpoint: %w", err)
	}
	return nil
}

func (r *PGRepository) QueryNearestCheckpoint(ctx context.Context, symbol string, at time.Time) (domain.SnapshotEvent, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT snapshot_time, symbol, last_update_id, bids, asks
		FROM order_book_snapshots
		WHERE symbol = $1 AND snapshot_time <= $2
		ORDER BY snapshot_time DESC
		LIMIT 1
	`, symbol, at)

	var snap domain.SnapshotEvent
	var bidsJSON, asksJSON []byte
	err := row.Scan(&snap.CapturedAt, &snap.Symbol, &snap.LastUpdateID, &bidsJSON, &asksJSON)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.SnapshotEvent{}, fmt.Errorf("no checkpoint found for %s before %s", symbol, at)
		}
		return domain.SnapshotEvent{}, fmt.Errorf("query checkpoint: %w", err)
	}
	bids, err := jsonToLevels(bidsJSON)
	if err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("decode bids: %w", err)
	}
	asks, err := jsonToLevels(asksJSON)
	if err != nil {
		return domain.SnapshotEvent{}, fmt.Errorf("decode asks: %w", err)
	}
	snap.Bids = bids
	snap.Asks = asks
	return snap, nil
}

func (r *PGRepository) QueryDiffs(ctx context.Context, symbol string, from time.Time, to time.Time) ([]domain.DiffEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT event_time, symbol, first_update_id, final_update_id, bids, asks
		FROM order_book_diffs
		WHERE symbol = $1 AND event_time >= $2 AND event_time <= $3
		ORDER BY event_time ASC, final_update_id ASC
	`, symbol, from, to)
	if err != nil {
		return nil, fmt.Errorf("query diffs: %w", err)
	}
	defer rows.Close()

	var out []domain.DiffEvent
	for rows.Next() {
		var ev domain.DiffEvent
		var bidsJSON, asksJSON []byte
		if err := rows.Scan(&ev.EventTime, &ev.Symbol, &ev.FirstUpdateID, &ev.FinalUpdateID, &bidsJSON, &asksJSON); err != nil {
			return nil, fmt.Errorf("scan diff: %w", err)
		}
		bids, err := jsonToLevels(bidsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode bids: %w", err)
		}
		asks, err := jsonToLevels(asksJSON)
		if err != nil {
			return nil, fmt.Errorf("decode asks: %w", err)
		}
		ev.Bids = bids
		ev.Asks = asks
		out = append(out, ev)
	}
	return out, rows.Err()
}

func levelsToJSON(levels []domain.PriceLevel) ([]byte, error) {
	pairs := make([][2]string, 0, len(levels))
	for _, lvl := range levels {
		pairs = append(pairs, [2]string{lvl.Price.String(), lvl.Quantity.String()})
	}
	return json.Marshal(pairs)
}

func jsonToLevels(data []byte) ([]domain.PriceLevel, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var pairs [][2]string
	if err := json.Unmarshal(data, &pairs); err != nil {
		return nil, err
	}
	out := make([]domain.PriceLevel, 0, len(pairs))
	for _, p := range pairs {
		price, err := decimal.NewFromString(p[0])
		if err != nil {
			return nil, fmt.Errorf("parse price %q: %w", p[0], err)
		}
		qty, err := decimal.NewFromString(p[1])
		if err != nil {
			return nil, fmt.Errorf("parse qty %q: %w", p[1], err)
		}
		out = append(out, domain.PriceLevel{Price: price, Quantity: qty})
	}
	return out, nil
}
