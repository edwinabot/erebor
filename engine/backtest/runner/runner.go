package runner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/edwinabot/erebor/backtest/collector"
	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/publisher"
	"github.com/edwinabot/erebor/backtest/replay"
	"github.com/edwinabot/erebor/backtest/repository"
	ingestrepository "github.com/edwinabot/erebor/ingest/repository"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// RunnerConfig holds the configuration parameters for a single backtest run.
type RunnerConfig struct {
	RunID          string
	Symbols        []string
	From           time.Time
	To             time.Time
	Depth          int
	SpeedMode      domain.SpeedMode
	SpeedFactor    float64
	StrategyConfig string
}

// BacktestRunner orchestrates the full lifecycle of a single backtest run:
// run record creation, multi-symbol replay fan-out, signal collection,
// stream TTL, and final status persistence.
//
// Lifecycle per spec §9:
//
//	PENDING → RUNNING → COMPLETED
//	                  → FAILED
//	                  → CANCELLED   (SIGTERM or context cancellation)
type BacktestRunner struct {
	cfg       RunnerConfig
	namespace string

	btRepo     *repository.BacktestRepository
	ingestRepo ingestrepository.Repository
	l2Pub      *publisher.L2Publisher
	ctrlPub    *publisher.ControlPublisher
	redis      *redis.Client
	logger     *zap.Logger
}

// New creates a BacktestRunner. namespace is derived from cfg.RunID and is used
// as the prefix for all stream keys.
func New(
	cfg RunnerConfig,
	btRepo *repository.BacktestRepository,
	ingestRepo ingestrepository.Repository,
	l2Pub *publisher.L2Publisher,
	ctrlPub *publisher.ControlPublisher,
	redisClient *redis.Client,
	logger *zap.Logger,
) *BacktestRunner {
	return &BacktestRunner{
		cfg:        cfg,
		namespace:  "erebor:backtest:" + cfg.RunID,
		btRepo:     btRepo,
		ingestRepo: ingestRepo,
		l2Pub:      l2Pub,
		ctrlPub:    ctrlPub,
		redis:      redisClient,
		logger:     logger.With(zap.String("component", "backtest-runner"), zap.String("run_id", cfg.RunID)),
	}
}

// Run executes the backtest from start to finish, blocking until complete.
// Cancelling ctx triggers a CANCELLED transition and clean shutdown.
func (r *BacktestRunner) Run(ctx context.Context) error {
	start := time.Now()
	r.logger.Info("backtest run starting",
		zap.String("run_id", r.cfg.RunID),
		zap.Strings("symbols", r.cfg.Symbols),
		zap.Time("from", r.cfg.From),
		zap.Time("to", r.cfg.To),
		zap.String("speed_mode", string(r.cfg.SpeedMode)),
		zap.Float64("speed_factor", r.cfg.SpeedFactor),
		zap.Int("depth", r.cfg.Depth),
		zap.String("namespace", r.namespace),
	)

	// 1. Create run record (PENDING).
	var speedFactor *float64
	if r.cfg.SpeedMode == domain.SpeedNX {
		speedFactor = &r.cfg.SpeedFactor
	}
	rec := domain.RunRecord{
		RunID:          r.cfg.RunID,
		Symbols:        r.cfg.Symbols,
		FromTime:       r.cfg.From,
		ToTime:         r.cfg.To,
		SpeedMode:      r.cfg.SpeedMode,
		SpeedFactor:    speedFactor,
		StrategyConfig: r.cfg.StrategyConfig,
		Status:         domain.RunStatusPending,
	}
	if err := r.btRepo.CreateRun(ctx, rec); err != nil {
		return fmt.Errorf("create run record: %w", err)
	}

	// 2. Transition → RUNNING.
	now := time.Now()
	if err := r.btRepo.UpdateRunStatus(ctx, r.cfg.RunID, domain.RunStatusRunning, &now, nil, ""); err != nil {
		return fmt.Errorf("set run RUNNING: %w", err)
	}

	// 3. Publish REPLAY_START so consumers can initialise.
	if err := r.ctrlPub.Publish(ctx, domain.ControlEvent{
		RunID:   r.cfg.RunID,
		Type:    domain.ControlReplayStart,
		Payload: map[string]string{"symbols": strings.Join(r.cfg.Symbols, ",")},
	}); err != nil {
		r.logger.Warn("failed to publish REPLAY_START; continuing", zap.Error(err))
	}

	// 4. Start signal collector in background.
	col := collector.New(r.redis, r.namespace, r.cfg.RunID, r.logger)
	colCtx, colCancel := context.WithCancel(context.Background())
	defer colCancel()
	col.Start(colCtx)

	// 5. Fan out one ReplayEngine per symbol.
	speed := replay.NewSpeedController(r.cfg.SpeedMode, r.cfg.SpeedFactor, r.logger)
	g, gctx := errgroup.WithContext(ctx)

	for _, sym := range r.cfg.Symbols {
		sym := sym
		g.Go(func() error {
			eng := replay.NewEngine(
				replay.EngineConfig{
					RunID:  r.cfg.RunID,
					Symbol: sym,
					From:   r.cfg.From,
					To:     r.cfg.To,
					Depth:  r.cfg.Depth,
				},
				r.ingestRepo,
				r.btRepo,
				r.l2Pub,
				r.ctrlPub,
				speed,
				r.logger,
			)
			return eng.Run(gctx)
		})
	}

	r.logger.Info("replay engines launched", zap.Int("symbol_count", len(r.cfg.Symbols)))
	runErr := g.Wait()

	// 6. Stop collector and wait for it to flush.
	colCancel()
	col.Wait()
	signalCounts := col.SignalCounts()
	r.logger.Info("collector stopped",
		zap.Int64("total_signals", col.TotalSignals()),
		zap.Any("per_symbol", signalCounts),
	)

	// Use a background context for terminal state writes — the run context may
	// already be cancelled (SIGTERM), but we still need to persist final status.
	bgCtx := context.Background()

	// 7a. Context cancelled → CANCELLED (SIGTERM path).
	if ctx.Err() != nil {
		r.logger.Warn("run cancelled by context",
			zap.Duration("elapsed", time.Since(start)),
			zap.Error(ctx.Err()),
		)
		r.publishControl(bgCtx, domain.ControlCancelled, nil)
		_ = r.btRepo.UpdateRunStatus(bgCtx, r.cfg.RunID, domain.RunStatusCancelled, nil, nil, "")
		return ctx.Err()
	}

	// 7b. ReplayEngine error → FAILED.
	if runErr != nil {
		r.logger.Error("run failed",
			zap.Duration("elapsed", time.Since(start)),
			zap.Error(runErr),
		)
		r.publishControl(bgCtx, domain.ControlCancelled, nil)
		completed := time.Now()
		_ = r.btRepo.UpdateRunStatus(bgCtx, r.cfg.RunID, domain.RunStatusFailed, nil, &completed, runErr.Error())
		return runErr
	}

	// 8. Publish REPLAY_COMPLETE (fire-and-forget — erebor-signals drains on its own).
	if err := r.ctrlPub.Publish(ctx, domain.ControlEvent{
		RunID:   r.cfg.RunID,
		Type:    domain.ControlReplayComplete,
		Payload: map[string]string{"symbols": strings.Join(r.cfg.Symbols, ",")},
	}); err != nil {
		r.logger.Warn("failed to publish REPLAY_COMPLETE", zap.Error(err))
	}

	// 9. Set 24-hour TTL on all run-namespaced stream keys.
	expiredCount := r.expireStreams(bgCtx)
	r.logger.Info("stream TTLs set",
		zap.Int("stream_count", expiredCount),
		zap.Duration("ttl", 24*time.Hour),
	)

	// 10. Transition → COMPLETED.
	completed := time.Now()
	if err := r.btRepo.UpdateRunStatus(bgCtx, r.cfg.RunID, domain.RunStatusCompleted, nil, &completed, ""); err != nil {
		r.logger.Error("failed to mark run COMPLETED", zap.Error(err))
	}

	r.logger.Info("backtest run complete",
		zap.String("run_id", r.cfg.RunID),
		zap.Duration("elapsed", time.Since(start)),
		zap.Int64("total_signals", col.TotalSignals()),
		zap.Any("signals_per_symbol", signalCounts),
	)
	return nil
}

// publishControl is a best-effort helper used during error/cancellation paths.
func (r *BacktestRunner) publishControl(ctx context.Context, evType domain.ControlEventType, payload map[string]string) {
	if err := r.ctrlPub.Publish(ctx, domain.ControlEvent{
		RunID:   r.cfg.RunID,
		Type:    evType,
		Payload: payload,
	}); err != nil {
		r.logger.Warn("failed to publish control event",
			zap.String("type", string(evType)),
			zap.Error(err),
		)
	}
}

// expireStreams sets a 24-hour TTL on all stream keys under the run namespace.
// Returns the number of keys that were expired.
func (r *BacktestRunner) expireStreams(ctx context.Context) int {
	pattern := r.namespace + ":*"
	keys, err := r.redis.Keys(ctx, pattern).Result()
	if err != nil {
		r.logger.Error("failed to list stream keys for TTL", zap.String("pattern", pattern), zap.Error(err))
		return 0
	}

	const ttl = 24 * time.Hour
	var count int
	for _, key := range keys {
		if err := r.redis.Expire(ctx, key, ttl).Err(); err != nil {
			r.logger.Warn("failed to set TTL on stream key", zap.String("key", key), zap.Error(err))
			continue
		}
		r.logger.Debug("TTL set on stream key", zap.String("key", key), zap.Duration("ttl", ttl))
		count++
	}
	return count
}
