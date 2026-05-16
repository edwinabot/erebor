package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/edwinabot/erebor/backtest/config"
	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/publisher"
	"github.com/edwinabot/erebor/backtest/repository"
	"github.com/edwinabot/erebor/backtest/runner"
	ingestrepository "github.com/edwinabot/erebor/ingest/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// parsedFlags holds the validated, parsed values from CLI flags.
type parsedFlags struct {
	runID          string
	symbols        []string
	from           time.Time
	to             time.Time
	speedMode      domain.SpeedMode
	speedFactor    float64
	depth          int
	strategyConfig string
}

func main() {
	// ── CLI flags ────────────────────────────────────────────────────────────

	runIDFlag := flag.String("run-id", "", "backtest run UUID (v7 generated if absent)")
	symbolsFlag := flag.String("symbols", "", "comma-separated symbols, e.g. BTCUSDT,ETHUSDT (required)")
	fromFlag := flag.String("from", "", "replay start time, RFC3339, e.g. 2026-01-01T00:00:00Z (required)")
	toFlag := flag.String("to", "", "replay end time, RFC3339 (required)")
	speedFlag := flag.String("speed", "AFAP", "speed mode: AFAP | NX | WALL_CLOCK")
	speedFactorFlag := flag.Float64("speed-factor", 1.0, "replay speed multiplier (only applies to NX mode)")
	depthFlag := flag.Int("depth", 10, "order book depth for published L2 snapshots")
	strategyConfigFlag := flag.String("strategy-config", "{}", "strategy parameters as JSON, e.g. '{\"maker_fee_bps\":10}'")
	logLevelFlag := flag.String("log-level", "info", "log level: debug | info | warn | error")

	flag.Parse()

	// ── Logger ───────────────────────────────────────────────────────────────

	logger, err := buildLogger(*logLevelFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build logger: %v\n", err)
		os.Exit(2)
	}
	defer func() { _ = logger.Sync() }()

	root := logger.With(zap.String("component", "main"))

	// ── Validate and parse flags ─────────────────────────────────────────────

	pf, err := validateFlags(*symbolsFlag, *fromFlag, *toFlag, *speedFlag, *strategyConfigFlag, *speedFactorFlag, *depthFlag)
	if err != nil {
		root.Fatal("invalid flags", zap.Error(err))
	}

	// ── Run ID ───────────────────────────────────────────────────────────────

	runID := *runIDFlag
	if runID == "" {
		id, genErr := uuid.NewV7()
		if genErr != nil {
			root.Fatal("failed to generate run ID", zap.Error(genErr))
		}
		runID = id.String()
	}

	root.Info("erebor-backtest starting",
		zap.String("run_id", runID),
		zap.Strings("symbols", pf.symbols),
		zap.Time("from", pf.from),
		zap.Time("to", pf.to),
		zap.String("speed_mode", string(pf.speedMode)),
		zap.Float64("speed_factor", pf.speedFactor),
		zap.Int("depth", pf.depth),
		zap.String("strategy_config", pf.strategyConfig),
	)

	// ── Infrastructure config ────────────────────────────────────────────────

	cfg, err := config.Load()
	if err != nil {
		root.Fatal("load config", zap.Error(err))
	}
	root.Info("infrastructure config loaded",
		zap.String("redis_addr", cfg.RedisAddr),
	)

	// ── Signal handling ──────────────────────────────────────────────────────

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// ── Redis ────────────────────────────────────────────────────────────────

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
	})
	defer func() { _ = redisClient.Close() }()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		root.Fatal("redis ping failed", zap.String("addr", cfg.RedisAddr), zap.Error(err))
	}
	root.Info("redis connected", zap.String("addr", cfg.RedisAddr))

	// ── TimescaleDB ──────────────────────────────────────────────────────────

	pool, err := pgxpool.New(ctx, cfg.TimescaleDSN)
	if err != nil {
		root.Fatal("failed to open DB pool", zap.Error(err))
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		root.Fatal("DB ping failed", zap.Error(err))
	}
	root.Info("timescaledb connected")

	// ── Component wiring ─────────────────────────────────────────────────────

	namespace := "erebor:backtest:" + runID

	btRepo := repository.New(pool, logger)
	ingestRepo := ingestrepository.New(pool)

	pubs := runner.Publishers{
		L2:      publisher.NewL2Publisher(redisClient, namespace, logger),
		Control: publisher.NewControlPublisher(redisClient, namespace, logger),
	}

	r := runner.New(
		runner.RunnerConfig{
			RunID:          runID,
			Symbols:        pf.symbols,
			From:           pf.from,
			To:             pf.to,
			Depth:          pf.depth,
			SpeedMode:      pf.speedMode,
			SpeedFactor:    pf.speedFactor,
			StrategyConfig: pf.strategyConfig,
		},
		btRepo,
		ingestRepo,
		pubs,
		redisClient,
		logger,
	)

	// ── Run ──────────────────────────────────────────────────────────────────

	root.Info("starting backtest run", zap.String("run_id", runID))
	if err := r.Run(ctx); err != nil {
		if ctx.Err() != nil {
			root.Info("run cancelled by signal", zap.String("run_id", runID))
			os.Exit(0)
		}
		root.Error("run failed", zap.String("run_id", runID), zap.Error(err))
		os.Exit(1)
	}

	root.Info("erebor-backtest finished", zap.String("run_id", runID))
}

// validateFlags validates and parses all CLI flag values, returning a parsedFlags
// struct. Returns an error describing the first invalid value encountered.
func validateFlags(symbolsFlag, fromFlag, toFlag, speedFlag, strategyConfigFlag string, speedFactorFlag float64, depthFlag int) (parsedFlags, error) {
	if symbolsFlag == "" {
		return parsedFlags{}, fmt.Errorf("--symbols is required")
	}
	if fromFlag == "" {
		return parsedFlags{}, fmt.Errorf("--from is required")
	}
	if toFlag == "" {
		return parsedFlags{}, fmt.Errorf("--to is required")
	}

	symbols := splitSymbols(symbolsFlag)
	if len(symbols) == 0 {
		return parsedFlags{}, fmt.Errorf("--symbols must contain at least one symbol")
	}

	from, err := time.Parse(time.RFC3339, fromFlag)
	if err != nil {
		return parsedFlags{}, fmt.Errorf("--from must be a valid RFC3339 timestamp (%q): %w", fromFlag, err)
	}
	to, err := time.Parse(time.RFC3339, toFlag)
	if err != nil {
		return parsedFlags{}, fmt.Errorf("--to must be a valid RFC3339 timestamp (%q): %w", toFlag, err)
	}
	if !to.After(from) {
		return parsedFlags{}, fmt.Errorf("--to (%s) must be after --from (%s)", to, from)
	}

	speedMode, err := parseSpeedMode(speedFlag)
	if err != nil {
		return parsedFlags{}, fmt.Errorf("invalid --speed value: %w", err)
	}
	if speedMode == domain.SpeedNX && speedFactorFlag <= 0 {
		return parsedFlags{}, fmt.Errorf("--speed-factor must be positive for NX mode (got %g)", speedFactorFlag)
	}

	if !json.Valid([]byte(strategyConfigFlag)) {
		return parsedFlags{}, fmt.Errorf("--strategy-config is not valid JSON (%q)", strategyConfigFlag)
	}

	return parsedFlags{
		symbols:        symbols,
		from:           from,
		to:             to,
		speedMode:      speedMode,
		speedFactor:    speedFactorFlag,
		depth:          depthFlag,
		strategyConfig: strategyConfigFlag,
	}, nil
}

// splitSymbols parses a comma-separated symbols string into a deduplicated,
// uppercased slice.
func splitSymbols(s string) []string {
	parts := strings.Split(s, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		sym := strings.ToUpper(strings.TrimSpace(p))
		if sym == "" {
			continue
		}
		if _, ok := seen[sym]; ok {
			continue
		}
		seen[sym] = struct{}{}
		out = append(out, sym)
	}
	return out
}

// parseSpeedMode validates and converts the --speed flag value.
func parseSpeedMode(s string) (domain.SpeedMode, error) {
	switch strings.ToUpper(s) {
	case "AFAP":
		return domain.SpeedAFAP, nil
	case "NX":
		return domain.SpeedNX, nil
	case "WALL_CLOCK":
		return domain.SpeedWallClock, nil
	default:
		return "", fmt.Errorf("unknown speed mode %q; valid values: AFAP, NX, WALL_CLOCK", s)
	}
}

func buildLogger(level string) (*zap.Logger, error) {
	zcfg := zap.NewProductionEncoderConfig()
	zcfg.TimeKey = "ts"
	zcfg.MessageKey = "msg"
	zcfg.LevelKey = "level"
	zcfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder

	lvl, err := parseLevel(level, zapcore.InfoLevel)
	if err != nil {
		return nil, fmt.Errorf("log level: %w", err)
	}

	encoder := zapcore.NewJSONEncoder(zcfg)
	core := zapcore.NewCore(encoder, zapcore.Lock(os.Stderr), lvl)
	return zap.New(core, zap.AddCaller()), nil
}

func parseLevel(s string, fallback zapcore.Level) (zap.AtomicLevel, error) {
	lvl := zap.NewAtomicLevelAt(fallback)
	if s == "" {
		return lvl, nil
	}
	if err := lvl.UnmarshalText([]byte(s)); err != nil {
		return lvl, fmt.Errorf("invalid level %q: %w", s, err)
	}
	return lvl, nil
}
