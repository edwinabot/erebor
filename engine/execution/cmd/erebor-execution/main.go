package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	backtestdomain "github.com/edwinabot/erebor/backtest/domain"
	backtest "github.com/edwinabot/erebor/backtest/execution"
	"github.com/edwinabot/erebor/execution/blotter"
	"github.com/edwinabot/erebor/execution/config"
	"github.com/edwinabot/erebor/execution/consumer"
	"github.com/edwinabot/erebor/execution/decider"
	"github.com/edwinabot/erebor/execution/l2cache"
	"github.com/edwinabot/erebor/execution/order"
	"github.com/edwinabot/erebor/execution/repository"
	"github.com/edwinabot/erebor/execution/session"
	"github.com/edwinabot/erebor/fillmath"
	riskpkg "github.com/edwinabot/erebor/risk"
	signalsdomain "github.com/edwinabot/erebor/signals/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const liveNamespace = "erebor:live"

func main() {
	configPath := flag.String("config", "config/execution.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := buildLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("erebor-execution starting",
		zap.Strings("symbols", cfg.Symbols),
		zap.String("health_addr", cfg.Health.Addr),
	)

	// Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Fatal("redis ping failed", zap.String("addr", cfg.Redis.Addr), zap.Error(err))
	}
	logger.Info("redis connected", zap.String("addr", cfg.Redis.Addr))

	// TimescaleDB
	dsn := requireEnv("TIMESCALE_DSN", logger)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Fatal("pgxpool connect failed", zap.Error(err))
	}
	defer pool.Close()

	repo := repository.New(pool, logger)

	// Session lifecycle
	sessionMgr := session.NewManager(repo, logger)
	result, err := sessionMgr.Start(ctx, cfg.Symbols, cfg.StrategyConfig)
	if err != nil {
		logger.Fatal("session start failed", zap.Error(err))
	}
	sess := result.Session
	logger.Info("paper session active",
		zap.String("session_id", sess.SessionID),
		zap.Bool("recovered", result.Recovered),
	)

	// Strategy config
	stratCfg, err := backtest.ParseStrategyConfig(cfg.StrategyConfig)
	if err != nil {
		logger.Fatal("parse strategy_config failed", zap.Error(err))
	}

	// Risk
	riskCfg := riskpkg.Config{
		InitialCapital:  stratCfg.InitialCapital,
		MaxPositionQty:  stratCfg.MaxPositionQty,
		MaxDrawdownPct:  stratCfg.MaxDrawdownPct,
		RunLossLimitPct: stratCfg.RunLossLimitPct,
	}
	haltStore := riskpkg.NewRedisHaltStore(redisClient)
	riskPublisher := riskpkg.NewRedisPublisher(redisClient)
	riskChecker := riskpkg.NewWithLogger(riskCfg, riskPublisher, logger, liveNamespace, sess.SessionID,
		riskpkg.WithHaltStore(haltStore),
	)

	// L2 cache
	l2Cache := l2cache.New(redisClient, liveNamespace, cfg.Symbols, logger)
	l2Cache.Start(ctx)

	// Blotter
	initialEquity := stratCfg.InitialCapital
	if result.Recovered && !result.Equity.IsZero() {
		initialEquity = result.Equity
	}
	blot := blotter.New(sess.SessionID, initialEquity, repo, logger)
	if result.Recovered {
		blot.SeedPositions(result.Positions)
	}

	// Seed risk checker positions from recovery
	if result.Recovered {
		for _, p := range result.Positions {
			side := backtestdomain.SideBuy
			riskChecker.RecordFill(p.Symbol, side, p.NetQty.Abs(), p.AvgEntry, decimal.Zero)
		}
	}

	// Order publisher
	orderPub := order.NewPublisher(redisClient, liveNamespace, logger)

	// Decider
	dec := decider.New(stratCfg)

	// Signal handler (the hot path)
	handler := buildHandler(handlerDeps{
		sessionID:  sess.SessionID,
		dec:        dec,
		risk:       riskChecker,
		cache:      l2Cache,
		blot:       blot,
		orderPub:   orderPub,
		stratCfg:   stratCfg,
		sessionMgr: sessionMgr,
		logger:     logger,
	})

	// Signal consumer
	cons := consumer.New(redisClient, liveNamespace, logger,
		consumer.WithHandler(handler),
	)
	if err := cons.Start(ctx); err != nil {
		logger.Fatal("consumer start failed", zap.Error(err))
	}
	logger.Info("signal consumer started")

	// Health server
	healthSrv := startHealthServer(cfg.Health.Addr, cons, sess, logger)

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop health server
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("health server shutdown error", zap.Error(err))
	}

	// Drain consumer
	cons.Stop()

	// Mark session stopped
	if err := sessionMgr.Stop(shutdownCtx); err != nil {
		logger.Error("session stop failed", zap.Error(err))
	}

	logger.Info("erebor-execution stopped")
}

// handlerDeps bundles all dependencies needed by the signal handler closure.
type handlerDeps struct {
	sessionID  string
	dec        *decider.Decider
	risk       *riskpkg.Checker
	cache      *l2cache.Cache
	blot       *blotter.Blotter
	orderPub   *order.Publisher
	stratCfg   backtest.StrategyConfig
	sessionMgr *session.Manager
	logger     *zap.Logger
}

func buildHandler(d handlerDeps) consumer.Handler {
	return func(ctx context.Context, msgID string, sig signalsdomain.SignalEvent) error {
		posState, _ := d.blot.Position(sig.Symbol)
		side, shouldTrade := d.dec.Decide(sig.Symbol, sig, posState)
		if !shouldTrade {
			d.logger.Debug("no trade", zap.String("symbol", sig.Symbol), zap.String("signal", sig.Name))
			return nil
		}
		if riskErr := d.risk.CanTrade(sig.Symbol, side, d.stratCfg.TradeQty, sig.EventTime); riskErr != nil {
			d.logger.Warn("trade blocked by risk",
				zap.String("symbol", sig.Symbol),
				zap.String("side", string(side)),
				zap.Error(riskErr),
			)
			if d.risk.Halted() {
				_ = d.sessionMgr.Halt(ctx, riskErr.Error())
			}
			return nil
		}
		return executeFill(ctx, d, msgID, sig, side)
	}
}

func executeFill(ctx context.Context, d handlerDeps, msgID string, sig signalsdomain.SignalEvent, side backtestdomain.Side) error {
	bid, ask, ok := d.cache.BestPrices(sig.Symbol)
	if !ok {
		d.logger.Warn("no L2 data; skipping fill",
			zap.String("symbol", sig.Symbol),
			zap.String("signal_id", msgID),
		)
		return nil
	}

	isBuy := side == backtestdomain.SideBuy
	bestPrice := ask
	if !isBuy {
		bestPrice = bid
	}
	fillPrice := fillmath.ComputeFillPrice(isBuy, bestPrice, d.stratCfg.SlippageBps)
	fee := fillmath.ComputeFee(d.stratCfg.TradeQty, fillPrice, d.stratCfg.TakerFeeBps)

	tradeID, _ := uuid.NewV7()
	fillReq := blotter.FillRequest{
		TradeID:        tradeID.String(),
		Symbol:         sig.Symbol,
		Side:           side,
		FillPrice:      fillPrice,
		FillQty:        d.stratCfg.TradeQty,
		Fee:            fee,
		EventTime:      sig.EventTime,
		SignalName:     sig.Name,
		SignalStreamID: msgID,
	}
	if err := d.blot.RecordFill(ctx, fillReq); err != nil {
		d.logger.Error("blotter record fill failed; will retry on redelivery",
			zap.String("symbol", sig.Symbol),
			zap.String("signal_id", msgID),
			zap.Error(err),
		)
		return err
	}

	d.risk.RecordFill(sig.Symbol, side, d.stratCfg.TradeQty, fillPrice, fee)
	publishOrder(ctx, d, sig, side, fillPrice, fee)

	d.logger.Info("paper fill executed",
		zap.String("symbol", sig.Symbol),
		zap.String("side", string(side)),
		zap.String("fill_price", fillPrice.String()),
		zap.String("qty", d.stratCfg.TradeQty.String()),
		zap.String("fee", fee.String()),
		zap.Time("event_time", sig.EventTime),
	)
	return nil
}

func publishOrder(ctx context.Context, d handlerDeps, sig signalsdomain.SignalEvent, side backtestdomain.Side, fillPrice, fee decimal.Decimal) {
	orderID, _ := uuid.NewV7()
	ord := backtestdomain.OrderEvent{
		RunID:      d.sessionID,
		Symbol:     sig.Symbol,
		EventTime:  sig.EventTime,
		OrderID:    orderID.String(),
		Side:       side,
		Type:       backtestdomain.OrderTypeMarket,
		Price:      decimal.Zero,
		Quantity:   d.stratCfg.TradeQty,
		Status:     backtestdomain.OrderStatusFilled,
		FillPrice:  fillPrice,
		FillQty:    d.stratCfg.TradeQty,
		Fee:        fee,
		SignalName: sig.Name,
	}
	if err := d.orderPub.Publish(ctx, ord); err != nil {
		d.logger.Error("publish order failed (fill already persisted)",
			zap.String("order_id", orderID.String()),
			zap.Error(err),
		)
	}
}

func startHealthServer(addr string, cons *consumer.Consumer, _ interface{}, logger *zap.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !cons.IsRunning() {
			http.Error(w, "consumer not running", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warn("health server error", zap.Error(err))
		}
	}()
	logger.Info("health server started", zap.String("addr", addr))
	return srv
}

func buildLogger(cfg config.Config) (*zap.Logger, error) {
	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(cfg.Log.Level)); err != nil {
		level.SetLevel(zap.InfoLevel)
	}
	zapCfg := zap.NewProductionConfig()
	zapCfg.Level = level
	if cfg.Log.FilePath != "" {
		zapCfg.OutputPaths = append(zapCfg.OutputPaths, cfg.Log.FilePath)
	}
	return zapCfg.Build()
}

func requireEnv(key string, logger *zap.Logger) string {
	v := os.Getenv(key)
	if v == "" {
		logger.Fatal("required env var not set", zap.String("var", key))
	}
	return v
}
