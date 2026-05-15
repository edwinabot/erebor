package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/edwinabot/erebor/signals/config"
	"github.com/edwinabot/erebor/signals/consumer"
	"github.com/edwinabot/erebor/signals/publisher"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(2)
	}

	logger, closeLogFile, err := buildLogger(cfg.Log.Level, cfg.Log.FileLevel, cfg.Log.FilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build logger: %v\n", err)
		os.Exit(2)
	}
	defer func() { _ = closeLogFile() }()
	defer func() { _ = logger.Sync() }()

	rootLogger := logger.With(zap.String("component", "main"))

	if len(cfg.Symbols) == 0 {
		rootLogger.Fatal("at least one symbol must be configured")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		rootLogger.Fatal("redis ping failed", zap.Error(err))
	}

	pub := publisher.New(redisClient, cfg.StreamNamespace)

	consumerID, err := os.Hostname()
	if err != nil {
		consumerID = fmt.Sprintf("erebor-signals-%d", os.Getpid())
	}

	cons := consumer.New(
		redisClient,
		pub,
		cfg.StreamNamespace,
		cfg.Symbols,
		cfg.SignalDepth,
		logger,
		consumer.WithConsumerID(consumerID),
	)

	if err := cons.Start(ctx); err != nil {
		rootLogger.Fatal("consumer start failed", zap.Error(err))
	}

	healthSrv := startHealthServer(cfg.Health.Addr, cons, logger)

	rootLogger.Info("erebor-signals started",
		zap.Strings("symbols", cfg.Symbols),
		zap.String("namespace", cfg.StreamNamespace),
		zap.Int("signal_depth", cfg.SignalDepth),
		zap.String("health_addr", cfg.Health.Addr),
	)

	<-ctx.Done()
	rootLogger.Info("shutdown initiated")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// 1. Stop health probes.
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		rootLogger.Warn("health server shutdown error", zap.Error(err))
	}

	// 2. Wait for the consumer read loop to exit.
	// The loop observes ctx cancellation and returns on the next XREADGROUP timeout.
	cons.Stop()

	rootLogger.Info("shutdown complete")
}

func startHealthServer(addr string, cons *consumer.Consumer, logger *zap.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
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
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.With(zap.String("component", "health")).Error("health server failed", zap.Error(err))
		}
	}()
	return srv
}

func buildLogger(stderrLevel, fileLevel, filePath string) (*zap.Logger, func() error, error) {
	zcfg := zap.NewProductionEncoderConfig()
	zcfg.TimeKey = "ts"
	zcfg.MessageKey = "msg"
	zcfg.LevelKey = "level"
	zcfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder

	stderrLvl, err := parseLevel(stderrLevel, zapcore.InfoLevel)
	if err != nil {
		return nil, nil, fmt.Errorf("stderr log level: %w", err)
	}
	fileLvl, err := parseLevel(fileLevel, zapcore.DebugLevel)
	if err != nil {
		return nil, nil, fmt.Errorf("file log level: %w", err)
	}

	encoder := zapcore.NewJSONEncoder(zcfg)
	cores := []zapcore.Core{
		zapcore.NewCore(encoder, zapcore.Lock(os.Stderr), stderrLvl),
	}

	closer := func() error { return nil }
	if filePath != "" {
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("open log file %q: %w", filePath, err)
		}
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(f), fileLvl))
		closer = f.Close
	}

	logger := zap.New(zapcore.NewTee(cores...), zap.AddCaller())
	return logger, closer, nil
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
