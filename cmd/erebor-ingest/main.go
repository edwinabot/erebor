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
	"strings"
	"syscall"
	"time"

	"github.com/edwinabot/erebor/ingest/book"
	"github.com/edwinabot/erebor/ingest/dispatch"
	"github.com/edwinabot/erebor/ingest/fetcher"
	"github.com/edwinabot/erebor/ingest/repository"
	"github.com/edwinabot/erebor/ingest/stream"
	"github.com/edwinabot/erebor/ingest/symbol"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type symbolConfig struct {
	Name                    string        `mapstructure:"name"`
	DepthLimit              int           `mapstructure:"depth_limit"`
	CheckpointInterval      time.Duration `mapstructure:"checkpoint_interval"`
	CheckpointDiffThreshold int           `mapstructure:"checkpoint_diff_threshold"`
	MaxBufferSize           int           `mapstructure:"max_buffer_size"`
}

type appConfig struct {
	Binance struct {
		WebSocketBaseURL string `mapstructure:"websocket_base_url"`
		RESTBaseURL      string `mapstructure:"rest_base_url"`
	} `mapstructure:"binance"`
	Symbols []symbolConfig `mapstructure:"symbols"`
	Log     struct {
		Level string `mapstructure:"level"`
	} `mapstructure:"log"`
	Health struct {
		Addr string `mapstructure:"addr"`
	} `mapstructure:"health"`
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	flag.Parse()

	if err := requireEnv("BINANCE_API_KEY", "BINANCE_API_SECRET", "DATABASE_DSN"); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(2)
	}

	logger, err := buildLogger(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build logger: %v\n", err)
		os.Exit(2)
	}
	defer func() { _ = logger.Sync() }()

	rootLogger := logger.With(zap.String("component", "main"))

	if len(cfg.Symbols) == 0 {
		rootLogger.Fatal("at least one symbol must be configured")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_DSN"))
	if err != nil {
		rootLogger.Fatal("pgx connect", zap.Error(err))
	}
	defer pool.Close()

	repo := repository.New(pool)
	df := fetcher.New(cfg.Binance.RESTBaseURL)

	handlers := make(map[string]symbol.SymbolHandler, len(cfg.Symbols))
	concrete := make([]*symbol.Handler, 0, len(cfg.Symbols))
	symbolNames := make([]string, 0, len(cfg.Symbols))
	for _, sc := range cfg.Symbols {
		name := strings.ToUpper(sc.Name)
		if name == "" {
			rootLogger.Fatal("symbol entry missing name")
		}
		ob := book.New(name)
		h := symbol.NewHandler(symbol.Config{
			Symbol:                  name,
			DepthLimit:              sc.DepthLimit,
			CheckpointInterval:      sc.CheckpointInterval,
			CheckpointDiffThreshold: sc.CheckpointDiffThreshold,
			MaxBufferSize:           sc.MaxBufferSize,
		}, ob, df, repo, logger)
		handlers[name] = h
		concrete = append(concrete, h)
		symbolNames = append(symbolNames, name)
	}

	healthAddr := cfg.Health.Addr
	if healthAddr == "" {
		healthAddr = ":8080"
	}
	healthSrv := startHealthServer(healthAddr, concrete, logger)

	sm := stream.New(stream.Config{
		BaseURL: cfg.Binance.WebSocketBaseURL,
		Symbols: symbolNames,
	}, logger)

	dp := dispatch.New(handlers, logger)

	for _, h := range concrete {
		h.Start(ctx)
	}

	if err := sm.Connect(ctx); err != nil {
		rootLogger.Fatal("stream connect", zap.Error(err))
	}

	rootLogger.Info("ingest service started",
		zap.Int("symbols", len(symbolNames)),
		zap.String("health_addr", healthAddr),
	)

	dispatchDone := make(chan struct{})
	go func() {
		dp.Run(ctx, sm.Events())
		close(dispatchDone)
	}()

	<-ctx.Done()
	rootLogger.Info("shutdown initiated")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// 1. Stop accepting new health probes.
	if err := healthSrv.Shutdown(shutdownCtx); err != nil {
		rootLogger.Warn("health server shutdown error", zap.Error(err))
	}

	// 2. Close the WebSocket stream — no new diffs will be produced. This
	//    closes the Events channel, which lets the dispatcher loop exit.
	if err := sm.Close(); err != nil {
		rootLogger.Warn("stream close error", zap.Error(err))
	}

	// 3. Wait for the dispatcher to finish processing whatever was already
	//    on the events channel. After this, no new HandleDiff calls will run.
	select {
	case <-dispatchDone:
	case <-shutdownCtx.Done():
		rootLogger.Warn("dispatcher did not drain before deadline")
	}

	// 4. Wait for any in-flight snapshot fetches to unwind. They observe
	//    ctx cancellation and return promptly.
	for _, h := range concrete {
		h.Stop()
	}

	rootLogger.Info("shutdown complete")
}

func requireEnv(keys ...string) error {
	var missing []string
	for _, k := range keys {
		if os.Getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}
	return nil
}

func loadConfig(path string) (appConfig, error) {
	var cfg appConfig
	v := viper.New()
	v.SetConfigFile(path)
	v.SetDefault("binance.websocket_base_url", "wss://stream.binance.com:9443")
	v.SetDefault("binance.rest_base_url", "https://api.binance.com")
	v.SetDefault("log.level", "info")
	v.SetDefault("health.addr", ":8080")
	if err := v.ReadInConfig(); err != nil {
		return cfg, err
	}
	if err := v.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func buildLogger(level string) (*zap.Logger, error) {
	zcfg := zap.NewProductionConfig()
	zcfg.EncoderConfig.TimeKey = "ts"
	zcfg.EncoderConfig.MessageKey = "msg"
	zcfg.EncoderConfig.LevelKey = "level"
	zcfg.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	if level != "" {
		var lvl zap.AtomicLevel
		if err := lvl.UnmarshalText([]byte(level)); err != nil {
			return nil, fmt.Errorf("invalid log level %q: %w", level, err)
		}
		zcfg.Level = lvl
	}
	return zcfg.Build()
}

func startHealthServer(addr string, handlers []*symbol.Handler, logger *zap.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		anySynced := false
		for _, h := range handlers {
			if h.State() == symbol.Synced {
				anySynced = true
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		var body map[string]string
		if anySynced {
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
