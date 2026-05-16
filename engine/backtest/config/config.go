package config

import (
	"fmt"
	"os"
)

// Config holds infrastructure connection parameters for erebor-backtest.
// Run parameters (symbols, time range, speed, strategy) are provided via CLI flags.
type Config struct {
	TimescaleDSN  string
	RedisAddr     string
	RedisPassword string
}

// Load reads infrastructure configuration from environment variables.
// TIMESCALE_DSN is required. REDIS_ADDR defaults to localhost:6379.
func Load() (Config, error) {
	dsn := os.Getenv("TIMESCALE_DSN")
	if dsn == "" {
		return Config{}, fmt.Errorf("TIMESCALE_DSN environment variable is required")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	return Config{
		TimescaleDSN:  dsn,
		RedisAddr:     addr,
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
	}, nil
}
