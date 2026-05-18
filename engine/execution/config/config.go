package config

import "github.com/spf13/viper"

// Config holds all erebor-execution runtime configuration.
type Config struct {
	Symbols        []string `mapstructure:"symbols"`
	StrategyConfig string   `mapstructure:"strategy_config"`
	Redis          struct {
		Addr     string `mapstructure:"addr"`
		Password string `mapstructure:"password"`
	} `mapstructure:"redis"`
	Log struct {
		Level     string `mapstructure:"level"`
		FileLevel string `mapstructure:"file_level"`
		FilePath  string `mapstructure:"file_path"`
	} `mapstructure:"log"`
	Health struct {
		Addr string `mapstructure:"addr"`
	} `mapstructure:"health"`
}

// Load reads and validates the config file at path.
func Load(path string) (Config, error) {
	var cfg Config
	v := viper.New()
	v.SetConfigFile(path)

	v.SetDefault("strategy_config", "{}")
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.file_level", "debug")
	v.SetDefault("health.addr", ":8082")

	v.BindEnv("redis.addr", "REDIS_ADDR")         //nolint:errcheck
	v.BindEnv("redis.password", "REDIS_PASSWORD") //nolint:errcheck

	if err := v.ReadInConfig(); err != nil {
		return cfg, err
	}
	if err := v.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
