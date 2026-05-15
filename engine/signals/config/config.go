package config

import "github.com/spf13/viper"

type Config struct {
	Symbols         []string `mapstructure:"symbols"`
	StreamNamespace string   `mapstructure:"stream_namespace"`
	SignalDepth     int      `mapstructure:"signal_depth"`
	Redis           struct {
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

func Load(path string) (Config, error) {
	var cfg Config
	v := viper.New()
	v.SetConfigFile(path)

	v.SetDefault("stream_namespace", "erebor:live")
	v.SetDefault("signal_depth", 10)
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("log.level", "info")
	v.SetDefault("health.addr", ":8080")

	// Allow env vars to override config file values.
	v.BindEnv("redis.addr", "REDIS_ADDR")         //nolint:errcheck
	v.BindEnv("redis.password", "REDIS_PASSWORD")  //nolint:errcheck
	v.BindEnv("stream_namespace", "STREAM_NAMESPACE") //nolint:errcheck

	if err := v.ReadInConfig(); err != nil {
		return cfg, err
	}
	if err := v.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
