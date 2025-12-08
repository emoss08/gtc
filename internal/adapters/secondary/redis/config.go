package redis

import (
	"os"
	"strconv"
)

type Config struct {
	URL          string
	StreamPrefix string
	MaxStreamLen int64
	Enabled      bool
}

func LoadConfig() Config {
	cfg := Config{
		URL:          os.Getenv("REDIS_URL"),
		StreamPrefix: getEnv("REDIS_STREAM_PREFIX", "cdc"),
		MaxStreamLen: getInt64("REDIS_MAX_STREAM_LEN", 10000),
	}
	cfg.Enabled = cfg.URL != ""
	return cfg
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}
