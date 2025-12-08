package redisjson

import (
	"os"
)

type Config struct {
	URL     string
	Prefix  string
	Enabled bool
}

func LoadConfig() Config {
	cfg := Config{
		URL:    os.Getenv("REDIS_JSON_URL"),
		Prefix: getEnv("REDIS_JSON_PREFIX", "cdc"),
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
