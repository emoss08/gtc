package meilisearch

import (
	"os"
)

type Config struct {
	URL     string
	APIKey  string
	Enabled bool
}

func LoadConfig() Config {
	cfg := Config{
		URL:    os.Getenv("MEILISEARCH_URL"),
		APIKey: os.Getenv("MEILISEARCH_API_KEY"),
	}
	cfg.Enabled = cfg.URL != ""
	return cfg
}
