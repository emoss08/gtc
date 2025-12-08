package meilisearch

import (
	"encoding/json"
	"os"
)

type Config struct {
	URL          string
	APIKey       string
	TableMapping map[string]string
	Enabled      bool
}

func LoadConfig() (Config, error) {
	cfg := Config{
		URL:          os.Getenv("MEILISEARCH_URL"),
		APIKey:       os.Getenv("MEILISEARCH_API_KEY"),
		TableMapping: make(map[string]string),
	}
	cfg.Enabled = cfg.URL != ""

	if mappingStr := os.Getenv("MEILISEARCH_TABLE_MAPPING"); mappingStr != "" {
		if err := json.Unmarshal([]byte(mappingStr), &cfg.TableMapping); err != nil {
			return cfg, err
		}
	}

	return cfg, nil
}
