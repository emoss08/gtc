package config

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"
)

func Load() (*Config, error) {
	return &Config{
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/trenova_go_db?replication=database"),
		SlotName:        getEnv("CDC_SLOT_NAME", "cdc_demo_slot"),
		PublicationName: getEnv("CDC_PUBLICATION_NAME", "cdc_demo_publication"),
		StandbyTimeout:  getDuration("CDC_STANDBY_TIMEOUT", 10*time.Second),
		ParallelSinks:   getBool("CDC_PARALLEL_SINKS", false),
		ProcessTimeout:  getDuration("CDC_PROCESS_TIMEOUT", 30*time.Second),
		ExcludedTables:  parseTableSet("CDC_EXCLUDED_TABLES"),
	}, nil
}

func parseTableSet(key string) map[string]struct{} {
	result := make(map[string]struct{})
	val := os.Getenv(key)
	if val == "" {
		return result
	}

	for _, table := range strings.Split(val, ",") {
		table = strings.TrimSpace(table)
		if table != "" {
			result[table] = struct{}{}
		}
	}

	return result
}

func LoadRedisConfig() RedisConfig {
	cfg := RedisConfig{
		URL:          os.Getenv("REDIS_URL"),
		StreamPrefix: getEnv("REDIS_STREAM_PREFIX", "cdc"),
		MaxStreamLen: getInt64("REDIS_MAX_STREAM_LEN", 10000),
	}
	cfg.Enabled = cfg.URL != ""
	return cfg
}

func LoadMeilisearchConfig() (MeilisearchConfig, error) {
	cfg := MeilisearchConfig{
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

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		b, _ := strconv.ParseBool(val)
		return b
	}
	return defaultVal
}

func getDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
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
