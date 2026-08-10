package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type SinksConfig struct {
	RedisStream RedisKeyedSinkConfig  `yaml:"redis_stream"`
	RedisJSON   RedisKeyedSinkConfig  `yaml:"redis_json"`
	Meilisearch MeilisearchSinkConfig `yaml:"meilisearch"`
}

// RedisKeyedSinkConfig configures a Redis-backed sink whose keys are built
// from a per-table key pattern. It is shared by the stream and JSON sinks.
type RedisKeyedSinkConfig struct {
	SyncAll           bool              `yaml:"sync_all"`
	DefaultKeyPattern string            `yaml:"default_key_pattern"`
	Tables            map[string]string `yaml:"tables"`
}

func (c *RedisKeyedSinkConfig) ShouldProcess(schema, table string) bool {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if _, ok := c.Tables[fullName]; ok {
		return true
	}
	if _, ok := c.Tables[table]; ok {
		return true
	}

	return c.SyncAll
}

// GetKeyPattern returns the configured pattern for the table, or "" when
// nothing is configured; the sink's key resolver applies its own default.
func (c *RedisKeyedSinkConfig) GetKeyPattern(schema, table string) string {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if pattern, ok := c.Tables[fullName]; ok {
		return pattern
	}

	if pattern, ok := c.Tables[table]; ok {
		return pattern
	}

	return c.DefaultKeyPattern
}

type MeilisearchSinkConfig struct {
	Tables map[string]string `yaml:"tables"`
}

func (c *MeilisearchSinkConfig) GetIndex(schema, table string) (string, bool) {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if index, ok := c.Tables[fullName]; ok {
		return index, true
	}

	if index, ok := c.Tables[table]; ok {
		return index, true
	}

	return "", false
}

func LoadSinksConfig() (*SinksConfig, error) {
	cfg := &SinksConfig{}

	configPath := os.Getenv("SINK_CONFIG_FILE")
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read sinks config file: %w", err)
		}
		if unmarshalErr := yaml.Unmarshal(data, cfg); unmarshalErr != nil {
			return nil, fmt.Errorf("parse sinks config: %w", unmarshalErr)
		}
	}

	if cfg.RedisStream.Tables == nil {
		cfg.RedisStream.Tables = make(map[string]string)
	}
	if cfg.RedisJSON.Tables == nil {
		cfg.RedisJSON.Tables = make(map[string]string)
	}
	if cfg.Meilisearch.Tables == nil {
		cfg.Meilisearch.Tables = make(map[string]string)
	}

	// Plug-and-play default: with no sink config file, mirror every table.
	if configPath == "" {
		cfg.RedisStream.SyncAll = true
		cfg.RedisJSON.SyncAll = true
	}

	return cfg, nil
}
