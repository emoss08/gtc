package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type SinksConfig struct {
	RedisStream RedisStreamSinkConfig `yaml:"redis_stream"`
	RedisJSON   RedisJSONSinkConfig   `yaml:"redis_json"`
	Meilisearch MeilisearchSinkConfig `yaml:"meilisearch"`
}

type RedisStreamSinkConfig struct {
	SyncAll           bool              `yaml:"sync_all"`
	DefaultKeyPattern string            `yaml:"default_key_pattern"`
	Tables            map[string]string `yaml:"tables"`
}

type RedisJSONSinkConfig struct {
	SyncAll           bool              `yaml:"sync_all"`
	DefaultKeyPattern string            `yaml:"default_key_pattern"`
	Tables            map[string]string `yaml:"tables"`
}

func (c *RedisStreamSinkConfig) ShouldProcess(schema, table string) bool {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if _, ok := c.Tables[fullName]; ok {
		return true
	}
	if _, ok := c.Tables[table]; ok {
		return true
	}

	return c.SyncAll
}

func (c *RedisJSONSinkConfig) ShouldProcess(schema, table string) bool {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if _, ok := c.Tables[fullName]; ok {
		return true
	}
	if _, ok := c.Tables[table]; ok {
		return true
	}

	return c.SyncAll
}

type MeilisearchSinkConfig struct {
	Tables map[string]string `yaml:"tables"`
}

func (c *RedisStreamSinkConfig) GetKeyPattern(schema, table string) string {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if pattern, ok := c.Tables[fullName]; ok {
		return pattern
	}

	if pattern, ok := c.Tables[table]; ok {
		return pattern
	}

	if c.DefaultKeyPattern != "" {
		return c.DefaultKeyPattern
	}

	return "{{.Prefix}}:{{.Schema}}:{{.Table}}"
}

func (c *RedisJSONSinkConfig) GetKeyPattern(schema, table string) string {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	if pattern, ok := c.Tables[fullName]; ok {
		return pattern
	}

	if pattern, ok := c.Tables[table]; ok {
		return pattern
	}

	if c.DefaultKeyPattern != "" {
		return c.DefaultKeyPattern
	}

	return "{{.Prefix}}:{{.Schema}}:{{.Table}}:{{.Field \"id\"}}"
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
	configPath := os.Getenv("SINK_CONFIG_FILE")
	if configPath == "" {
		return &SinksConfig{
			RedisStream: RedisStreamSinkConfig{
				Tables: make(map[string]string),
			},
			RedisJSON: RedisJSONSinkConfig{
				Tables: make(map[string]string),
			},
			Meilisearch: MeilisearchSinkConfig{
				Tables: make(map[string]string),
			},
		}, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read sinks config file: %w", err)
	}

	var cfg SinksConfig
	if unmarshalErr := yaml.Unmarshal(data, &cfg); unmarshalErr != nil {
		return nil, fmt.Errorf("parse sinks config: %w", unmarshalErr)
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

	return &cfg, nil
}
