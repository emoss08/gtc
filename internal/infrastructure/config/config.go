package config

import "time"

type Config struct {
	DatabaseURL     string
	SlotName        string
	PublicationName string
	StandbyTimeout  time.Duration
	ParallelSinks   bool
	ProcessTimeout  time.Duration
	ExcludedTables  map[string]struct{}
}

type RedisConfig struct {
	URL          string
	StreamPrefix string
	MaxStreamLen int64
	Enabled      bool
}

type MeilisearchConfig struct {
	URL          string
	APIKey       string
	TableMapping map[string]string
	Enabled      bool
}
