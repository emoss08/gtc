package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL: getEnv(
			"DATABASE_URL",
			"postgres://postgres:postgres@localhost:5432/postgres?replication=database",
		),
		SlotName:        getEnv("CDC_SLOT_NAME", "cdc_demo_slot"),
		PublicationName: getEnv("CDC_PUBLICATION_NAME", "cdc_demo_publication"),
		StandbyTimeout:  getDuration("CDC_STANDBY_TIMEOUT", 10*time.Second),
		ParallelSinks:   getBool("CDC_PARALLEL_SINKS", false),
		// Kept well below Postgres's default wal_sender_timeout (60s):
		// sinks run synchronously with the WAL stream, and a stalled sink
		// must not starve standby keepalives long enough to be disconnected.
		ProcessTimeout:    getDuration("CDC_PROCESS_TIMEOUT", 10*time.Second),
		ExcludedTables:    parseTableSet("CDC_EXCLUDED_TABLES"),
		HTTPPort:          getInt("HTTP_PORT", 8080),
		AutoCreateSlot:    getBool("CDC_AUTO_CREATE_SLOT", true),
		AutoCreatePub:     getBool("CDC_AUTO_CREATE_PUBLICATION", true),
		SlotRetryInterval: getDuration("CDC_SLOT_RETRY_INTERVAL", 5*time.Second),
		SlotRetryTimeout:  getDuration("CDC_SLOT_RETRY_TIMEOUT", 60*time.Second),
		Resilience: ResilienceConfig{
			CircuitBreakerThreshold: getUint32("CDC_CIRCUIT_BREAKER_THRESHOLD", 5),
			CircuitBreakerTimeout:   getDuration("CDC_CIRCUIT_BREAKER_TIMEOUT", 30*time.Second),
			MaxRetries:              getInt("CDC_MAX_RETRIES", 3),
			RetryBackoffInitial:     getDuration("CDC_RETRY_BACKOFF_INITIAL", 100*time.Millisecond),
			RetryBackoffMax:         getDuration("CDC_RETRY_BACKOFF_MAX", 10*time.Second),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
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

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// The typed getters fall back to the default on unparsable input instead of
// silently returning the type's zero value.

func getBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
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

func getInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getUint32(key string, defaultVal uint32) uint32 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseUint(val, 10, 32); err == nil {
			return uint32(i)
		}
	}
	return defaultVal
}
