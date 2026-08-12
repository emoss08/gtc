package clickhouse

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Enabled bool
	// URL is a ClickHouse DSN (clickhouse://user:pass@host:9000/db). Setting
	// CLICKHOUSE_URL enables the sink.
	URL string
	// Database holds the mirrored tables; it overrides any database in the
	// DSN and is created when missing.
	Database string
	// TablePrefix prefixes generated table names, so several sources can
	// share one database.
	TablePrefix string
	// AutoCreateTables creates and extends target tables from the source
	// schema, which is what makes the sink plug-and-play.
	AutoCreateTables bool
	// AsyncInsert lets ClickHouse batch the many small inserts a CDC stream
	// produces, instead of one part per row.
	AsyncInsert bool
	// WaitForInsert waits for the async batch to be flushed before reporting
	// success. Turning it off trades GTC's at-least-once guarantee for
	// throughput: the WAL position advances past rows that are still only
	// buffered in the server.
	WaitForInsert bool
	Timeout       time.Duration
}

func LoadConfig() Config {
	cfg := Config{
		URL:              os.Getenv("CLICKHOUSE_URL"),
		Database:         getEnv("CLICKHOUSE_DATABASE", "gtc"),
		TablePrefix:      os.Getenv("CLICKHOUSE_TABLE_PREFIX"),
		AutoCreateTables: getBool("CLICKHOUSE_AUTO_CREATE_TABLES", true),
		AsyncInsert:      getBool("CLICKHOUSE_ASYNC_INSERT", true),
		WaitForInsert:    getBool("CLICKHOUSE_WAIT_FOR_INSERT", true),
		Timeout:          getDuration("CLICKHOUSE_TIMEOUT", 10*time.Second),
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
