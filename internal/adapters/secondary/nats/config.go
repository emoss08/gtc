package nats

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Enabled bool
	// URL is a NATS server URL (nats://host:4222). Setting NATS_URL enables
	// the sink. Comma-separated URLs form a cluster.
	URL string
	// SubjectPrefix prefixes generated subjects: <prefix>.<schema>.<table>.
	SubjectPrefix string
	// Credentials is a path to a NATS credentials (.creds) file.
	Credentials string
	Token       string
	// JetStream publishes with acknowledgement, so a delivery is only
	// reported successful once the server has persisted it. Core NATS is
	// fire-and-forget, which would silently drop events whenever no
	// subscriber is listening — at odds with at-least-once delivery.
	JetStream bool
	// Stream is the JetStream stream that captures this deployment's
	// subjects; AutoCreateStream creates it when missing, so a fresh NATS
	// server needs no manual setup.
	Stream           string
	AutoCreateStream bool
	Timeout          time.Duration
}

func LoadConfig() Config {
	cfg := Config{
		URL:              os.Getenv("NATS_URL"),
		SubjectPrefix:    getEnv("NATS_SUBJECT_PREFIX", "cdc"),
		Credentials:      os.Getenv("NATS_CREDENTIALS"),
		Token:            os.Getenv("NATS_TOKEN"),
		JetStream:        getBool("NATS_JETSTREAM", true),
		Stream:           getEnv("NATS_STREAM", "GTC"),
		AutoCreateStream: getBool("NATS_AUTO_CREATE_STREAM", true),
		Timeout:          getDuration("NATS_TIMEOUT", 5*time.Second),
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
