package webhook

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Enabled bool
	// URL receives a POST per event. Setting WEBHOOK_URL enables the sink.
	URL string
	// SigningSecret keys an HMAC-SHA256 signature over the request body so
	// receivers can verify the payload really came from this deployment.
	SigningSecret string
	// AuthHeader, when set, is sent verbatim as the Authorization header.
	AuthHeader string
	Timeout    time.Duration
	// MaxIdleConns bounds the connection pool for a single destination.
	MaxIdleConns int
}

func LoadConfig() Config {
	cfg := Config{
		URL:           os.Getenv("WEBHOOK_URL"),
		SigningSecret: os.Getenv("WEBHOOK_SIGNING_SECRET"),
		AuthHeader:    os.Getenv("WEBHOOK_AUTH_HEADER"),
		// Kept below the default CDC_PROCESS_TIMEOUT so a hung receiver is
		// cut off by the sink rather than by the pipeline's own deadline.
		Timeout:      getDuration("WEBHOOK_TIMEOUT", 5*time.Second),
		MaxIdleConns: getInt("WEBHOOK_MAX_IDLE_CONNS", 10),
	}
	cfg.Enabled = cfg.URL != ""
	return cfg
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
