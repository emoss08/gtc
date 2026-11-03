package config

import (
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
)

type Config struct {
	DatabaseURL       string
	SlotName          string
	PublicationName   string
	StandbyTimeout    time.Duration
	ParallelSinks     bool
	ProcessTimeout    time.Duration
	Batch             BatchConfig
	ExcludedTables    map[string]struct{}
	HTTPPort          int
	Resilience        ResilienceConfig
	AutoCreateSlot    bool
	AutoCreatePub     bool
	SlotRetryInterval time.Duration
	SlotRetryTimeout  time.Duration
	Backfill          BackfillConfig
	DLQ               DLQConfig
	// MaskHMACKey keys the hmac256 mask strategy in sink transforms. Empty
	// means the strategy is unavailable (configs using it fail at startup).
	MaskHMACKey string
	// SchemaEvents publishes detected DDL changes to the Redis stream
	// "<REDIS_STREAM_PREFIX>:schema". Detection, metrics, logging and the
	// /api/schema history are always on; this only controls publishing.
	SchemaEvents bool
	// Pprof exposes Go's profiler under /debug/pprof on the HTTP port.
	Pprof bool
}

// BatchConfig shapes the batches the WAL reader hands to the sinks.
type BatchConfig struct {
	// MaxEvents caps one batch. Backfill chunks larger than this are
	// delivered in several batches.
	MaxEvents int
	// Linger is the longest an event waits for its batch to fill when the
	// stream goes quiet; under load batches fill before it elapses.
	Linger time.Duration
}

func (c BatchConfig) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.MaxEvents, validation.Required, validation.Min(1)),
		validation.Field(&c.Linger, validation.Required, validation.Min(time.Millisecond)),
	)
}

type DLQConfig struct {
	// Enabled turns on dead-lettering (requires REDIS_URL; without it the
	// DLQ is silently unavailable and failures always stall the pipeline).
	Enabled bool
	// Threshold is how many full failure cycles (resilience retries plus
	// stream replay) the same event must go through before it is parked.
	Threshold int
	// MaxEntries caps the queue; when full, parking fails and the
	// pipeline stalls rather than dropping events.
	MaxEntries int64
}

type BackfillConfig struct {
	// Mode: "auto" backfills all published tables when the replication
	// slot is first created (and resumes interrupted backfills on start);
	// "manual" only backfills via the HTTP API; "off" disables backfill.
	Mode      string
	ChunkSize int
	// Lanes is how many tables backfill concurrently.
	Lanes      int
	ChunkDelay time.Duration
	// StateTable is the table used to persist per-table progress in the
	// source database. Empty disables persistence (backfills restart from
	// scratch after a crash).
	StateTable string
}

type ResilienceConfig struct {
	CircuitBreakerThreshold uint32
	CircuitBreakerTimeout   time.Duration
	MaxRetries              int
	RetryBackoffInitial     time.Duration
	RetryBackoffMax         time.Duration
}

func (c Config) Validate() error {
	return validation.ValidateStruct(
		&c,
		validation.Field(&c.DatabaseURL, validation.Required, is.URL),
		validation.Field(&c.SlotName, validation.Required),
		validation.Field(&c.PublicationName, validation.Required),
		validation.Field(&c.StandbyTimeout, validation.Required, validation.Min(time.Second)),
		validation.Field(&c.ProcessTimeout, validation.Required, validation.Min(time.Second)),
		validation.Field(
			&c.HTTPPort,
			validation.Required,
			validation.Min(1),
			validation.Max(65535),
		),
		validation.Field(&c.Resilience),
		validation.Field(&c.Backfill),
		validation.Field(&c.Batch),
	)
}

func (c BackfillConfig) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.Mode, validation.Required, validation.In("auto", "manual", "off")),
		validation.Field(&c.ChunkSize, validation.Min(1)),
		validation.Field(&c.Lanes, validation.Min(1)),
	)
}

func (c ResilienceConfig) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.CircuitBreakerThreshold, validation.Min(uint32(1))),
		validation.Field(&c.CircuitBreakerTimeout, validation.Min(time.Second)),
		validation.Field(&c.MaxRetries, validation.Min(0)),
		validation.Field(&c.RetryBackoffInitial, validation.Min(time.Millisecond)),
		validation.Field(&c.RetryBackoffMax, validation.Min(time.Millisecond)),
	)
}
