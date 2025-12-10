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
	ExcludedTables    map[string]struct{}
	HTTPPort          int
	Resilience        ResilienceConfig
	AutoCreateSlot    bool
	AutoCreatePub     bool
	SlotRetryInterval time.Duration
	SlotRetryTimeout  time.Duration
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
