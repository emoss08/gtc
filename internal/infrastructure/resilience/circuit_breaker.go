package resilience

import (
	"context"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/sony/gobreaker/v2"
)

type ResilienceConfig struct {
	MaxRetries          int
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	CircuitOpenDuration time.Duration
	FailureThreshold    uint32
}

func DefaultResilienceConfig() ResilienceConfig {
	return ResilienceConfig{
		MaxRetries:          3,
		InitialBackoff:      100 * time.Millisecond,
		MaxBackoff:          5 * time.Second,
		CircuitOpenDuration: 30 * time.Second,
		FailureThreshold:    5,
	}
}

type ResilientSink struct {
	inner   domain.Sink
	breaker *gobreaker.CircuitBreaker[any]
	config  ResilienceConfig
}

func NewResilientSink(sink domain.Sink, cfg ResilienceConfig) *ResilientSink {
	settings := gobreaker.Settings{
		Name:    sink.Name(),
		Timeout: cfg.CircuitOpenDuration,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cfg.FailureThreshold
		},
	}

	return &ResilientSink{
		inner:   sink,
		breaker: gobreaker.NewCircuitBreaker[any](settings),
		config:  cfg,
	}
}

func (s *ResilientSink) Name() string {
	return s.inner.Name()
}

func (s *ResilientSink) Initialize(ctx context.Context) error {
	return s.inner.Initialize(ctx)
}

func (s *ResilientSink) Process(ctx context.Context, event domain.CDCEvent) error {
	_, err := s.breaker.Execute(func() (any, error) {
		return nil, s.processWithRetry(ctx, event)
	})
	return err
}

func (s *ResilientSink) processWithRetry(ctx context.Context, event domain.CDCEvent) error {
	backoff := s.config.InitialBackoff
	var lastErr error

	for i := 0; i <= s.config.MaxRetries; i++ {
		if err := s.inner.Process(ctx, event); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if i < s.config.MaxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, s.config.MaxBackoff)
		}
	}

	return lastErr
}

func (s *ResilientSink) Shutdown(ctx context.Context) error {
	return s.inner.Shutdown(ctx)
}

func (s *ResilientSink) HealthCheck(ctx context.Context) error {
	return s.inner.HealthCheck(ctx)
}
