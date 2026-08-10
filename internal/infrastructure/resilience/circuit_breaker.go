package resilience

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
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

// ResilientSink wraps a Sink with retry and circuit-breaker behavior. When
// the circuit is open, Process fails fast with domain.ErrCircuitOpen instead
// of hammering an unhealthy sink; the failure still propagates so the WAL
// position is not confirmed and the event is redelivered later.
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
		OnStateChange: func(name string, _, to gobreaker.State) {
			metrics.CircuitBreakerState.WithLabelValues(name).Set(breakerStateValue(to))
		},
	}

	return &ResilientSink{
		inner:   sink,
		breaker: gobreaker.NewCircuitBreaker[any](settings),
		config:  cfg,
	}
}

func breakerStateValue(s gobreaker.State) float64 {
	switch s {
	case gobreaker.StateClosed:
		return 0
	case gobreaker.StateHalfOpen:
		return 1
	default:
		return 2
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
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return fmt.Errorf("%w: %s", domain.ErrCircuitOpen, err)
	}
	return err
}

func (s *ResilientSink) processWithRetry(ctx context.Context, event domain.CDCEvent) error {
	backoff := s.config.InitialBackoff
	var lastErr error

	for i := 0; i <= s.config.MaxRetries; i++ {
		lastErr = s.inner.Process(ctx, event)
		if lastErr == nil {
			return nil
		}

		// Retrying after the context is done cannot succeed; surface the
		// underlying failure immediately.
		if ctx.Err() != nil || i == s.config.MaxRetries {
			return lastErr
		}

		metrics.RetryAttempts.WithLabelValues(s.inner.Name()).Inc()
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, s.config.MaxBackoff)
	}

	return lastErr
}

func (s *ResilientSink) Shutdown(ctx context.Context) error {
	return s.inner.Shutdown(ctx)
}

func (s *ResilientSink) HealthCheck(ctx context.Context) error {
	return s.inner.HealthCheck(ctx)
}
