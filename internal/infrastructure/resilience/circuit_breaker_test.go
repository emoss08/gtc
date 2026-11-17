package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
)

type failingSink struct {
	err       error
	healthErr error
	calls     int
}

func (s *failingSink) Name() string                      { return "failing" }
func (s *failingSink) Initialize(context.Context) error  { return nil }
func (s *failingSink) Shutdown(context.Context) error    { return nil }
func (s *failingSink) HealthCheck(context.Context) error { return s.healthErr }
func (s *failingSink) Process(context.Context, []domain.CDCEvent) error {
	s.calls++
	return s.err
}

func testConfig() ResilienceConfig {
	return ResilienceConfig{
		AttemptTimeout:      time.Second,
		MaxRetries:          2,
		InitialBackoff:      time.Millisecond,
		MaxBackoff:          time.Millisecond,
		CircuitOpenDuration: time.Minute,
		FailureThreshold:    100,
	}
}

var oneEvent = []domain.CDCEvent{{ID: "0/1"}}

func TestUnclassifiedFailureOfUnhealthySinkIsUnavailable(t *testing.T) {
	inner := &failingSink{err: errors.New("WRITE failed"), healthErr: errors.New("ping: refused")}
	err := NewResilientSink(inner, testConfig()).Process(context.Background(), oneEvent)

	if !domain.IsUnavailable(err) {
		t.Fatalf("a failure while the health check fails must be unavailable, got %v", err)
	}
	if inner.calls != 3 {
		t.Errorf("retries must run before the diagnosis, calls=%d", inner.calls)
	}
}

func TestUnclassifiedFailureOfHealthySinkStaysTransient(t *testing.T) {
	inner := &failingSink{err: errors.New("WRONGTYPE")}
	err := NewResilientSink(inner, testConfig()).Process(context.Background(), oneEvent)

	if err == nil || domain.IsUnavailable(err) || domain.IsPermanent(err) {
		t.Fatalf("a failure of a healthy sink must stay transient, got %v", err)
	}
}

func TestPermanentFailureIsNotDiagnosed(t *testing.T) {
	inner := &failingSink{err: domain.Permanent(errors.New("bad")), healthErr: errors.New("down")}
	err := NewResilientSink(inner, testConfig()).Process(context.Background(), oneEvent)

	if !domain.IsPermanent(err) || inner.calls != 1 {
		t.Fatalf("a permanent failure is returned at once, unchanged: %v (calls=%d)", err, inner.calls)
	}
}
