package services

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
)

type fakeSink struct {
	name       string
	processErr error
	processed  int
}

func (s *fakeSink) Name() string                      { return s.name }
func (s *fakeSink) Initialize(context.Context) error  { return nil }
func (s *fakeSink) Shutdown(context.Context) error    { return nil }
func (s *fakeSink) HealthCheck(context.Context) error { return nil }
func (s *fakeSink) Process(_ context.Context, events []domain.CDCEvent) error {
	s.processed += len(events)
	return s.processErr
}

type fakeWALReader struct{}

func (fakeWALReader) Start(context.Context, ports.WALBatchHandler) error { return nil }
func (fakeWALReader) Stop(context.Context) error                         { return nil }
func (fakeWALReader) CurrentLSN() string                                 { return "0/0" }

func newTestService(t *testing.T, cfg CDCServiceConfig, sinks ...domain.Sink) *cdcService {
	t.Helper()
	svc := NewCDCService(CDCServiceParams{
		WALReader: fakeWALReader{},
		Registry:  NewSinkRegistryWithSinks(RegistryParams{Sinks: sinks}),
		Logger:    slog.Default(),
		Config:    cfg,
	})
	return svc.(*cdcService)
}

func testEvent(schema, table string) domain.CDCEvent {
	return domain.CDCEvent{
		ID:        "0/1000",
		Operation: domain.OperationInsert,
		Schema:    schema,
		Table:     table,
	}
}

func TestHandleBatchPropagatesSinkFailure(t *testing.T) {
	failing := &fakeSink{name: "bad", processErr: errors.New("connection refused")}
	healthy := &fakeSink{name: "good"}
	svc := newTestService(t, CDCServiceConfig{}, failing, healthy)

	err := svc.handleBatch(context.Background(), []domain.CDCEvent{testEvent("public", "users")})
	if err == nil {
		t.Fatal("expected error when a sink fails; a nil return would confirm the LSN and lose the event")
	}
	if !errors.Is(err, domain.ErrSinkProcessFailed) {
		t.Errorf("expected ErrSinkProcessFailed, got %v", err)
	}
	if healthy.processed != 1 {
		t.Errorf("healthy sink should still have been attempted, processed=%d", healthy.processed)
	}
}

func TestHandleBatchSucceedsWhenAllSinksSucceed(t *testing.T) {
	sink := &fakeSink{name: "good"}
	svc := newTestService(t, CDCServiceConfig{}, sink)

	if err := svc.handleBatch(context.Background(), []domain.CDCEvent{testEvent("public", "users")}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sink.processed != 1 {
		t.Errorf("expected 1 processed event, got %d", sink.processed)
	}
}

func TestHandleBatchSkipsExcludedTables(t *testing.T) {
	sink := &fakeSink{name: "good"}
	svc := newTestService(t, CDCServiceConfig{
		ExcludedTables: map[string]struct{}{"public.migrations": {}, "audit_logs": {}},
	}, sink)

	for _, tc := range []struct{ schema, table string }{
		{"public", "migrations"}, // full-name match
		{"other", "audit_logs"},  // bare-name match
	} {
		if err := svc.handleBatch(context.Background(), []domain.CDCEvent{testEvent(tc.schema, tc.table)}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if sink.processed != 0 {
		t.Errorf("excluded tables must not reach sinks, processed=%d", sink.processed)
	}
}

func TestHandleBatchRejectsDuringShutdown(t *testing.T) {
	sink := &fakeSink{name: "good"}
	svc := newTestService(t, CDCServiceConfig{}, sink)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	err := svc.handleBatch(context.Background(), []domain.CDCEvent{testEvent("public", "users")})
	if !errors.Is(err, domain.ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown so the LSN is not confirmed, got %v", err)
	}
	if sink.processed != 0 {
		t.Errorf("no sink should run after shutdown, processed=%d", sink.processed)
	}
}
