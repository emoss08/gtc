package dlq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// memStore is an in-memory Store for testing the manager and decorator.
type memStore struct {
	mu      sync.Mutex
	entries map[string]domain.DeadLetterEntry
	full    bool
}

func newMemStore() *memStore {
	return &memStore{entries: make(map[string]domain.DeadLetterEntry)}
}

func (s *memStore) Park(_ context.Context, entry domain.DeadLetterEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.full {
		return ErrFull
	}
	s.entries[entry.ID] = entry
	return nil
}

func (s *memStore) Get(_ context.Context, id string) (*domain.DeadLetterEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[id]
	if !ok {
		return nil, ErrNotFound
	}
	return &entry, nil
}

func (s *memStore) List(_ context.Context, limit int) ([]domain.DeadLetterEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.DeadLetterEntry
	for _, e := range s.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *memStore) Remove(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[id]; !ok {
		return ErrNotFound
	}
	delete(s.entries, id)
	return nil
}

func (s *memStore) Len(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.entries)), nil
}

// flakySink fails a configurable number of times, then succeeds.
type flakySink struct {
	name      string
	failures  int
	processed int
	err       error
}

func (s *flakySink) Name() string                      { return s.name }
func (s *flakySink) Initialize(context.Context) error  { return nil }
func (s *flakySink) Shutdown(context.Context) error    { return nil }
func (s *flakySink) HealthCheck(context.Context) error { return nil }
func (s *flakySink) Process(context.Context, domain.CDCEvent) error {
	s.processed++
	if s.failures > 0 {
		s.failures--
		if s.err != nil {
			return s.err
		}
		return fmt.Errorf("boom %d", s.processed)
	}
	return nil
}

func testEvent(id string) domain.CDCEvent {
	return domain.CDCEvent{
		ID:        id,
		Operation: domain.OperationUpdate,
		Schema:    "public",
		Table:     "users",
		NewData:   map[string]any{"id": 1},
		Metadata:  domain.EventMetadata{LSN: id},
	}
}

func newTestManager(store Store) *Manager {
	return NewManager(ManagerParams{
		Store:        store,
		Threshold:    3,
		RetryTimeout: time.Second,
		Logger:       discardLogger(),
	})
}

func TestParkAfterThresholdFailures(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := &flakySink{name: "meili", failures: 100}
	sink := m.WrapSink(inner)
	ctx := context.Background()

	// First two failures propagate (the stream would tear down and replay).
	for i := 0; i < 2; i++ {
		if err := sink.Process(ctx, testEvent("0/100")); err == nil {
			t.Fatalf("failure %d must propagate before the threshold", i+1)
		}
	}

	// Third failure parks: pipeline advances.
	if err := sink.Process(ctx, testEvent("0/100")); err != nil {
		t.Fatalf("threshold failure must park and return nil, got %v", err)
	}

	entries, _ := store.List(ctx, 0)
	if len(entries) != 1 {
		t.Fatalf("expected 1 parked entry, got %d", len(entries))
	}
	e := entries[0]
	if e.ID != "meili:0/100" || e.SinkName != "meili" || e.Attempts != 3 {
		t.Errorf("unexpected entry: %+v", e)
	}
	if e.Operation != "UPDATE" || e.Table != "users" {
		t.Errorf("entry missing display fields: %+v", e)
	}
}

func TestCircuitOpenNeverParks(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := &flakySink{name: "s", failures: 100, err: fmt.Errorf("wrapped: %w", domain.ErrCircuitOpen)}
	sink := m.WrapSink(inner)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := sink.Process(ctx, testEvent("0/100")); err == nil {
			t.Fatal("circuit-open failures must always propagate")
		}
	}
	if n, _ := store.Len(ctx); n != 0 {
		t.Fatalf("a down sink must not fill the DLQ, got %d entries", n)
	}
}

func TestSuccessClearsFailureCount(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := &flakySink{name: "s", failures: 2} // fails twice, then succeeds
	sink := m.WrapSink(inner)
	ctx := context.Background()

	_ = sink.Process(ctx, testEvent("0/100")) // fail 1
	_ = sink.Process(ctx, testEvent("0/100")) // fail 2
	if err := sink.Process(ctx, testEvent("0/100")); err != nil {
		t.Fatalf("third attempt should succeed: %v", err)
	}

	// The count was cleared: two fresh failures must NOT reach threshold.
	inner.failures = 2
	_ = sink.Process(ctx, testEvent("0/100"))
	if err := sink.Process(ctx, testEvent("0/100")); err == nil {
		t.Fatal("second failure after reset must propagate, not park")
	}
	if n, _ := store.Len(ctx); n != 0 {
		t.Fatal("nothing should be parked after count reset")
	}
}

func TestFullDLQKeepsStalling(t *testing.T) {
	store := newMemStore()
	store.full = true
	m := newTestManager(store)
	sink := m.WrapSink(&flakySink{name: "s", failures: 100})
	ctx := context.Background()

	var err error
	for i := 0; i < 3; i++ {
		err = sink.Process(ctx, testEvent("0/100"))
	}
	if err == nil {
		t.Fatal("a full DLQ must propagate the failure, never drop the event")
	}
	if !errors.Is(err, ErrFull) {
		t.Errorf("error should mention the full DLQ: %v", err)
	}
}

func TestRetrySuccessRemovesEntry(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := &flakySink{name: "s", failures: 3}
	sink := m.WrapSink(inner)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = sink.Process(ctx, testEvent("0/100"))
	}
	if n, _ := store.Len(ctx); n != 1 {
		t.Fatal("expected a parked entry")
	}

	// Sink recovered (failures exhausted): retry succeeds and removes.
	if err := m.Retry(ctx, "s:0/100"); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if n, _ := store.Len(ctx); n != 0 {
		t.Fatal("successful retry must remove the entry")
	}
}

func TestRetryFailureUpdatesEntry(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := &flakySink{name: "s", failures: 10}
	sink := m.WrapSink(inner)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = sink.Process(ctx, testEvent("0/100"))
	}

	if err := m.Retry(ctx, "s:0/100"); err == nil {
		t.Fatal("retry against a still-failing sink must return the error")
	}
	entry, err := store.Get(ctx, "s:0/100")
	if err != nil {
		t.Fatal("failed retry must keep the entry")
	}
	if entry.Attempts != 4 {
		t.Errorf("failed retry must bump attempts, got %d", entry.Attempts)
	}
}

func TestRetryAll(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	recovered := &flakySink{name: "a", failures: 3}
	broken := &flakySink{name: "b", failures: 1000}
	sinkA := m.WrapSink(recovered)
	sinkB := m.WrapSink(broken)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = sinkA.Process(ctx, testEvent("0/100"))
		_ = sinkB.Process(ctx, testEvent("0/200"))
	}

	result, err := m.RetryAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Retried != 2 || result.Succeeded != 1 || result.Failed != 1 {
		t.Errorf("unexpected result: %+v", result)
	}
	if n, _ := store.Len(ctx); n != 1 {
		t.Errorf("only the still-failing entry should remain, got %d", n)
	}
}

func TestDiscard(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	sink := m.WrapSink(&flakySink{name: "s", failures: 100})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = sink.Process(ctx, testEvent("0/100"))
	}
	if err := m.Discard(ctx, "s:0/100"); err != nil {
		t.Fatal(err)
	}
	if n, _ := store.Len(ctx); n != 0 {
		t.Fatal("discard must remove the entry")
	}
	if err := m.Discard(ctx, "s:0/100"); !errors.Is(err, ErrNotFound) {
		t.Errorf("discarding a missing entry must return ErrNotFound, got %v", err)
	}
}
