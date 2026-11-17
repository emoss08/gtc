package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/dlq"
	"github.com/emoss08/gtc/internal/infrastructure/resilience"
)

// outageSink stands in for a destination that can be restarted: while down,
// every request fails the way a refused connection does and its health check
// fails. It can also reject chosen events for their data. It records what it
// applied, in order.
type outageSink struct {
	name string

	mu       sync.Mutex
	down     bool
	rejects  map[string]bool
	applied  []string
	attempts int
}

func newOutageSink(name string) *outageSink {
	return &outageSink{name: name, rejects: make(map[string]bool)}
}

func (s *outageSink) Name() string                     { return s.name }
func (s *outageSink) Initialize(context.Context) error { return nil }
func (s *outageSink) Shutdown(context.Context) error   { return nil }

func refused() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
}

func (s *outageSink) HealthCheck(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return refused()
	}
	return nil
}

func (s *outageSink) Process(_ context.Context, events []domain.CDCEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.down {
		return fmt.Errorf("%s add documents: %w", s.name, refused())
	}
	for _, e := range events {
		if s.rejects[e.ID] {
			return domain.Permanent(fmt.Errorf("document %s rejected: invalid_document_id", e.ID))
		}
	}
	for _, e := range events {
		s.applied = append(s.applied, e.ID)
	}
	return nil
}

func (s *outageSink) setDown(down bool) {
	s.mu.Lock()
	s.down = down
	s.mu.Unlock()
}

func (s *outageSink) setRejects(ids ...string) {
	s.mu.Lock()
	s.rejects = make(map[string]bool)
	for _, id := range ids {
		s.rejects[id] = true
	}
	s.mu.Unlock()
}

func (s *outageSink) appliedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.applied)
}

func (s *outageSink) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// pipeline is the production layering — dead letters over resilience over
// the sink — under a CDC service, with a dead-letter queue in an in-memory
// Redis and every delay shrunk to milliseconds.
type pipeline struct {
	svc    *cdcService
	store  dlq.Store
	search *outageSink
	cache  *outageSink
}

func newPipeline(t *testing.T) *pipeline {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := dlq.NewRedisStore("redis://"+mr.Addr(), "test", 100)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := dlq.NewManager(dlq.ManagerParams{
		Store:        store,
		Threshold:    3,
		RetryTimeout: time.Second,
		Logger:       logger,
	})
	resCfg := resilience.ResilienceConfig{
		AttemptTimeout:      time.Second,
		MaxRetries:          1,
		InitialBackoff:      time.Millisecond,
		MaxBackoff:          2 * time.Millisecond,
		CircuitOpenDuration: 10 * time.Millisecond,
		FailureThreshold:    5,
	}
	wrap := func(s domain.Sink) domain.Sink {
		return manager.WrapSink(resilience.NewResilientSink(s, resCfg))
	}

	search := newOutageSink("meilisearch")
	cache := newOutageSink("redis-json")
	svc := NewCDCService(CDCServiceParams{
		WALReader: fakeWALReader{},
		Registry:  NewSinkRegistryWithSinks(RegistryParams{Sinks: []domain.Sink{wrap(search), wrap(cache)}}),
		Logger:    logger,
		Config: CDCServiceConfig{
			ParallelSinks:       true,
			StallBackoffInitial: time.Millisecond,
			StallBackoffMax:     5 * time.Millisecond,
		},
	}).(*cdcService)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = svc.Stop(ctx)
	})

	return &pipeline{svc: svc, store: store, search: search, cache: cache}
}

func batch(ids ...string) []domain.CDCEvent {
	out := make([]domain.CDCEvent, len(ids))
	for i, id := range ids {
		out[i] = domain.CDCEvent{
			ID:        id,
			Operation: domain.OperationUpdate,
			Schema:    "public",
			Table:     "shipments",
			NewData:   map[string]any{"id": id},
			Metadata:  domain.EventMetadata{LSN: id},
		}
	}
	return out
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (p *pipeline) parked(t *testing.T) int64 {
	t.Helper()
	n, err := p.store.Len(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSearchOutageLongerThanThresholdLosesNothing(t *testing.T) {
	p := newPipeline(t)
	ctx := context.Background()

	p.search.setDown(true)
	result := make(chan error, 1)
	go func() { result <- p.svc.handleBatch(ctx, batch("0/100")) }()

	// Many times more failed deliveries than the old dead-letter threshold
	// (3 cycles) and the circuit breaker's (5), with the breaker opening
	// and half-opening along the way.
	eventually(t, "the outage to outlast the thresholds", func() bool {
		return p.search.attemptCount() >= 30
	})
	stalls := p.svc.Stalls()
	if len(stalls) != 1 || stalls[0].Sink != "meilisearch" || stalls[0].Since.IsZero() {
		t.Fatalf("the stalled sink must be reported, got %+v", stalls)
	}
	if n := p.parked(t); n != 0 {
		t.Fatalf("a down sink must never park a change, got %d dead letters", n)
	}
	select {
	case err := <-result:
		t.Fatalf("the batch must be held while the sink is down, returned %v", err)
	default:
	}

	p.search.setDown(false)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("the held batch must be delivered once the sink is back: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the held batch was not delivered after the sink came back")
	}

	// The changes that arrived meanwhile were kept by the slot and follow.
	for _, b := range [][]domain.CDCEvent{batch("0/101", "0/102"), batch("0/103")} {
		if err := p.svc.handleBatch(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	want := []string{"0/100", "0/101", "0/102", "0/103"}
	if got := p.search.appliedIDs(); !slices.Equal(got, want) {
		t.Errorf("search applied %v, want every change in order %v", got, want)
	}
	if got := p.cache.appliedIDs(); !slices.Equal(got, want) {
		t.Errorf("cache applied %v, want %v once each: the held batch is not re-sent to sinks that took it", got, want)
	}
	if n := p.parked(t); n != 0 {
		t.Errorf("the dead-letter queue must be empty, got %d", n)
	}
	if stalls := p.svc.Stalls(); len(stalls) != 0 {
		t.Errorf("the stall must clear once the sink takes the batch, got %+v", stalls)
	}
}

func TestRejectedChangeIsParkedAndDoesNotBlockLaterChanges(t *testing.T) {
	p := newPipeline(t)
	ctx := context.Background()
	p.search.setRejects("0/201")

	for _, b := range [][]domain.CDCEvent{batch("0/200", "0/201", "0/202"), batch("0/203")} {
		if err := p.svc.handleBatch(ctx, b); err != nil {
			t.Fatalf("a rejected change must not stall the stream: %v", err)
		}
	}

	if got, want := p.search.appliedIDs(), []string{"0/200", "0/202", "0/203"}; !slices.Equal(got, want) {
		t.Errorf("search applied %v, want %v", got, want)
	}
	if got, want := p.cache.appliedIDs(), []string{"0/200", "0/201", "0/202", "0/203"}; !slices.Equal(got, want) {
		t.Errorf("cache applied %v, want %v", got, want)
	}
	if _, err := p.store.Get(ctx, "meilisearch:0/201"); err != nil {
		t.Fatalf("the rejected change must be parked: %v", err)
	}
	if n := p.parked(t); n != 1 {
		t.Errorf("only the rejected change is parked, got %d", n)
	}
	if stalls := p.svc.Stalls(); len(stalls) != 0 {
		t.Errorf("a rejection is not a stall, got %+v", stalls)
	}
}

func TestHoldStopsOnShutdown(t *testing.T) {
	p := newPipeline(t)
	p.search.setDown(true)

	result := make(chan error, 1)
	go func() { result <- p.svc.handleBatch(context.Background(), batch("0/400")) }()
	eventually(t, "the stall", func() bool { return len(p.svc.Stalls()) == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.svc.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	err := <-result
	if err == nil || !errors.Is(err, domain.ErrSinkProcessFailed) {
		t.Fatalf("a held batch must fail on shutdown so its position is not confirmed, got %v", err)
	}
	if n := p.parked(t); n != 0 {
		t.Errorf("shutdown must not park anything, got %d", n)
	}
}

func TestHoldResumesAtTheFailedPiece(t *testing.T) {
	sink := &limitedSink{outageSink: newOutageSink("webhook"), failAt: 2}
	svc := newTestService(t, CDCServiceConfig{
		StallBackoffInitial: time.Millisecond,
		StallBackoffMax:     time.Millisecond,
	}, sink)

	if err := svc.handleBatch(context.Background(), batch("0/1", "0/2", "0/3")); err != nil {
		t.Fatal(err)
	}
	if got, want := sink.appliedIDs(), []string{"0/1", "0/2", "0/3"}; !slices.Equal(got, want) {
		t.Errorf("applied %v, want %v: pieces already delivered are not posted again", got, want)
	}
}

// limitedSink delivers one event per call and is unavailable for the first
// failAt calls that reach event 0/2.
type limitedSink struct {
	*outageSink
	failAt int
}

func (s *limitedSink) MaxBatchEvents() int { return 1 }

func (s *limitedSink) Process(ctx context.Context, events []domain.CDCEvent) error {
	if events[0].ID == "0/2" && s.failAt > 0 {
		s.failAt--
		return domain.Unavailable(errors.New("503 service unavailable"))
	}
	return s.outageSink.Process(ctx, events)
}
