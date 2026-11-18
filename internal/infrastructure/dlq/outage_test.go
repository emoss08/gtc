package dlq

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/emoss08/gtc/internal/core/domain"
)

// scriptedSink is a sink that can be switched down (every call fails as
// unavailable) and that rejects a set of event IDs for their data. It records
// every event it applied, in order.
type scriptedSink struct {
	name string

	mu      sync.Mutex
	down    bool
	rejects map[string]bool
	applied []string
	calls   int
}

func newScriptedSink(name string) *scriptedSink {
	return &scriptedSink{name: name, rejects: make(map[string]bool)}
}

func (s *scriptedSink) Name() string                      { return s.name }
func (s *scriptedSink) Initialize(context.Context) error  { return nil }
func (s *scriptedSink) Shutdown(context.Context) error    { return nil }
func (s *scriptedSink) HealthCheck(context.Context) error { return nil }

func (s *scriptedSink) Process(_ context.Context, events []domain.CDCEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.down {
		return domain.Unavailable(errors.New("dial tcp: connection refused"))
	}
	for _, e := range events {
		if s.rejects[e.ID] {
			return domain.Permanent(errors.New("document rejected: " + e.ID))
		}
	}
	for _, e := range events {
		s.applied = append(s.applied, e.ID)
	}
	return nil
}

func (s *scriptedSink) appliedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.applied)
}

func events(ids ...string) []domain.CDCEvent {
	out := make([]domain.CDCEvent, len(ids))
	for i, id := range ids {
		out[i] = testEvent(id)
	}
	return out
}

func TestUnavailableSinkNeverParksPastThreshold(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := newScriptedSink("meilisearch")
	inner.down = true
	sink := m.WrapSink(inner)
	ctx := context.Background()

	// Far more failure cycles than CDC_DLQ_THRESHOLD, on single-event and
	// multi-event batches: the old code parked the first event at cycle 3.
	for i := 0; i < 10; i++ {
		for _, batch := range [][]domain.CDCEvent{events("0/100"), events("0/100", "0/101", "0/102")} {
			err := sink.Process(ctx, batch)
			if !domain.IsUnavailable(err) {
				t.Fatalf("cycle %d: a down sink must propagate an unavailable error, got %v", i+1, err)
			}
		}
	}
	if n, _ := store.Len(ctx); n != 0 {
		t.Fatalf("a down sink must not park anything, got %d entries", n)
	}

	m.mu.Lock()
	failures := len(m.failures)
	m.mu.Unlock()
	if failures != 0 {
		t.Errorf("an outage must not count toward the threshold, %d failure records", failures)
	}
}

func TestRejectedChangeParkedAndLaterChangesDelivered(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := newScriptedSink("meilisearch")
	inner.rejects["0/102"] = true
	sink := m.WrapSink(inner)
	ctx := context.Background()

	if err := sink.Process(ctx, events("0/100", "0/101", "0/102", "0/103")); err != nil {
		t.Fatalf("a rejected change must be parked, not stall the batch: %v", err)
	}
	if got, want := inner.appliedIDs(), []string{"0/100", "0/101", "0/103"}; !slices.Equal(got, want) {
		t.Fatalf("applied %v, want %v (in order, without the rejected change)", got, want)
	}
	if _, err := store.Get(ctx, "meilisearch:0/102"); err != nil {
		t.Fatalf("the rejected change must be parked: %v", err)
	}
	if n, _ := store.Len(ctx); n != 1 {
		t.Fatalf("only the rejected change is parked, got %d entries", n)
	}
}

// downAfter takes the sink down once it has answered the given number of
// calls.
type downAfter struct {
	*scriptedSink
	after int
}

func (d *downAfter) Process(ctx context.Context, events []domain.CDCEvent) error {
	err := d.scriptedSink.Process(ctx, events)
	d.mu.Lock()
	if d.calls >= d.after {
		d.down = true
	}
	d.mu.Unlock()
	return err
}

func TestOutageDuringIsolationStallsWithoutParking(t *testing.T) {
	store := newMemStore()
	m := newTestManager(store)
	inner := newScriptedSink("meilisearch")
	inner.rejects["0/100"] = true
	// The whole batch is rejected; the sink then goes down before the
	// halves are tried.
	sink := m.WrapSink(&downAfter{scriptedSink: inner, after: 1})
	ctx := context.Background()

	err := sink.Process(ctx, events("0/100", "0/101"))
	if !domain.IsUnavailable(err) {
		t.Fatalf("isolation must stop at an outage, got %v", err)
	}
	if n, _ := store.Len(ctx); n != 0 {
		t.Fatalf("nothing may be parked while the sink is down, got %d", n)
	}
}
