package dlq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/emoss08/gtc/internal/core/domain"
)

func newTestStore(t *testing.T, maxEntries int64) Store {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := NewRedisStore("redis://"+mr.Addr(), "cdc", maxEntries)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func entry(id string, failedAt time.Time) domain.DeadLetterEntry {
	return domain.DeadLetterEntry{
		ID:           id,
		SinkName:     "meili",
		Schema:       "public",
		Table:        "users",
		Operation:    "UPDATE",
		Error:        "boom",
		Attempts:     3,
		LastFailedAt: failedAt,
		Event: domain.CDCEvent{
			ID:        id,
			Operation: domain.OperationUpdate,
			Schema:    "public",
			Table:     "users",
			NewData:   map[string]any{"id": float64(1)}, // JSON round-trip type
		},
	}
}

func TestRedisStoreRoundTrip(t *testing.T) {
	store := newTestStore(t, 100)
	ctx := context.Background()
	base := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	if err := store.Park(ctx, entry("meili:0/100", base)); err != nil {
		t.Fatal(err)
	}
	if err := store.Park(ctx, entry("meili:0/200", base.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}

	got, err := store.Get(ctx, "meili:0/100")
	if err != nil {
		t.Fatal(err)
	}
	if got.Event.Operation != domain.OperationUpdate {
		t.Errorf("operation must survive the JSON round-trip, got %v", got.Event.Operation)
	}
	if got.Event.NewData["id"] != float64(1) {
		t.Errorf("event data must round-trip: %v", got.Event.NewData)
	}

	// Newest first.
	entries, err := store.List(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].ID != "meili:0/200" {
		t.Errorf("expected newest-first listing, got %+v", entries)
	}

	if n, _ := store.Len(ctx); n != 2 {
		t.Errorf("expected 2 entries, got %d", n)
	}

	if err := store.Remove(ctx, "meili:0/100"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "meili:0/100"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after removal, got %v", err)
	}
	if err := store.Remove(ctx, "meili:0/100"); !errors.Is(err, ErrNotFound) {
		t.Errorf("removing a missing entry must return ErrNotFound, got %v", err)
	}
}

func TestRedisStoreCapacity(t *testing.T) {
	store := newTestStore(t, 2)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.Park(ctx, entry("a:1", now)); err != nil {
		t.Fatal(err)
	}
	if err := store.Park(ctx, entry("a:2", now)); err != nil {
		t.Fatal(err)
	}

	// A third distinct entry must be rejected.
	if err := store.Park(ctx, entry("a:3", now)); !errors.Is(err, ErrFull) {
		t.Fatalf("expected ErrFull, got %v", err)
	}

	// Re-parking an existing entry (replay overwrite) must still work.
	if err := store.Park(ctx, entry("a:1", now.Add(time.Second))); err != nil {
		t.Fatalf("overwriting an existing entry must be allowed at capacity: %v", err)
	}
}
