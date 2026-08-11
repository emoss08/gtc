package schema

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/emoss08/gtc/internal/core/domain"
)

func testRecorder() *Recorder {
	return NewRecorder(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func change(table string, kinds ...string) domain.SchemaChange {
	return domain.SchemaChange{Schema: "public", Table: table, Kinds: kinds}
}

func TestRecorderReturnsNewestFirst(t *testing.T) {
	r := testRecorder()
	for _, name := range []string{"a", "b", "c"} {
		r.OnSchemaChange(context.Background(), change(name, domain.SchemaChangeColumnAdded))
	}

	history := r.History()
	if len(history) != 3 {
		t.Fatalf("history length = %d, want 3", len(history))
	}
	for i, want := range []string{"c", "b", "a"} {
		if history[i].Table != want {
			t.Errorf("history[%d] = %s, want %s", i, history[i].Table, want)
		}
	}
}

func TestRecorderCapsHistory(t *testing.T) {
	r := testRecorder()
	for i := range historyCapacity + 25 {
		r.OnSchemaChange(context.Background(),
			change(fmt.Sprintf("t%d", i), domain.SchemaChangeColumnAdded))
	}

	history := r.History()
	if len(history) != historyCapacity {
		t.Fatalf("history length = %d, want %d", len(history), historyCapacity)
	}
	// The oldest entries are evicted, so the newest must still be first.
	if want := fmt.Sprintf("t%d", historyCapacity+24); history[0].Table != want {
		t.Errorf("newest = %s, want %s", history[0].Table, want)
	}
}

// History must hand out a copy: a caller mutating the result (or a concurrent
// append) must not corrupt the recorder's own buffer.
func TestHistoryIsACopy(t *testing.T) {
	r := testRecorder()
	r.OnSchemaChange(context.Background(), change("users", domain.SchemaChangeColumnAdded))

	history := r.History()
	history[0].Table = "mutated"

	if again := r.History(); again[0].Table != "users" {
		t.Errorf("recorder state was mutated through History(): %s", again[0].Table)
	}
}

func TestObserversFanOut(t *testing.T) {
	first, second := testRecorder(), testRecorder()
	observers := Observers{first, second}

	observers.OnSchemaChange(context.Background(), change("users", domain.SchemaChangeColumnAdded))

	if len(first.History()) != 1 || len(second.History()) != 1 {
		t.Errorf("fan-out missed an observer: %d, %d",
			len(first.History()), len(second.History()))
	}
}
