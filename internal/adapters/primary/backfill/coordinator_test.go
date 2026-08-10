package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
)

// fakeDB scripts chunk selects and records watermark emissions so the test
// can play the role of the WAL stream.
type fakeDB struct {
	pk         []string
	chunks     [][]Row // successive SelectChunk results; empty slice ends the table
	selects    int
	watermarks chan watermarkPayload
	saved      chan TableState
}

func newFakeDB(pk []string, chunks [][]Row) *fakeDB {
	return &fakeDB{
		pk:         pk,
		chunks:     chunks,
		watermarks: make(chan watermarkPayload, 16),
		saved:      make(chan TableState, 16),
	}
}

func (f *fakeDB) EmitWatermark(_ context.Context, payload []byte) error {
	var p watermarkPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	f.watermarks <- p
	return nil
}

func (f *fakeDB) PrimaryKey(context.Context, string, string) ([]string, error) {
	return f.pk, nil
}

func (f *fakeDB) SelectChunk(
	_ context.Context, _, _ string, _, _ []string, _ int,
) (*Chunk, error) {
	idx := f.selects
	f.selects++
	if idx >= len(f.chunks) {
		return &Chunk{}, nil
	}
	rows := f.chunks[idx]
	chunk := &Chunk{Rows: rows}
	if len(rows) > 0 {
		chunk.LastCursor = []string{rows[len(rows)-1].Key}
	}
	return chunk, nil
}

func (f *fakeDB) PublicationTables(context.Context, string) ([][2]string, error) {
	return [][2]string{{"public", "users"}}, nil
}

func (f *fakeDB) EnsureStateTable(context.Context) error { return nil }
func (f *fakeDB) LoadState(context.Context) (map[string]TableState, error) {
	return map[string]TableState{}, nil
}
func (f *fakeDB) SaveState(_ context.Context, _ string, cursor []string, done bool) error {
	f.saved <- TableState{Cursor: cursor, Done: done}
	return nil
}
func (f *fakeDB) ClearState(context.Context, string) error { return nil }
func (f *fakeDB) Close()                                   {}

func row(id int, name string) Row {
	return Row{
		Key:    fmt.Sprintf("%d", id),
		Values: map[string]any{"id": int64(id), "name": name},
	}
}

func waitWatermark(t *testing.T, db *fakeDB, mark string) watermarkPayload {
	t.Helper()
	select {
	case p := <-db.watermarks:
		if p.Mark != mark {
			t.Fatalf("expected %s watermark, got %s", mark, p.Mark)
		}
		return p
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s watermark", mark)
		return watermarkPayload{}
	}
}

func marshal(t *testing.T, p watermarkPayload) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func waitForState(t *testing.T, c *Coordinator, want domain.BackfillState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range c.Status() {
			if s.State == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("table never reached state %s: %+v", want, c.Status())
}

func TestBackfillDropsRowsSupersededByStream(t *testing.T) {
	db := newFakeDB([]string{"id"}, [][]Row{
		{row(1, "ada"), row(2, "bob"), row(3, "cy")},
	})
	c := NewCoordinator(db, Config{SlotName: "slot1", ChunkSize: 10}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	if err := c.EnqueueTable("public", "users"); err != nil {
		t.Fatal(err)
	}

	// Chunk 1: play the stream role.
	low := waitWatermark(t, db, "low")
	if evs := c.HandleWatermark("0/10", marshal(t, low)); len(evs) != 0 {
		t.Fatalf("low watermark must not emit events, got %d", len(evs))
	}

	// A live UPDATE for id=2 arrives between the watermarks: the stream
	// version is newer than the chunk's copy.
	c.ObserveEvent(domain.CDCEvent{
		Operation: domain.OperationUpdate,
		Schema:    "public", Table: "users",
		NewData: map[string]any{"id": int64(2), "name": "bob2"},
	})
	// An event for another table must not affect the chunk.
	c.ObserveEvent(domain.CDCEvent{
		Operation: domain.OperationUpdate,
		Schema:    "public", Table: "orders",
		NewData: map[string]any{"id": int64(1)},
	})

	high := waitWatermark(t, db, "high")
	events := c.HandleWatermark("0/20", marshal(t, high))
	if len(events) != 2 {
		t.Fatalf("expected 2 events (row 2 superseded), got %d", len(events))
	}
	for _, ev := range events {
		if ev.Operation != domain.OperationRead {
			t.Errorf("expected READ, got %s", ev.Operation)
		}
		if ev.NewData["id"] == int64(2) {
			t.Error("superseded row 2 must not be emitted")
		}
		if ev.Metadata.LSN != "0/20" {
			t.Errorf("expected high-watermark LSN, got %s", ev.Metadata.LSN)
		}
	}
	c.WatermarkDelivered(marshal(t, high))

	// Chunk 2 is empty: table completes.
	low2 := waitWatermark(t, db, "low")
	c.HandleWatermark("0/30", marshal(t, low2))
	high2 := waitWatermark(t, db, "high")
	if evs := c.HandleWatermark("0/40", marshal(t, high2)); len(evs) != 0 {
		t.Fatalf("empty chunk must emit no events, got %d", len(evs))
	}
	c.WatermarkDelivered(marshal(t, high2))

	waitForState(t, c, domain.BackfillDone)
}

func TestBackfillRetriesChunkOnStreamRestart(t *testing.T) {
	db := newFakeDB([]string{"id"}, [][]Row{
		{row(1, "ada")},
		{row(1, "ada")}, // retry selects again
	})
	c := NewCoordinator(db, Config{SlotName: "slot1", ChunkSize: 10}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	if err := c.EnqueueTable("public", "users"); err != nil {
		t.Fatal(err)
	}

	// First attempt: watermarks are emitted, then the stream dies.
	firstLow := waitWatermark(t, db, "low")
	waitWatermark(t, db, "high")
	c.OnStreamRestart()

	// Retry emits fresh watermarks with a new chunk ID.
	low := waitWatermark(t, db, "low")
	if low.Chunk == firstLow.Chunk {
		t.Fatal("retry must use a fresh chunk ID")
	}

	// A redelivered watermark from the dead attempt must be ignored.
	if evs := c.HandleWatermark("0/10", marshal(t, firstLow)); evs != nil {
		t.Fatal("stale watermark must be ignored")
	}

	c.HandleWatermark("0/10", marshal(t, low))
	high := waitWatermark(t, db, "high")
	events := c.HandleWatermark("0/20", marshal(t, high))
	if len(events) != 1 {
		t.Fatalf("expected 1 event on retry, got %d", len(events))
	}
	c.WatermarkDelivered(marshal(t, high))

	// Drain the final empty chunk.
	low2 := waitWatermark(t, db, "low")
	c.HandleWatermark("0/30", marshal(t, low2))
	high2 := waitWatermark(t, db, "high")
	c.HandleWatermark("0/40", marshal(t, high2))
	c.WatermarkDelivered(marshal(t, high2))

	waitForState(t, c, domain.BackfillDone)
}

func TestForeignSlotWatermarksAreIgnored(t *testing.T) {
	c := NewCoordinator(newFakeDB([]string{"id"}, nil), Config{SlotName: "mine"}, slog.Default())

	payload := marshal(t, watermarkPayload{Slot: "someone-elses", Chunk: "x", Mark: "high"})
	if evs := c.HandleWatermark("0/10", payload); evs != nil {
		t.Fatal("watermarks from other slots must be ignored")
	}
}

func TestEnqueueExcludedTableFails(t *testing.T) {
	c := NewCoordinator(newFakeDB([]string{"id"}, nil), Config{
		SlotName:       "slot1",
		ExcludedTables: map[string]struct{}{"public.secrets": {}},
	}, slog.Default())

	if err := c.EnqueueTable("public", "secrets"); err == nil {
		t.Fatal("enqueueing an excluded table must fail")
	}
}

func TestNonReplicationURL(t *testing.T) {
	got := NonReplicationURL("postgres://u:p@host:5432/db?replication=database&sslmode=disable")
	want := "postgres://u:p@host:5432/db?sslmode=disable"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}

	unchanged := "postgres://u:p@host:5432/db"
	if got := NonReplicationURL(unchanged); got != unchanged {
		t.Errorf("URL without replication param must be unchanged, got %q", got)
	}
}
