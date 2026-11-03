package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
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
	loaded     map[string]TableState // returned by LoadState; nil means empty
	persisted  map[string]TableState // returned by LoadTableState

	mu      sync.Mutex
	cursors [][]string // cursor passed to each SelectChunk
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
	_ context.Context, _, _ string, _, cursor []string, _ int,
) (*Chunk, error) {
	f.mu.Lock()
	f.cursors = append(f.cursors, cursor)
	f.mu.Unlock()
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
	if f.loaded != nil {
		return f.loaded, nil
	}
	return map[string]TableState{}, nil
}
func (f *fakeDB) LoadTableState(_ context.Context, table string) (TableState, bool, error) {
	ts, ok := f.persisted[table]
	return ts, ok, nil
}
func (f *fakeDB) SaveState(_ context.Context, _ string, state TableState) error {
	f.saved <- state
	return nil
}
func (f *fakeDB) MarkPending(context.Context, []string) error { return nil }
func (f *fakeDB) Close()                                      {}

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
	c.OnStreamStart()

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

	// Three rows is less than the chunk size: the table is complete
	// without another round trip.
	waitForState(t, c, domain.BackfillDone)
	assertNoWatermark(t, db)
	if got := rowsCopied(c); got != 2 {
		t.Errorf("rows copied = %d, want 2 (the superseded row is not counted)", got)
	}
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
	c.OnStreamStart()

	if err := c.EnqueueTable("public", "users"); err != nil {
		t.Fatal(err)
	}

	// First attempt: watermarks are emitted, then the stream dies.
	firstLow := waitWatermark(t, db, "low")
	waitWatermark(t, db, "high")
	c.OnStreamStop()

	// Nothing is retried until the stream is back.
	assertNoWatermark(t, db)
	c.OnStreamStart()

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

	waitForState(t, c, domain.BackfillDone)
}

func TestBackfillContinuesAfterFullChunk(t *testing.T) {
	db := newFakeDB([]string{"id"}, [][]Row{
		{row(1, "ada"), row(2, "bob")}, // == ChunkSize: more may follow
		{row(3, "cy")},
	})
	c := NewCoordinator(db, Config{SlotName: "slot1", ChunkSize: 2}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	c.OnStreamStart()
	if err := c.EnqueueTable("public", "users"); err != nil {
		t.Fatal(err)
	}

	playChunk(t, c, db, 2)
	if status := statusOf(t, c); status.State != domain.BackfillRunning {
		t.Fatalf("a full chunk must not complete the table, state = %s", status.State)
	}
	playChunk(t, c, db, 1)

	waitForState(t, c, domain.BackfillDone)
	if got := rowsCopied(c); got != 3 {
		t.Errorf("rows copied = %d, want 3", got)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.cursors) != 2 || len(db.cursors[1]) != 1 || db.cursors[1][0] != "2" {
		t.Errorf("second chunk must start after the first chunk's last key, cursors = %v", db.cursors)
	}
}

func TestBackfillWaitsForTheStream(t *testing.T) {
	db := newFakeDB([]string{"id"}, [][]Row{{row(1, "ada")}})
	c := NewCoordinator(db, Config{SlotName: "slot1", ChunkSize: 10}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	if err := c.EnqueueTable("public", "users"); err != nil {
		t.Fatal(err)
	}

	// A standby node does not own the stream: its watermarks would never
	// come back to it, so it must not emit any.
	assertNoWatermark(t, db)

	c.OnStreamStart()
	playChunk(t, c, db, 1)
	waitForState(t, c, domain.BackfillDone)
}

func TestBackfillSkipsTableCompletedByAnotherNode(t *testing.T) {
	db := newFakeDB([]string{"id"}, [][]Row{{row(1, "ada")}})
	finished := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	db.persisted = map[string]TableState{
		"public.users": {Done: true, RowsCopied: 42, UpdatedAt: finished},
	}
	db.loaded = map[string]TableState{"public.users": {Cursor: []string{"7"}, RowsCopied: 7}}
	c := NewCoordinator(db, Config{SlotName: "slot1", ChunkSize: 10}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx) // queues the table as interrupted, from the stale snapshot
	c.OnStreamStart()

	waitForState(t, c, domain.BackfillDone)
	assertNoWatermark(t, db)
	status := statusOf(t, c)
	if status.RowsCopied != 42 || status.CompletedAt == nil || !status.CompletedAt.Equal(finished) {
		t.Errorf("status must reflect the persisted completion, got %+v", status)
	}
}

func TestBackfillResumesFromPersistedCursor(t *testing.T) {
	db := newFakeDB([]string{"id"}, [][]Row{{row(9, "zed")}})
	db.loaded = map[string]TableState{"public.users": {Cursor: []string{"3"}, RowsCopied: 3}}
	// Another node got further than this process's snapshot.
	db.persisted = map[string]TableState{"public.users": {Cursor: []string{"8"}, RowsCopied: 8}}
	c := NewCoordinator(db, Config{SlotName: "slot1", ChunkSize: 10}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	c.OnStreamStart()

	playChunk(t, c, db, 1)
	waitForState(t, c, domain.BackfillDone)

	db.mu.Lock()
	cursors := db.cursors
	db.mu.Unlock()
	if len(cursors) != 1 || len(cursors[0]) != 1 || cursors[0][0] != "8" {
		t.Errorf("must resume from the persisted cursor, selects used %v", cursors)
	}
	if got := rowsCopied(c); got != 9 {
		t.Errorf("rows copied = %d, want 9 (8 before + 1)", got)
	}
}

func TestChunkIDsAreUniqueAcrossInstances(t *testing.T) {
	ids := make([]string, 2)
	for i := range ids {
		db := newFakeDB([]string{"id"}, [][]Row{{row(1, "ada")}})
		c := NewCoordinator(db, Config{SlotName: "shared", ChunkSize: 10}, slog.Default())
		ctx, cancel := context.WithCancel(context.Background())
		c.Start(ctx)
		c.OnStreamStart()
		if err := c.EnqueueTable("public", "users"); err != nil {
			t.Fatal(err)
		}
		ids[i] = waitWatermark(t, db, "low").Chunk
		cancel()
	}
	if ids[0] == ids[1] {
		t.Fatalf("two processes sharing a slot produced the same chunk id %q", ids[0])
	}
}

// playChunk plays the stream's part for one chunk of want rows.
func playChunk(t *testing.T, c *Coordinator, db *fakeDB, want int) {
	t.Helper()
	low := waitWatermark(t, db, "low")
	c.HandleWatermark("0/10", marshal(t, low))
	high := waitWatermark(t, db, "high")
	if events := c.HandleWatermark("0/20", marshal(t, high)); len(events) != want {
		t.Fatalf("chunk emitted %d events, want %d", len(events), want)
	}
	c.WatermarkDelivered(marshal(t, high))
}

func assertNoWatermark(t *testing.T, db *fakeDB) {
	t.Helper()
	select {
	case p := <-db.watermarks:
		t.Fatalf("unexpected %s watermark for chunk %s", p.Mark, p.Chunk)
	case <-time.After(100 * time.Millisecond):
	}
}

func statusOf(t *testing.T, c *Coordinator) domain.BackfillTableStatus {
	t.Helper()
	for _, s := range c.Status() {
		if s.Schema == "public" && s.Table == "users" {
			return s
		}
	}
	t.Fatalf("no status for public.users: %+v", c.Status())
	return domain.BackfillTableStatus{}
}

func rowsCopied(c *Coordinator) int64 {
	for _, s := range c.Status() {
		if s.Table == "users" {
			return s.RowsCopied
		}
	}
	return -1
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

func TestStartShowsBackfillsCompletedBeforeRestart(t *testing.T) {
	completedAt := time.Date(2026, 10, 5, 2, 19, 42, 0, time.UTC)
	db := newFakeDB([]string{"id"}, nil)
	db.loaded = map[string]TableState{
		"public.users": {Done: true, RowsCopied: 42, UpdatedAt: completedAt},
	}
	c := NewCoordinator(db, Config{SlotName: "slot1"}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	status := c.Status()
	if len(status) != 1 {
		t.Fatalf("want the completed table in status after a restart, got %+v", status)
	}
	got := status[0]
	if got.State != domain.BackfillDone || got.RowsCopied != 42 {
		t.Errorf("want done with 42 rows, got %s with %d", got.State, got.RowsCopied)
	}
	if got.CompletedAt == nil || !got.CompletedAt.Equal(completedAt) {
		t.Errorf("want completed_at %v, got %v", completedAt, got.CompletedAt)
	}
}

func TestReplayAllRequeuesCompletedTables(t *testing.T) {
	db := newFakeDB([]string{"id"}, nil)
	c := NewCoordinator(db, Config{SlotName: "slot1", PublicationName: "pub"}, slog.Default())
	c.state = map[string]TableState{"public.users": {Done: true, RowsCopied: 3}}

	// Resume semantics (startup): a completed table is not queued again.
	if err := c.EnqueueAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.queue) != 0 {
		t.Fatalf("EnqueueAll must skip completed tables, queued %v", c.queue)
	}

	queued, err := c.ReplayAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if queued != 1 || len(c.queue) != 1 {
		t.Fatalf("ReplayAll must queue the completed table, queued=%d queue=%v", queued, c.queue)
	}
	if _, ok := c.state["public.users"]; ok {
		t.Error("ReplayAll must reset the table's progress")
	}

	// A table already queued is not queued twice.
	if again, _ := c.ReplayAll(context.Background()); again != 0 {
		t.Errorf("want 0 tables queued while the replay is pending, got %d", again)
	}
}

func TestBackfillPersistsRowsCopied(t *testing.T) {
	// A full chunk (rows == ChunkSize) is followed by another, so its
	// progress is saved with a cursor before the table completes.
	db := newFakeDB([]string{"id"}, [][]Row{{row(1, "ada"), row(2, "bob")}})
	c := NewCoordinator(db, Config{SlotName: "slot1", ChunkSize: 2}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	c.OnStreamStart()
	if err := c.EnqueueTable("public", "users"); err != nil {
		t.Fatal(err)
	}

	low := waitWatermark(t, db, "low")
	c.HandleWatermark("0/1", marshal(t, low))
	high := waitWatermark(t, db, "high")
	c.HandleWatermark("0/2", marshal(t, high))
	c.WatermarkDelivered(marshal(t, high))

	select {
	case saved := <-db.saved:
		if saved.RowsCopied != 2 || len(saved.Cursor) == 0 {
			t.Errorf("want the chunk's progress saved with 2 rows copied, got %+v", saved)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("backfill never saved progress")
	}
}
