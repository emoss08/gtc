package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

// Coordinator implements lock-free, resumable backfill using the DBLog
// watermark algorithm: each chunk of existing rows is selected between a low
// and a high watermark written into the WAL via pg_logical_emit_message. Live
// events observed between the watermarks supersede matching chunk rows, and
// the remainder is emitted into the sink pipeline exactly at the high
// watermark's stream position — so per-key ordering holds and streaming never
// pauses.
type Coordinator struct {
	db     DB
	logger *slog.Logger
	cfg    Config

	mu      sync.Mutex
	queue   []tableRef
	state   map[string]TableState
	status  map[string]*domain.BackfillTableStatus
	current *chunkInFlight

	wake      chan struct{}
	chunkDone chan struct{}
	restart   chan struct{}
	chunkSeq  int
}

type Config struct {
	SlotName        string
	PublicationName string
	ChunkSize       int
	ChunkDelay      time.Duration
	ExcludedTables  map[string]struct{}
}

type tableRef struct {
	Schema, Table string
}

func (t tableRef) String() string { return t.Schema + "." + t.Table }

type chunkInFlight struct {
	table      tableRef
	id         string
	collecting bool
	// seen collects primary keys observed on the live stream after the low
	// watermark; pending rows with these keys are stale and dropped.
	seen    map[string]struct{}
	pending []Row
	pkCols  []string
	lsn     string
}

type watermarkPayload struct {
	Slot  string `json:"slot"`
	Chunk string `json:"chunk"`
	Mark  string `json:"mark"` // "low" | "high"
}

var (
	_ ports.BackfillController = (*Coordinator)(nil)
	_ ports.BackfillManager    = (*Coordinator)(nil)
)

func NewCoordinator(db DB, cfg Config, logger *slog.Logger) *Coordinator {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 1000
	}
	return &Coordinator{
		db:        db,
		logger:    logger.With(slog.String("component", "backfill")),
		cfg:       cfg,
		state:     make(map[string]TableState),
		status:    make(map[string]*domain.BackfillTableStatus),
		wake:      make(chan struct{}, 1),
		chunkDone: make(chan struct{}, 1),
		restart:   make(chan struct{}, 1),
	}
}

// Start loads persisted progress, re-enqueues interrupted tables, and runs
// the chunking worker until ctx is cancelled.
func (c *Coordinator) Start(ctx context.Context) {
	if err := c.db.EnsureStateTable(ctx); err != nil {
		c.logger.Warn("backfill state table unavailable, progress will not survive restarts",
			slog.String("error", err.Error()))
	} else if state, err := c.db.LoadState(ctx); err != nil {
		c.logger.Warn("failed to load backfill state", slog.String("error", err.Error()))
	} else {
		c.mu.Lock()
		c.state = state
		for name, ts := range state {
			if !ts.Done {
				if ref, ok := splitTableName(name); ok {
					c.logger.Info("resuming interrupted backfill", slog.String("table", name))
					c.enqueueLocked(ref)
				}
			}
		}
		c.mu.Unlock()
	}

	go c.worker(ctx)
}

func (c *Coordinator) Close() {
	c.db.Close()
}

// --- ports.BackfillManager ---

func (c *Coordinator) EnqueueTable(schema, table string) error {
	ref := tableRef{Schema: schema, Table: table}
	if c.isExcluded(ref) {
		return fmt.Errorf("table %s is excluded from CDC", ref)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.current != nil && c.current.table == ref {
		return fmt.Errorf("backfill for %s is already running", ref)
	}
	for _, queued := range c.queue {
		if queued == ref {
			return fmt.Errorf("backfill for %s is already queued", ref)
		}
	}

	// Re-triggering a table is the replay primitive: start over from the
	// beginning regardless of prior completion.
	delete(c.state, ref.String())
	go func() {
		if err := c.db.ClearState(context.Background(), ref.String()); err != nil {
			c.logger.Warn("failed to clear backfill state", slog.String("error", err.Error()))
		}
	}()

	c.enqueueLocked(ref)
	return nil
}

func (c *Coordinator) EnqueueAll(ctx context.Context) error {
	tables, err := c.db.PublicationTables(ctx, c.cfg.PublicationName)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	enqueued := 0
	for _, t := range tables {
		ref := tableRef{Schema: t[0], Table: t[1]}
		if c.isExcluded(ref) {
			continue
		}
		if ts, ok := c.state[ref.String()]; ok && ts.Done {
			continue
		}
		if c.current != nil && c.current.table == ref {
			continue
		}
		alreadyQueued := false
		for _, queued := range c.queue {
			if queued == ref {
				alreadyQueued = true
				break
			}
		}
		if !alreadyQueued {
			c.enqueueLocked(ref)
			enqueued++
		}
	}

	c.logger.Info("backfill enqueued", slog.Int("tables", enqueued))
	return nil
}

func (c *Coordinator) Status() []domain.BackfillTableStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]domain.BackfillTableStatus, 0, len(c.status))
	for _, s := range c.status {
		out = append(out, *s)
	}
	return out
}

// --- ports.BackfillController (called from the WAL streaming goroutine) ---

func (c *Coordinator) ObserveEvent(event domain.CDCEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cur := c.current
	if cur == nil || !cur.collecting {
		return
	}
	if event.Schema != cur.table.Schema || event.Table != cur.table.Table {
		return
	}

	data := event.NewData
	if event.Operation == domain.OperationDelete {
		data = event.OldData
	}
	if data == nil {
		return
	}

	key, ok := pkKeyFromData(data, cur.pkCols)
	if !ok {
		return
	}
	cur.seen[key] = struct{}{}
}

func (c *Coordinator) HandleWatermark(lsn string, content []byte) []domain.CDCEvent {
	var payload watermarkPayload
	if err := json.Unmarshal(content, &payload); err != nil || payload.Slot != c.cfg.SlotName {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cur := c.current
	if cur == nil || cur.id != payload.Chunk {
		// Watermark from an abandoned chunk attempt (stream replay after
		// reconnect); the coordinator has already moved on.
		return nil
	}

	switch payload.Mark {
	case "low":
		cur.collecting = true
		return nil
	case "high":
		cur.collecting = false
		cur.lsn = lsn

		events := make([]domain.CDCEvent, 0, len(cur.pending))
		for _, row := range cur.pending {
			if _, superseded := cur.seen[row.Key]; superseded {
				continue
			}
			events = append(events, domain.CDCEvent{
				ID:        fmt.Sprintf("bf:%s:%s", cur.id, row.Key),
				Operation: domain.OperationRead,
				Schema:    cur.table.Schema,
				Table:     cur.table.Table,
				NewData:   row.Values,
				Metadata: domain.EventMetadata{
					LSN:       lsn,
					Timestamp: time.Now().UTC(),
				},
			})
		}
		return events
	}
	return nil
}

func (c *Coordinator) WatermarkDelivered(content []byte) {
	var payload watermarkPayload
	if err := json.Unmarshal(content, &payload); err != nil || payload.Mark != "high" {
		return
	}

	c.mu.Lock()
	match := c.current != nil && c.current.id == payload.Chunk
	c.mu.Unlock()

	if match {
		signal(c.chunkDone)
	}
}

func (c *Coordinator) OnStreamRestart() {
	c.mu.Lock()
	inFlight := c.current != nil
	c.mu.Unlock()

	if inFlight {
		signal(c.restart)
	}
}

// --- worker ---

func (c *Coordinator) worker(ctx context.Context) {
	for {
		ref, ok := c.pop()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-c.wake:
				continue
			}
		}

		if err := c.backfillTable(ctx, ref); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("backfill failed",
				slog.String("table", ref.String()),
				slog.String("error", err.Error()),
			)
			c.setStatus(ref, func(s *domain.BackfillTableStatus) {
				s.State = domain.BackfillFailed
				s.Error = err.Error()
			})
		}
	}
}

func (c *Coordinator) backfillTable(ctx context.Context, ref tableRef) error {
	pkCols, err := c.db.PrimaryKey(ctx, ref.Schema, ref.Table)
	if err != nil {
		return err
	}
	if len(pkCols) == 0 {
		c.logger.Warn("skipping backfill, table has no primary key",
			slog.String("table", ref.String()))
		c.setStatus(ref, func(s *domain.BackfillTableStatus) {
			s.State = domain.BackfillSkipped
			s.Error = "no primary key"
		})
		return nil
	}

	c.mu.Lock()
	cursor := c.state[ref.String()].Cursor
	c.mu.Unlock()

	now := time.Now().UTC()
	c.setStatus(ref, func(s *domain.BackfillTableStatus) {
		s.State = domain.BackfillRunning
		if s.StartedAt == nil {
			s.StartedAt = &now
		}
	})
	c.logger.Info("backfill started",
		slog.String("table", ref.String()),
		slog.Any("pk", pkCols),
		slog.Bool("resuming", len(cursor) > 0),
	)

	for {
		delivered, nextCursor, rowCount, err := c.runChunk(ctx, ref, pkCols, cursor)
		if err != nil {
			return err
		}

		if rowCount == 0 {
			c.mu.Lock()
			c.state[ref.String()] = TableState{Done: true}
			c.mu.Unlock()
			if err := c.db.SaveState(ctx, ref.String(), nil, true); err != nil {
				c.logger.Warn("failed to persist backfill completion", slog.String("error", err.Error()))
			}
			done := time.Now().UTC()
			c.setStatus(ref, func(s *domain.BackfillTableStatus) {
				s.State = domain.BackfillDone
				s.CompletedAt = &done
				s.Error = ""
			})
			c.logger.Info("backfill complete", slog.String("table", ref.String()))
			return nil
		}

		cursor = nextCursor
		c.mu.Lock()
		c.state[ref.String()] = TableState{Cursor: cursor}
		c.mu.Unlock()
		if err := c.db.SaveState(ctx, ref.String(), cursor, false); err != nil {
			c.logger.Warn("failed to persist backfill progress", slog.String("error", err.Error()))
		}

		metrics.BackfillRowsTotal.WithLabelValues(ref.Schema, ref.Table).Add(float64(delivered))
		c.setStatus(ref, func(s *domain.BackfillTableStatus) {
			s.RowsCopied += int64(delivered)
		})

		if c.cfg.ChunkDelay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.ChunkDelay):
			}
		}
	}
}

// runChunk executes one watermarked chunk, retrying on stream restarts. It
// returns the number of rows delivered to sinks, the cursor for the next
// chunk, and the number of rows selected (0 means the table is exhausted).
func (c *Coordinator) runChunk(
	ctx context.Context,
	ref tableRef,
	pkCols, cursor []string,
) (delivered int, nextCursor []string, rowCount int, err error) {
	for {
		c.mu.Lock()
		c.chunkSeq++
		chunk := &chunkInFlight{
			table:  ref,
			id:     fmt.Sprintf("%s-%d", ref.String(), c.chunkSeq),
			seen:   make(map[string]struct{}),
			pkCols: pkCols,
		}
		c.current = chunk
		drain(c.chunkDone)
		drain(c.restart)
		c.mu.Unlock()

		clearCurrent := func() {
			c.mu.Lock()
			c.current = nil
			c.mu.Unlock()
		}

		// DBLog ordering: low watermark, then the chunk select, then the
		// high watermark. Any event streamed between the two watermarks
		// for a selected key supersedes the selected row.
		if err := c.emitWatermark(ctx, chunk.id, "low"); err != nil {
			clearCurrent()
			return 0, nil, 0, err
		}

		selected, err := c.db.SelectChunk(ctx, ref.Schema, ref.Table, pkCols, cursor, c.cfg.ChunkSize)
		if err != nil {
			clearCurrent()
			return 0, nil, 0, err
		}

		c.mu.Lock()
		chunk.pending = selected.Rows
		c.mu.Unlock()

		if err := c.emitWatermark(ctx, chunk.id, "high"); err != nil {
			clearCurrent()
			return 0, nil, 0, err
		}

		select {
		case <-ctx.Done():
			clearCurrent()
			return 0, nil, 0, ctx.Err()

		case <-c.restart:
			// The replication stream went down mid-chunk; whatever was
			// partially delivered is covered by idempotent re-emission.
			clearCurrent()
			c.logger.Warn("stream restarted mid-chunk, retrying",
				slog.String("table", ref.String()),
				slog.String("chunk", chunk.id),
			)
			select {
			case <-ctx.Done():
				return 0, nil, 0, ctx.Err()
			case <-time.After(time.Second):
			}
			continue

		case <-c.chunkDone:
			c.mu.Lock()
			deliveredRows := len(chunk.pending) - len(intersect(chunk.pending, chunk.seen))
			c.current = nil
			c.mu.Unlock()
			return deliveredRows, selected.LastCursor, len(selected.Rows), nil
		}
	}
}

func (c *Coordinator) emitWatermark(ctx context.Context, chunkID, mark string) error {
	payload, err := json.Marshal(watermarkPayload{
		Slot:  c.cfg.SlotName,
		Chunk: chunkID,
		Mark:  mark,
	})
	if err != nil {
		return err
	}
	if err := c.db.EmitWatermark(ctx, payload); err != nil {
		return fmt.Errorf("emit %s watermark: %w", mark, err)
	}
	return nil
}

// --- helpers ---

func (c *Coordinator) enqueueLocked(ref tableRef) {
	c.queue = append(c.queue, ref)
	if _, ok := c.status[ref.String()]; !ok || c.status[ref.String()].State != domain.BackfillRunning {
		c.status[ref.String()] = &domain.BackfillTableStatus{
			Schema: ref.Schema,
			Table:  ref.Table,
			State:  domain.BackfillPending,
		}
	}
	signal(c.wake)
}

func (c *Coordinator) pop() (tableRef, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return tableRef{}, false
	}
	ref := c.queue[0]
	c.queue = c.queue[1:]
	return ref, true
}

func (c *Coordinator) setStatus(ref tableRef, update func(*domain.BackfillTableStatus)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.status[ref.String()]
	if !ok {
		s = &domain.BackfillTableStatus{Schema: ref.Schema, Table: ref.Table}
		c.status[ref.String()] = s
	}
	update(s)
}

func (c *Coordinator) isExcluded(ref tableRef) bool {
	if _, ok := c.cfg.ExcludedTables[ref.String()]; ok {
		return true
	}
	_, ok := c.cfg.ExcludedTables[ref.Table]
	return ok
}

func pkKeyFromData(data map[string]any, pkCols []string) (string, bool) {
	parts := make([]string, len(pkCols))
	for i, col := range pkCols {
		val, ok := data[col]
		if !ok {
			return "", false
		}
		parts[i] = fmt.Sprintf("%v", val)
	}
	key := parts[0]
	for _, p := range parts[1:] {
		key += "\x1f" + p
	}
	return key, true
}

func splitTableName(name string) (tableRef, bool) {
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			return tableRef{Schema: name[:i], Table: name[i+1:]}, true
		}
	}
	return tableRef{}, false
}

func intersect(rows []Row, seen map[string]struct{}) []Row {
	var out []Row
	for _, r := range rows {
		if _, ok := seen[r.Key]; ok {
			out = append(out, r)
		}
	}
	return out
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func drain(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
}
