package backfill

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
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
//
// Several tables run at once (Config.Lanes), and within a table up to
// pipelineDepth chunks are in flight: the next chunk is selected while the
// stream is still delivering the previous one. Chunks of one table never
// overlap in the WAL (a chunk's low watermark follows the previous chunk's
// high watermark), and progress is persisted only for delivered chunks, in
// order.
type Coordinator struct {
	db     DB
	logger *slog.Logger
	cfg    Config

	mu       sync.Mutex
	queue    []tableRef
	running  map[tableRef]bool
	state    map[string]TableState
	status   map[string]*domain.BackfillTableStatus
	inflight map[string]*chunkInFlight // by chunk id

	wake     chan struct{}
	chunkSeq int
	// streaming is true while this process consumes the replication
	// stream; chunks only run then. streamUp is closed (and replaced) when
	// it starts.
	streaming bool
	streamUp  chan struct{}
	// restart is closed (and replaced) when in-flight chunks must be
	// abandoned: the stream went down, or came up with chunks emitted while
	// it was down.
	restart chan struct{}
	// instance tags this process's chunk ids. Several nodes may share a
	// slot (one streams, the others stand by) and see each other's
	// watermarks in the WAL; a bare per-process sequence would collide.
	instance string
}

const (
	// DefaultChunkSize is the number of rows selected per watermarked chunk.
	DefaultChunkSize = 5000
	// DefaultLanes is how many tables backfill concurrently.
	DefaultLanes = 4
	// pipelineDepth is how many chunks of one table may be in flight.
	pipelineDepth = 2
)

type Config struct {
	SlotName        string
	PublicationName string
	ChunkSize       int
	ChunkDelay      time.Duration
	// Lanes is how many tables backfill concurrently.
	Lanes          int
	ExcludedTables map[string]struct{}
	// Wanted reports whether any sink consumes backfilled rows of the table;
	// tables no sink wants are not backfilled. Nil wants every table.
	Wanted func(schema, table string) bool
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

	rowCount   int      // rows selected
	lastCursor []string // cursor after this chunk
	delivered  int      // rows emitted at the high watermark
	done       chan struct{}
	restart    <-chan struct{}
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
		cfg.ChunkSize = DefaultChunkSize
	}
	if cfg.Lanes <= 0 {
		cfg.Lanes = DefaultLanes
	}
	return &Coordinator{
		db:       db,
		logger:   logger.With(slog.String("component", "backfill")),
		cfg:      cfg,
		running:  make(map[tableRef]bool),
		state:    make(map[string]TableState),
		status:   make(map[string]*domain.BackfillTableStatus),
		inflight: make(map[string]*chunkInFlight),
		wake:     make(chan struct{}, cfg.Lanes),
		streamUp: make(chan struct{}),
		restart:  make(chan struct{}),
		instance: newInstanceID(),
	}
}

// Start loads persisted progress, re-enqueues interrupted tables, and runs
// the backfill lanes until ctx is cancelled.
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
			ref, ok := splitTableName(name)
			if !ok {
				continue
			}
			if ts.Done {
				// Show backfills finished by earlier runs, so status does not
				// read empty after every restart.
				completed := ts.UpdatedAt.UTC()
				c.status[name] = &domain.BackfillTableStatus{
					Schema:      ref.Schema,
					Table:       ref.Table,
					State:       domain.BackfillDone,
					RowsCopied:  ts.RowsCopied,
					CompletedAt: &completed,
				}
				continue
			}
			if c.isExcluded(ref) || !c.wanted(ref) {
				continue
			}
			c.logger.Info("resuming interrupted backfill", slog.String("table", name))
			c.enqueueLocked(ref)
			c.status[name].RowsCopied = ts.RowsCopied
		}
		c.mu.Unlock()
	}

	for range c.cfg.Lanes {
		go c.lane(ctx)
	}
}

func (c *Coordinator) Close() {
	c.db.Close()
}

func (c *Coordinator) wanted(ref tableRef) bool {
	return c.cfg.Wanted == nil || c.cfg.Wanted(ref.Schema, ref.Table)
}

// --- ports.BackfillManager ---

func (c *Coordinator) EnqueueTable(schema, table string) error {
	ref := tableRef{Schema: schema, Table: table}
	if c.isExcluded(ref) {
		return fmt.Errorf("table %s is excluded from CDC", ref)
	}
	if !c.wanted(ref) {
		return fmt.Errorf("no sink consumes rows of %s, nothing to backfill", ref)
	}
	c.mu.Lock()
	busy := c.busyLocked(ref)
	c.mu.Unlock()
	if busy {
		return fmt.Errorf("backfill for %s is already running or queued", ref)
	}

	// Re-triggering a table is the replay primitive: start over from the
	// beginning regardless of prior completion. The table is persisted as
	// pending before it is queued: resetting afterwards could erase the
	// completion a small table records within milliseconds.
	if err := c.db.MarkPending(context.Background(), []string{ref.String()}); err != nil {
		c.logger.Warn("failed to persist pending backfill", slog.String("error", err.Error()))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busyLocked(ref) {
		return fmt.Errorf("backfill for %s is already running or queued", ref)
	}
	delete(c.state, ref.String())
	c.enqueueLocked(ref)
	return nil
}

// ReplayAll re-backfills every published table from the beginning, including
// tables that already completed. Tables already running or queued are left as
// they are. It returns how many tables were queued.
func (c *Coordinator) ReplayAll(ctx context.Context) (int, error) {
	tables, err := c.db.PublicationTables(ctx, c.cfg.PublicationName)
	if err != nil {
		return 0, err
	}

	replay := make([]tableRef, 0, len(tables))
	c.mu.Lock()
	for _, t := range tables {
		ref := tableRef{Schema: t[0], Table: t[1]}
		if c.isExcluded(ref) || !c.wanted(ref) || c.busyLocked(ref) {
			continue
		}
		replay = append(replay, ref)
	}
	c.mu.Unlock()

	if err := c.db.MarkPending(ctx, tableNames(replay)); err != nil {
		c.logger.Warn("failed to persist pending backfills", slog.String("error", err.Error()))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	queued := 0
	for _, ref := range replay {
		if c.busyLocked(ref) {
			continue
		}
		delete(c.state, ref.String())
		c.enqueueLocked(ref)
		queued++
	}

	c.logger.Info("backfill replay enqueued", slog.Int("tables", queued))
	return queued, nil
}

// busyLocked reports whether the table is running or queued. Callers hold c.mu.
func (c *Coordinator) busyLocked(ref tableRef) bool {
	return c.running[ref] || slices.Contains(c.queue, ref)
}

func (c *Coordinator) EnqueueAll(ctx context.Context) error {
	tables, err := c.db.PublicationTables(ctx, c.cfg.PublicationName)
	if err != nil {
		return err
	}

	candidates := make([]tableRef, 0, len(tables))
	c.mu.Lock()
	for _, t := range tables {
		ref := tableRef{Schema: t[0], Table: t[1]}
		if c.isExcluded(ref) || !c.wanted(ref) || c.busyLocked(ref) {
			continue
		}
		if ts, ok := c.state[ref.String()]; ok && ts.Done {
			continue
		}
		candidates = append(candidates, ref)
	}
	c.mu.Unlock()

	// Persist the whole queue first: a restart before a table's first chunk
	// must still resume it, or its rows would never be backfilled.
	if err := c.db.MarkPending(ctx, tableNames(candidates)); err != nil {
		c.logger.Warn("failed to persist pending backfills; a restart may lose the queue",
			slog.String("error", err.Error()))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	enqueued := 0
	for _, ref := range candidates {
		if c.busyLocked(ref) {
			continue
		}
		c.enqueueLocked(ref)
		enqueued++
	}

	c.logger.Info("backfill enqueued", slog.Int("tables", enqueued))
	return nil
}

func tableNames(refs []tableRef) []string {
	names := make([]string, len(refs))
	for i, ref := range refs {
		names[i] = ref.String()
	}
	return names
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

	if len(c.inflight) == 0 {
		return
	}
	var key string
	var keyed bool
	for _, ch := range c.inflight {
		if !ch.collecting || event.Schema != ch.table.Schema || event.Table != ch.table.Table {
			continue
		}
		if !keyed {
			data := event.NewData
			if event.Operation == domain.OperationDelete {
				data = event.OldData
			}
			if data == nil {
				return
			}
			var ok bool
			if key, ok = pkKeyFromData(data, ch.pkCols); !ok {
				return
			}
			keyed = true
		}
		ch.seen[key] = struct{}{}
	}
}

func (c *Coordinator) HandleWatermark(lsn string, content []byte) []domain.CDCEvent {
	var payload watermarkPayload
	if err := json.Unmarshal(content, &payload); err != nil || payload.Slot != c.cfg.SlotName {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	ch := c.inflight[payload.Chunk]
	c.logger.Debug("watermark",
		slog.String("chunk", payload.Chunk), slog.String("mark", payload.Mark),
		slog.Bool("known", ch != nil), slog.Int("inflight", len(c.inflight)))
	if ch == nil {
		// Another node's chunk, or an abandoned attempt replayed after a
		// reconnect.
		return nil
	}

	switch payload.Mark {
	case "low":
		ch.collecting = true
		return nil
	case "high":
		ch.collecting = false
		events := make([]domain.CDCEvent, 0, len(ch.pending))
		now := time.Now().UTC()
		for _, row := range ch.pending {
			if _, superseded := ch.seen[row.Key]; superseded {
				continue
			}
			events = append(events, domain.CDCEvent{
				ID:        "bf:" + ch.id + ":" + row.Key,
				Operation: domain.OperationRead,
				Schema:    ch.table.Schema,
				Table:     ch.table.Table,
				NewData:   row.Values,
				Metadata: domain.EventMetadata{
					LSN:       lsn,
					Timestamp: now,
				},
			})
		}
		ch.delivered = len(events)
		ch.pending = nil // the events own the rows now
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
	defer c.mu.Unlock()
	if ch := c.inflight[payload.Chunk]; ch != nil {
		delete(c.inflight, payload.Chunk)
		close(ch.done)
	}
}

func (c *Coordinator) OnStreamStart() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.streaming = true
	close(c.streamUp)
	c.streamUp = make(chan struct{})
	// Chunks emitted while the stream was down may sit behind a position
	// another node already confirmed and never come back: re-emit them.
	if len(c.inflight) > 0 {
		c.restartLocked()
	}
}

func (c *Coordinator) OnStreamStop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.streaming = false
	if len(c.inflight) > 0 {
		c.restartLocked()
	}
}

// restartLocked abandons every in-flight chunk; their tables continue from
// the last delivered chunk with fresh watermarks.
func (c *Coordinator) restartLocked() {
	clear(c.inflight)
	close(c.restart)
	c.restart = make(chan struct{})
}

// waitForStream blocks until this process consumes the replication stream.
func (c *Coordinator) waitForStream(ctx context.Context) error {
	for {
		c.mu.Lock()
		up, ch := c.streaming, c.streamUp
		c.mu.Unlock()
		if up {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// --- lanes ---

func (c *Coordinator) lane(ctx context.Context) {
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

		err := c.backfillTable(ctx, ref)
		c.mu.Lock()
		delete(c.running, ref)
		c.mu.Unlock()

		if err != nil {
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

	// Another node may have advanced or finished this table since this
	// process loaded its state (failover between nodes sharing a slot):
	// the persisted row is the truth.
	if persisted, found, err := c.db.LoadTableState(ctx, ref.String()); err != nil {
		c.logger.Warn("failed to reload backfill state, using cached progress",
			slog.String("table", ref.String()), slog.String("error", err.Error()))
	} else {
		c.mu.Lock()
		if found {
			c.state[ref.String()] = persisted
		} else {
			delete(c.state, ref.String())
		}
		c.mu.Unlock()
		if found && persisted.Done {
			completed := persisted.UpdatedAt.UTC()
			c.setStatus(ref, func(s *domain.BackfillTableStatus) {
				s.State = domain.BackfillDone
				s.RowsCopied = persisted.RowsCopied
				s.CompletedAt = &completed
				s.Error = ""
			})
			c.logger.Info("backfill already completed", slog.String("table", ref.String()))
			return nil
		}
	}

	c.mu.Lock()
	prev := c.state[ref.String()]
	c.mu.Unlock()
	cursor := prev.Cursor
	var copied int64
	if len(cursor) > 0 {
		copied = prev.RowsCopied // resuming: keep the count from before
	}

	now := time.Now().UTC()
	c.setStatus(ref, func(s *domain.BackfillTableStatus) {
		s.State = domain.BackfillRunning
		s.RowsCopied = copied
		s.Error = ""
		if s.StartedAt == nil {
			s.StartedAt = &now
		}
	})
	c.logger.Info("backfill started",
		slog.String("table", ref.String()),
		slog.Any("pk", pkCols),
		slog.Bool("resuming", len(cursor) > 0),
	)

	// window holds this table's in-flight chunks in WAL order. next is
	// where the following chunk starts; exhausted is set once a short chunk
	// was selected.
	var window []*chunkInFlight
	next := cursor
	exhausted := false
	defer func() { c.abandon(window) }()

	for {
		for !exhausted && len(window) < pipelineDepth {
			ch, err := c.startChunk(ctx, ref, pkCols, next)
			if err != nil {
				return err
			}
			window = append(window, ch)
			next = ch.lastCursor
			if ch.rowCount < c.cfg.ChunkSize {
				exhausted = true
			}
		}

		head := window[0]
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-head.restart:
			// The stream went down (or came back) with chunks in flight:
			// everything after the last delivered chunk is selected again
			// with fresh watermarks; re-emitted rows are idempotent.
			c.logger.Warn("stream restarted mid-chunk, retrying",
				slog.String("table", ref.String()),
				slog.String("chunk", head.id),
			)
			c.abandon(window)
			window = nil
			next = cursor
			exhausted = false
			continue

		case <-head.done:
		}

		window = window[1:]
		copied += int64(head.delivered)
		metrics.BackfillRowsTotal.WithLabelValues(ref.Schema, ref.Table).Add(float64(head.delivered))

		// A short chunk is the table's last: keyset pagination found no
		// rows past it, and rows inserted since arrive on the live stream.
		if head.rowCount < c.cfg.ChunkSize {
			final := TableState{Done: true, RowsCopied: copied}
			c.mu.Lock()
			c.state[ref.String()] = final
			c.mu.Unlock()
			if err := c.db.SaveState(ctx, ref.String(), final); err != nil {
				c.logger.Warn("failed to persist backfill completion", slog.String("error", err.Error()))
			}
			done := time.Now().UTC()
			c.setStatus(ref, func(s *domain.BackfillTableStatus) {
				s.State = domain.BackfillDone
				s.RowsCopied = copied
				s.CompletedAt = &done
				s.Error = ""
			})
			c.logger.Info("backfill complete",
				slog.String("table", ref.String()), slog.Int64("rows", copied))
			return nil
		}

		cursor = head.lastCursor
		progress := TableState{Cursor: cursor, RowsCopied: copied}
		c.mu.Lock()
		c.state[ref.String()] = progress
		c.mu.Unlock()
		if err := c.db.SaveState(ctx, ref.String(), progress); err != nil {
			c.logger.Warn("failed to persist backfill progress", slog.String("error", err.Error()))
		}
		c.setStatus(ref, func(s *domain.BackfillTableStatus) {
			s.RowsCopied = copied
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

// startChunk emits the low watermark, selects the chunk, and emits the high
// watermark; the stream then delivers it. A chunk that selects no rows has
// nothing to deliver and completes at once.
func (c *Coordinator) startChunk(
	ctx context.Context,
	ref tableRef,
	pkCols, cursor []string,
) (*chunkInFlight, error) {
	if err := c.waitForStream(ctx); err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.chunkSeq++
	ch := &chunkInFlight{
		table:   ref,
		id:      fmt.Sprintf("%s-%s-%d", c.instance, ref.String(), c.chunkSeq),
		seen:    make(map[string]struct{}),
		pkCols:  pkCols,
		done:    make(chan struct{}),
		restart: c.restart,
	}
	c.inflight[ch.id] = ch
	c.mu.Unlock()

	// DBLog ordering: low watermark, then the chunk select, then the high
	// watermark. Any event streamed between the two watermarks for a
	// selected key supersedes the selected row.
	if err := c.emitWatermark(ctx, ch.id, "low"); err != nil {
		c.abandon([]*chunkInFlight{ch})
		return nil, err
	}

	selected, err := c.db.SelectChunk(ctx, ref.Schema, ref.Table, pkCols, cursor, c.cfg.ChunkSize)
	if err != nil {
		c.abandon([]*chunkInFlight{ch})
		return nil, err
	}

	c.mu.Lock()
	ch.pending = selected.Rows
	ch.rowCount = len(selected.Rows)
	ch.lastCursor = selected.LastCursor
	if ch.rowCount == 0 {
		delete(c.inflight, ch.id)
		close(ch.done)
	}
	c.mu.Unlock()
	if ch.rowCount == 0 {
		return ch, nil
	}

	if err := c.emitWatermark(ctx, ch.id, "high"); err != nil {
		c.abandon([]*chunkInFlight{ch})
		return nil, err
	}
	return ch, nil
}

// abandon forgets chunks that will not be awaited; their watermarks, if they
// still arrive, are ignored.
func (c *Coordinator) abandon(chunks []*chunkInFlight) {
	if len(chunks) == 0 {
		return
	}
	c.mu.Lock()
	for _, ch := range chunks {
		delete(c.inflight, ch.id)
	}
	c.mu.Unlock()
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
	c.running[ref] = true
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

func newInstanceID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
