package wal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// WatermarkPrefix is the pg_logical_emit_message prefix used for backfill
// watermarks.
const WatermarkPrefix = "gtc-backfill"

type Config struct {
	DatabaseURL         string
	SlotName            string
	PublicationName     string
	StandbyTimeout      time.Duration
	ReconnectBackoff    time.Duration
	MaxReconnectBackoff time.Duration
	AutoCreateSlot      bool
	AutoCreatePub       bool
	SlotRetryInterval   time.Duration
	SlotRetryTimeout    time.Duration
	// Controller, when set, interleaves backfill chunks with the live
	// stream (see ports.BackfillController).
	Controller ports.BackfillController
	// SchemaObserver, when set, is notified of DDL changes detected on
	// published tables.
	SchemaObserver ports.SchemaObserver
	// StreamStarted, when set, is called after replication is (re)established.
	// slotCreated reports whether this run created the replication slot —
	// true only on the very first start against a fresh database.
	StreamStarted func(slotCreated bool)
	// MaxBatchEvents caps the events handed to the sinks at once.
	MaxBatchEvents int
	// BatchLinger is the longest an event waits for its batch to fill
	// before the batch is handed over anyway.
	BatchLinger time.Duration
}

const (
	defaultMaxBatchEvents = 5000
	defaultBatchLinger    = 10 * time.Millisecond
)

type Reader struct {
	config      Config
	logger      *slog.Logger
	conn        *pgconn.PgConn
	decoder     *Decoder
	clientLSN   atomic.Uint64
	streaming   atomic.Bool
	slotCreated bool
	shutdown    chan struct{}
	done        chan struct{}
	stopOnce    sync.Once
}

func NewReader(cfg Config, logger *slog.Logger) *Reader {
	return &Reader{
		config:   cfg,
		logger:   logger.With(slog.String("component", "wal_reader")),
		decoder:  NewDecoder(),
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (r *Reader) Start(ctx context.Context, handler ports.WALBatchHandler) error {
	defer close(r.done)
	defer r.closeConnection()

	r.logger.Info("starting WAL reader",
		slog.String("slot_name", r.config.SlotName),
		slog.String("publication", r.config.PublicationName),
		slog.Duration("standby_timeout", r.config.StandbyTimeout),
	)

	backoff := r.config.ReconnectBackoff
	if backoff == 0 {
		backoff = time.Second
	}
	maxBackoff := r.config.MaxReconnectBackoff
	if maxBackoff == 0 {
		maxBackoff = 30 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.shutdown:
			return nil
		default:
		}

		if err := r.connect(ctx); err != nil {
			r.logger.Error("connection failed", slog.String("error", err.Error()))
			r.waitWithBackoff(ctx, backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if err := r.setupReplication(ctx); err != nil {
			r.logger.Error("replication setup failed", slog.String("error", err.Error()))
			r.closeConnection()
			r.waitWithBackoff(ctx, backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = r.config.ReconnectBackoff
		if backoff == 0 {
			backoff = time.Second
		}

		if r.config.StreamStarted != nil {
			r.config.StreamStarted(r.slotCreated)
		}

		r.logger.Info("WAL streaming started, listening for changes")
		if err := r.streamLoop(ctx, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("stream loop failed, will reconnect and replay from last confirmed LSN",
				slog.String("error", err.Error()),
				slog.Duration("backoff", backoff),
			)
			r.closeConnection()
			r.waitWithBackoff(ctx, backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		return nil
	}
}

func (r *Reader) waitWithBackoff(ctx context.Context, backoff time.Duration) {
	select {
	case <-ctx.Done():
	case <-r.shutdown:
	case <-time.After(backoff):
	}
}

func (r *Reader) closeConnection() {
	if r.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = r.conn.Close(ctx)
		cancel()
		r.conn = nil
	}
}

func (r *Reader) connect(ctx context.Context) error {
	r.logger.Debug("connecting to PostgreSQL")

	conn, err := pgconn.Connect(ctx, r.config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	r.conn = conn
	r.logger.Info("connected to PostgreSQL")
	return nil
}

func (r *Reader) setupReplication(ctx context.Context) error {
	r.logger.Debug("identifying system")

	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("identify system failed: %w", err)
	}

	r.logger.Info("system identified",
		slog.String("system_id", sysident.SystemID),
		slog.Int("timeline", int(sysident.Timeline)),
		slog.String("xlog_pos", sysident.XLogPos.String()),
		slog.String("db_name", sysident.DBName),
	)

	if err := r.ensurePublication(ctx); err != nil {
		return err
	}

	startLSN, err := r.ensureReplicationSlot(ctx)
	if err != nil {
		return err
	}

	if err := r.startReplicationWithRetry(ctx, startLSN); err != nil {
		return err
	}

	r.clientLSN.Store(uint64(startLSN))
	r.logger.Info("replication started", slog.String("lsn", startLSN.String()))
	return nil
}

// quoteLiteral makes a string safe to embed in a simple-protocol query.
// Replication connections reject the extended query protocol (parameterized
// queries), so catalog checks on r.conn must be plain SQL.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// readSimpleQuery runs one simple-protocol query and returns its rows.
func readSimpleQuery(ctx context.Context, conn *pgconn.PgConn, sql string) ([][][]byte, error) {
	results, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0].Rows, nil
}

func (r *Reader) ensurePublication(ctx context.Context) error {
	rows, err := readSimpleQuery(ctx, r.conn, fmt.Sprintf(
		"SELECT 1 FROM pg_publication WHERE pubname = %s",
		quoteLiteral(r.config.PublicationName),
	))
	if err != nil {
		return fmt.Errorf("check publication failed: %w", err)
	}

	if len(rows) > 0 {
		r.logger.Info("publication exists", slog.String("publication", r.config.PublicationName))
		return nil
	}

	if !r.config.AutoCreatePub {
		return fmt.Errorf(
			"publication %q does not exist and auto-create is disabled",
			r.config.PublicationName,
		)
	}

	r.logger.Info("creating publication", slog.String("publication", r.config.PublicationName))
	result := r.conn.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR ALL TABLES",
		pgx.Identifier{r.config.PublicationName}.Sanitize(),
	))
	if _, err := result.ReadAll(); err != nil {
		return fmt.Errorf("create publication failed: %w", err)
	}

	r.logger.Info("publication created", slog.String("publication", r.config.PublicationName))
	return nil
}

func (r *Reader) ensureReplicationSlot(ctx context.Context) (pglogrepl.LSN, error) {
	rows, err := readSimpleQuery(ctx, r.conn, fmt.Sprintf(
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s",
		quoteLiteral(r.config.SlotName),
	))
	if err != nil {
		return 0, fmt.Errorf("check replication slot failed: %w", err)
	}

	if len(rows) > 0 && len(rows[0]) > 0 {
		lsnStr := string(rows[0][0])
		startLSN, parseErr := pglogrepl.ParseLSN(lsnStr)
		if parseErr != nil {
			return 0, fmt.Errorf("parse confirmed_flush_lsn failed: %w", parseErr)
		}
		r.logger.Info("replication slot exists",
			slog.String("slot_name", r.config.SlotName),
			slog.String("confirmed_flush_lsn", startLSN.String()),
		)
		return startLSN, nil
	}

	if !r.config.AutoCreateSlot {
		return 0, fmt.Errorf(
			"replication slot %q does not exist and auto-create is disabled",
			r.config.SlotName,
		)
	}

	r.logger.Info("creating replication slot", slog.String("slot_name", r.config.SlotName))
	slotResult, err := pglogrepl.CreateReplicationSlot(
		ctx,
		r.conn,
		r.config.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false},
	)
	if err != nil {
		return 0, fmt.Errorf("create replication slot failed: %w", err)
	}
	r.slotCreated = true

	startLSN, err := pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("parse consistent_point failed: %w", err)
	}

	r.logger.Info("replication slot created",
		slog.String("slot_name", r.config.SlotName),
		slog.String("consistent_point", startLSN.String()),
	)
	return startLSN, nil
}

func (r *Reader) startReplicationWithRetry(ctx context.Context, startLSN pglogrepl.LSN) error {
	// Streaming of in-progress transactions is deliberately not requested:
	// streamed changes can belong to transactions that later abort, and
	// delivering them to sinks before commit would publish phantom data.
	// Logical decoding messages are enabled for backfill watermarks.
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.config.PublicationName),
		"messages 'true'",
	}

	deadline := time.Now().Add(r.config.SlotRetryTimeout)

	for {
		r.logger.Debug("starting replication", slog.String("start_lsn", startLSN.String()))

		err := pglogrepl.StartReplication(
			ctx,
			r.conn,
			r.config.SlotName,
			startLSN,
			pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
		)
		if err == nil {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("start replication failed after timeout: %w", err)
		}

		r.logger.Warn("replication slot busy, retrying",
			slog.String("error", err.Error()),
			slog.Duration("retry_in", r.config.SlotRetryInterval),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.config.SlotRetryInterval):
		}
	}
}

// delivery is one ordered unit of work for the delivery goroutine.
type delivery struct {
	events []domain.CDCEvent
	// ack is confirmed once events are handled (0: nothing to confirm).
	ack pglogrepl.LSN
	// watermark, when set, is reported delivered once events are handled.
	watermark []byte
}

// deliveryQueueDepth is how many batches may wait behind the one the sinks
// are working on. Decoding runs ahead of delivery by at most this much.
const deliveryQueueDepth = 2

// streamLoop reads the replication stream and hands events to the handler in
// batches. A batch closes when it reaches MaxBatchEvents, when BatchLinger has
// passed since its first event, or before a backfill watermark is handled;
// under load the socket backlog fills each batch, and when the stream is quiet
// the linger bounds the added latency. A batch may span transactions and may
// hold part of a large one.
//
// Decoding and delivery overlap: batches go to a delivery goroutine through a
// small ordered queue, so the next batch is decoded while sinks write the
// current one. Positions are confirmed by the delivery goroutine only after
// the batch holding every event before them was handled, so delivery stays
// at-least-once. This goroutine keeps sole use of the connection and keeps
// sending keepalives however long the sinks take.
func (r *Reader) streamLoop(ctx context.Context, handler ports.WALBatchHandler) error {
	r.streaming.Store(true)
	defer r.streaming.Store(false)

	if r.config.Controller != nil {
		r.config.Controller.OnStreamStart()
		defer r.config.Controller.OnStreamStop()
	}

	maxBatch := r.config.MaxBatchEvents
	if maxBatch <= 0 {
		maxBatch = defaultMaxBatchEvents
	}
	linger := r.config.BatchLinger
	if linger <= 0 {
		linger = defaultBatchLinger
	}

	confirm := func(lsn pglogrepl.LSN) {
		if uint64(lsn) > r.clientLSN.Load() {
			r.clientLSN.Store(uint64(lsn))
			metrics.LastProcessedLSN.Set(float64(lsn))
		}
	}

	// --- delivery goroutine ---
	queue := make(chan delivery, deliveryQueueDepth)
	failed := make(chan error, 1)
	var outstanding atomic.Int64 // queued or being delivered
	var eventsProcessed atomic.Int64
	deliverCtx, cancelDeliver := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for d := range queue {
			if len(d.events) > 0 {
				if err := r.deliver(deliverCtx, handler, d.events); err != nil {
					failed <- err
					return
				}
				eventsProcessed.Add(int64(len(d.events)))
			}
			if d.watermark != nil {
				r.config.Controller.WatermarkDelivered(d.watermark)
			}
			if d.ack > 0 {
				confirm(d.ack)
			}
			outstanding.Add(-1)
		}
	}()
	defer func() {
		// Batches still queued were never confirmed and are redelivered
		// after reconnect; drop them and let the one in progress finish.
		for {
			select {
			case <-queue:
				continue
			default:
			}
			break
		}
		close(queue)
		wg.Wait()
		cancelDeliver()
	}()

	nextDeadline := time.Now().Add(r.config.StandbyTimeout)
	keepalive := func() error {
		if err := r.sendStandbyStatus(ctx); err != nil {
			r.logger.Error("failed to send standby status", slog.String("error", err.Error()))
			return err
		}
		nextDeadline = time.Now().Add(r.config.StandbyTimeout)
		return nil
	}

	// enqueue hands a unit to the delivery goroutine, keeping the
	// connection alive while the queue is full.
	enqueue := func(d delivery) error {
		outstanding.Add(1)
		for {
			wait := time.Until(nextDeadline)
			timer := time.NewTimer(max(wait, 0))
			select {
			case queue <- d:
				timer.Stop()
				return nil
			case err := <-failed:
				timer.Stop()
				return err
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				if err := keepalive(); err != nil {
					return err
				}
			}
		}
	}

	var (
		batch      []domain.CDCEvent
		flushAt    time.Time     // when the open batch must be handed over
		pendingAck pglogrepl.LSN // confirmable once everything before it is handled
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		d := delivery{events: batch, ack: pendingAck}
		// A fresh slice: the delivery goroutine owns the old one.
		batch = nil
		flushAt = time.Time{}
		pendingAck = 0
		return enqueue(d)
	}

	for {
		select {
		case <-r.shutdown:
			r.logger.Info(
				"shutdown signal received",
				slog.Int64("events_processed", eventsProcessed.Load()),
			)
			return nil
		case <-ctx.Done():
			r.logger.Info("context cancelled", slog.Int64("events_processed", eventsProcessed.Load()))
			return ctx.Err()
		case err := <-failed:
			return err
		default:
		}

		now := time.Now()
		if len(batch) > 0 && !now.Before(flushAt) {
			if err := flush(); err != nil {
				return err
			}
		}
		// An ack with no batch to ride on is confirmed once everything
		// handed over before it is delivered.
		if pendingAck > 0 && len(batch) == 0 && outstanding.Load() == 0 {
			confirm(pendingAck)
			pendingAck = 0
		}

		if !now.Before(nextDeadline) {
			if err := keepalive(); err != nil {
				return err
			}
		}

		receiveBy := nextDeadline
		if len(batch) > 0 && flushAt.Before(receiveBy) {
			receiveBy = flushAt
		}
		if pendingAck > 0 && len(batch) == 0 {
			// Come back soon to confirm it once deliveries drain.
			if soon := now.Add(50 * time.Millisecond); soon.Before(receiveBy) {
				receiveBy = soon
			}
		}
		msgCtx, cancel := context.WithDeadline(ctx, receiveBy)
		rawMsg, err := r.conn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				// Shutdown: not a stream failure.
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue
			}
			r.logger.Error("receive message failed", slog.String("error", err.Error()))
			return fmt.Errorf("receive message failed: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			r.logger.Error("postgres WAL error",
				slog.String("severity", errMsg.Severity),
				slog.String("code", errMsg.Code),
				slog.String("message", errMsg.Message),
				slog.String("detail", errMsg.Detail),
			)
			return fmt.Errorf("postgres WAL error: %s", errMsg.Message)
		}

		result, err := r.decoder.Decode(rawMsg)
		if err != nil {
			r.logger.Error("decode failed", slog.String("error", err.Error()))
			return fmt.Errorf("decode failed: %w", err)
		}

		// Schema changes are advisory and never fail the stream: they
		// cannot be re-derived on redelivery, so a failing observer would
		// stall replication forever without recovering the notification.
		if r.config.SchemaObserver != nil {
			for _, change := range result.SchemaChanges {
				r.config.SchemaObserver.OnSchemaChange(ctx, change)
			}
		}

		for _, event := range result.Events {
			if r.config.Controller != nil {
				r.config.Controller.ObserveEvent(event)
			}
			if len(batch) == 0 {
				batch = make([]domain.CDCEvent, 0, min(maxBatch, 256))
				flushAt = time.Now().Add(linger)
			}
			batch = append(batch, event)
			if len(batch) >= maxBatch {
				if err := flush(); err != nil {
					return err
				}
			}
		}

		if msg := result.Message; msg != nil && msg.Prefix == WatermarkPrefix && r.config.Controller != nil {
			// Backfill chunk rows are emitted at the high-watermark position,
			// queued behind every live event streamed before it. A delivery
			// error tears the stream down and the coordinator retries the
			// chunk with fresh watermarks.
			if err := flush(); err != nil {
				return err
			}
			events := r.config.Controller.HandleWatermark(msg.LSN.String(), msg.Content)
			for start := 0; ; start += maxBatch {
				end := min(start+maxBatch, len(events))
				d := delivery{events: events[start:end]}
				if end == len(events) {
					d.watermark = msg.Content
				}
				if err := enqueue(d); err != nil {
					return err
				}
				if end == len(events) {
					break
				}
			}
		}

		if result.AckLSN > 0 && result.AckLSN > pendingAck {
			pendingAck = result.AckLSN
		}
		if result.ServerWALEnd > 0 {
			if lag := int64(uint64(result.ServerWALEnd) - r.clientLSN.Load()); lag >= 0 {
				metrics.WALLagBytes.Set(float64(lag))
			}
		}

		if result.RequestReply {
			if err := keepalive(); err != nil {
				return err
			}
		}
	}
}

// deliver runs the handler for one batch.
func (r *Reader) deliver(ctx context.Context, handler ports.WALBatchHandler, events []domain.CDCEvent) error {
	if err := handler(ctx, events); err != nil {
		r.logger.Error("handler failed",
			slog.String("error", err.Error()),
			slog.Int("events", len(events)),
			slog.String("first_event_id", events[0].ID),
		)
		return fmt.Errorf("handler failed: %w", err)
	}
	return nil
}

func (r *Reader) sendStandbyStatus(ctx context.Context) error {
	lsn := pglogrepl.LSN(r.clientLSN.Load())

	r.logger.Debug("sending standby status", slog.String("lsn", lsn.String()))

	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		r.conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn},
	)
}

// Stop signals the reader to shut down and waits for the streaming goroutine
// to exit (bounded by ctx). The connection is owned and closed by Start's
// goroutine; closing it here would race with an in-flight ReceiveMessage.
func (r *Reader) Stop(ctx context.Context) error {
	r.logger.Info("stopping WAL reader")
	r.stopOnce.Do(func() { close(r.shutdown) })

	select {
	case <-r.done:
		r.logger.Info("WAL reader stopped")
		return nil
	case <-ctx.Done():
		r.logger.Warn("timed out waiting for WAL reader to stop")
		return ctx.Err()
	}
}

func (r *Reader) CurrentLSN() string {
	return pglogrepl.LSN(r.clientLSN.Load()).String()
}

// Streaming reports whether the reader currently has a live replication
// stream. Used for readiness checks.
func (r *Reader) Streaming() bool {
	return r.streaming.Load()
}
