package wal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	// StreamStarted, when set, is called after replication is (re)established.
	// slotCreated reports whether this run created the replication slot —
	// true only on the very first start against a fresh database.
	StreamStarted func(slotCreated bool)
}

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

func (r *Reader) Start(ctx context.Context, handler ports.WALEventHandler) error {
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
			if r.config.Controller != nil {
				r.config.Controller.OnStreamRestart()
			}
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

func (r *Reader) streamLoop(ctx context.Context, handler ports.WALEventHandler) error {
	r.streaming.Store(true)
	defer r.streaming.Store(false)

	nextDeadline := time.Now().Add(r.config.StandbyTimeout)
	eventsProcessed := int64(0)

	for {
		select {
		case <-r.shutdown:
			r.logger.Info(
				"shutdown signal received",
				slog.Int64("events_processed", eventsProcessed),
			)
			return nil
		case <-ctx.Done():
			r.logger.Info("context cancelled", slog.Int64("events_processed", eventsProcessed))
			return ctx.Err()
		default:
		}

		if time.Now().After(nextDeadline) {
			if err := r.sendStandbyStatus(ctx); err != nil {
				r.logger.Error("failed to send standby status", slog.String("error", err.Error()))
				return err
			}
			nextDeadline = time.Now().Add(r.config.StandbyTimeout)
		}

		msgCtx, cancel := context.WithDeadline(ctx, nextDeadline)
		rawMsg, err := r.conn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
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

		for _, event := range result.Events {
			r.logger.Debug("event received",
				slog.String("operation", event.Operation.String()),
				slog.String("schema", event.Schema),
				slog.String("table", event.Table),
				slog.String("lsn", event.Metadata.LSN),
				slog.Int("xid", int(event.Metadata.TransactionID)),
			)

			if r.config.Controller != nil {
				r.config.Controller.ObserveEvent(event)
			}

			// A handler error means at least one sink did not durably
			// process the event. Returning the error tears down the
			// connection without confirming the transaction's LSN, so the
			// server redelivers it on reconnect (at-least-once delivery).
			if err := handler(ctx, event); err != nil {
				r.logger.Error("handler failed",
					slog.String("error", err.Error()),
					slog.String("event_id", event.ID),
				)
				return fmt.Errorf("handler failed: %w", err)
			}
			eventsProcessed++
		}

		if msg := result.Message; msg != nil && msg.Prefix == WatermarkPrefix && r.config.Controller != nil {
			// Backfill chunk rows are emitted at the high-watermark
			// position; a handler error tears the stream down and the
			// coordinator retries the chunk with fresh watermarks.
			events := r.config.Controller.HandleWatermark(msg.LSN.String(), msg.Content)
			for _, event := range events {
				if err := handler(ctx, event); err != nil {
					r.logger.Error("backfill handler failed",
						slog.String("error", err.Error()),
						slog.String("event_id", event.ID),
					)
					return fmt.Errorf("backfill handler failed: %w", err)
				}
				eventsProcessed++
			}
			r.config.Controller.WatermarkDelivered(msg.Content)
		}

		if uint64(result.AckLSN) > r.clientLSN.Load() {
			r.clientLSN.Store(uint64(result.AckLSN))
			metrics.LastProcessedLSN.Set(float64(result.AckLSN))
		}
		if result.ServerWALEnd > 0 {
			if lag := int64(uint64(result.ServerWALEnd) - r.clientLSN.Load()); lag >= 0 {
				metrics.WALLagBytes.Set(float64(lag))
			}
		}

		if result.RequestReply {
			if err := r.sendStandbyStatus(ctx); err != nil {
				r.logger.Error("failed to send requested standby status", slog.String("error", err.Error()))
				return err
			}
			nextDeadline = time.Now().Add(r.config.StandbyTimeout)
		}
	}
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
