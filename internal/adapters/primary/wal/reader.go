package wal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Config struct {
	DatabaseURL     string
	SlotName        string
	PublicationName string
	StandbyTimeout  time.Duration
}

type Reader struct {
	config    Config
	logger    *slog.Logger
	conn      *pgconn.PgConn
	decoder   *Decoder
	clientLSN pglogrepl.LSN
	mu        sync.RWMutex
	shutdown  chan struct{}
}

func NewReader(cfg Config, logger *slog.Logger) *Reader {
	return &Reader{
		config:   cfg,
		logger:   logger.With(slog.String("component", "wal_reader")),
		decoder:  NewDecoder(),
		shutdown: make(chan struct{}),
	}
}

func (r *Reader) Start(ctx context.Context, handler ports.WALEventHandler) error {
	r.logger.Info("starting WAL reader",
		slog.String("slot_name", r.config.SlotName),
		slog.String("publication", r.config.PublicationName),
		slog.Duration("standby_timeout", r.config.StandbyTimeout),
	)

	if err := r.connect(ctx); err != nil {
		r.logger.Error("connection failed", slog.String("error", err.Error()))
		return err
	}

	if err := r.setupReplication(ctx); err != nil {
		r.logger.Error("replication setup failed", slog.String("error", err.Error()))
		return err
	}

	r.logger.Info("WAL streaming started, listening for changes")
	return r.streamLoop(ctx, handler)
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

	r.logger.Debug("dropping existing publication", slog.String("publication", r.config.PublicationName))
	result := r.conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", r.config.PublicationName))
	if _, err = result.ReadAll(); err != nil {
		return fmt.Errorf("drop publication failed: %w", err)
	}

	r.logger.Debug("creating publication", slog.String("publication", r.config.PublicationName))
	result = r.conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.config.PublicationName))
	if _, err = result.ReadAll(); err != nil {
		return fmt.Errorf("create publication failed: %w", err)
	}
	r.logger.Info("publication created", slog.String("publication", r.config.PublicationName))

	r.logger.Debug("creating replication slot",
		slog.String("slot_name", r.config.SlotName),
		slog.Bool("temporary", true),
	)
	_, err = pglogrepl.CreateReplicationSlot(
		ctx,
		r.conn,
		r.config.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true},
	)
	if err != nil {
		return fmt.Errorf("create replication slot failed: %w", err)
	}
	r.logger.Info("replication slot created", slog.String("slot_name", r.config.SlotName))

	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.config.PublicationName),
		"messages 'true'",
		"streaming 'true'",
	}

	r.logger.Debug("starting replication",
		slog.String("start_lsn", sysident.XLogPos.String()),
	)
	err = pglogrepl.StartReplication(
		ctx,
		r.conn,
		r.config.SlotName,
		sysident.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
	)
	if err != nil {
		return fmt.Errorf("start replication failed: %w", err)
	}

	r.mu.Lock()
	r.clientLSN = sysident.XLogPos
	r.mu.Unlock()

	r.logger.Info("replication started", slog.String("lsn", sysident.XLogPos.String()))
	return nil
}

func (r *Reader) streamLoop(ctx context.Context, handler ports.WALEventHandler) error {
	nextDeadline := time.Now().Add(r.config.StandbyTimeout)
	eventsProcessed := int64(0)

	for {
		select {
		case <-r.shutdown:
			r.logger.Info("shutdown signal received", slog.Int64("events_processed", eventsProcessed))
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

			if err := handler(ctx, event); err != nil {
				r.logger.Error("handler failed",
					slog.String("error", err.Error()),
					slog.String("event_id", event.ID),
				)
				return fmt.Errorf("handler failed: %w", err)
			}
			eventsProcessed++
		}

		if result.LSN > r.clientLSN {
			r.mu.Lock()
			r.clientLSN = result.LSN
			r.mu.Unlock()
		}
	}
}

func (r *Reader) sendStandbyStatus(ctx context.Context) error {
	r.mu.RLock()
	lsn := r.clientLSN
	r.mu.RUnlock()

	r.logger.Debug("sending standby status", slog.String("lsn", lsn.String()))

	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		r.conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn},
	)
}

func (r *Reader) Stop(ctx context.Context) error {
	r.logger.Info("stopping WAL reader")
	close(r.shutdown)

	if r.conn != nil {
		if err := r.conn.Close(ctx); err != nil {
			r.logger.Error("failed to close connection", slog.String("error", err.Error()))
			return err
		}
	}

	r.logger.Info("WAL reader stopped")
	return nil
}

func (r *Reader) CurrentLSN() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clientLSN.String()
}
