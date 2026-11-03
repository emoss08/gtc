// Package nats publishes change events to NATS subjects — the "event bus
// without Kafka" half of GTC's story. Publishing goes through JetStream by
// default so a delivery is only reported successful once the server has
// persisted the message, and redelivered duplicates are collapsed by the
// server using the event's LSN as the message ID.
package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/emoss08/gtc/internal/core/domain"
)

// SubjectResolver decides which tables are published and builds each event's
// subject. It is satisfied by the shared key resolver without coupling this
// package to Redis.
type SubjectResolver interface {
	ShouldProcess(schema, table string) bool
	GenerateKey(event domain.CDCEvent) (string, error)
}

// DefaultSubjectPattern mirrors the Redis stream key layout, with NATS's dot
// separator: <prefix>.<schema>.<table>.
const DefaultSubjectPattern = "{{.Prefix}}.{{.Schema}}.{{.Table}}"

type Sink struct {
	conn     *nats.Conn
	js       jetstream.JetStream
	config   Config
	resolver SubjectResolver
	logger   *slog.Logger
}

var _ domain.Sink = (*Sink)(nil)

type SinkParams struct {
	Config   Config
	Resolver SubjectResolver
	Logger   *slog.Logger
}

func NewSink(p SinkParams) *Sink {
	return &Sink{
		config:   p.Config,
		resolver: p.Resolver,
		logger:   p.Logger.With(slog.String("component", "nats")),
	}
}

func (s *Sink) Name() string { return "nats" }

func (s *Sink) Wants(schema, table string, _ domain.Operation) bool {
	return s.resolver == nil || s.resolver.ShouldProcess(schema, table)
}

func (s *Sink) Initialize(ctx context.Context) error {
	opts := []nats.Option{
		nats.Name("gtc"),
		nats.Timeout(s.config.Timeout),
		// Keep reconnecting indefinitely: a NATS outage should stall the
		// pipeline (WAL is retained) rather than fail it permanently.
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			s.logger.Warn("disconnected from NATS", slog.Any("error", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			s.logger.Info("reconnected to NATS", slog.String("url", nc.ConnectedUrl()))
		}),
	}
	if s.config.Credentials != "" {
		opts = append(opts, nats.UserCredentials(s.config.Credentials))
	}
	if s.config.Token != "" {
		opts = append(opts, nats.Token(s.config.Token))
	}

	conn, err := nats.Connect(s.config.URL, opts...)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	s.conn = conn

	if s.config.JetStream {
		js, err := jetstream.New(conn, jetstream.WithPublishAsyncMaxPending(2*publishWindow))
		if err != nil {
			conn.Close()
			return fmt.Errorf("initialize JetStream: %w", err)
		}
		s.js = js
		if err := s.ensureStream(ctx); err != nil {
			conn.Close()
			return err
		}
	}

	s.logger.Info("sink initialized",
		slog.String("url", conn.ConnectedUrl()),
		slog.Bool("jetstream", s.config.JetStream),
		slog.String("subject_prefix", s.config.SubjectPrefix),
	)
	return nil
}

// ensureStream creates the stream that captures this deployment's subjects,
// so a fresh NATS server works without manual setup. A stream managed
// elsewhere that already covers the subjects is left alone.
func (s *Sink) ensureStream(ctx context.Context) error {
	if !s.config.AutoCreateStream {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	subject := SanitizeSubject(s.config.SubjectPrefix) + ".>"
	_, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     s.config.Stream,
		Subjects: []string{subject},
		Storage:  jetstream.FileStorage,
		// Redelivery after a failed transaction re-publishes the same LSN;
		// JetStream collapses those within this window.
		Duplicates: 2 * time.Minute,
	})
	switch {
	case err == nil:
		s.logger.Info("jetstream stream ready",
			slog.String("stream", s.config.Stream), slog.String("subjects", subject))
		return nil
	case errors.Is(err, jetstream.ErrStreamNameAlreadyInUse),
		strings.Contains(err.Error(), "overlap"):
		// Someone else owns a stream for these subjects; publishing still
		// works, and adopting their configuration is not ours to do.
		s.logger.Info("using existing jetstream stream for these subjects",
			slog.String("subjects", subject), slog.Any("detail", err))
		return nil
	default:
		return fmt.Errorf("ensure jetstream stream %q: %w", s.config.Stream, err)
	}
}

func (s *Sink) Shutdown(context.Context) error {
	if s.conn == nil {
		return nil
	}
	// Flush in-flight core-NATS publishes before closing.
	if err := s.conn.FlushTimeout(s.config.Timeout); err != nil {
		s.logger.Warn("flush on shutdown failed", slog.Any("error", err))
	}
	s.conn.Close()
	return nil
}

func (s *Sink) HealthCheck(context.Context) error {
	if s.conn == nil || !s.conn.IsConnected() {
		return fmt.Errorf("not connected to NATS")
	}
	return nil
}

type payload struct {
	EventID               string         `json:"event_id"`
	Operation             string         `json:"operation"`
	Schema                string         `json:"schema"`
	Table                 string         `json:"table"`
	OldData               map[string]any `json:"old_data"`
	NewData               map[string]any `json:"new_data"`
	UnchangedToastColumns []string       `json:"unchanged_toast_columns"`
	Metadata              metadata       `json:"metadata"`
}

type metadata struct {
	LSN           string    `json:"lsn"`
	TransactionID uint32    `json:"transaction_id"`
	Timestamp     time.Time `json:"timestamp"`
}

// publishWindow bounds how many JetStream publishes are awaiting an ack at
// once, below the client's async pending limit.
const publishWindow = 1024

// Process publishes the batch. JetStream publishes are pipelined in windows
// and the batch succeeds once every message is acknowledged as persisted;
// core NATS publishes are flushed once per batch. Async publishes are not
// retried individually: a failure fails the batch, which is redelivered and
// deduplicated by message ID.
func (s *Sink) Process(ctx context.Context, events []domain.CDCEvent) error {
	msgs := make([]*nats.Msg, 0, len(events))
	ids := make([]string, 0, len(events))
	for i := range events {
		event := &events[i]
		if s.resolver != nil && !s.resolver.ShouldProcess(event.Schema, event.Table) {
			continue
		}
		msg, err := s.message(*event)
		if err != nil {
			return domain.Permanent(err)
		}
		msgs = append(msgs, msg)
		ids = append(ids, event.ID)
	}
	if len(msgs) == 0 {
		return nil
	}

	if s.js == nil {
		for _, msg := range msgs {
			if err := s.conn.PublishMsg(msg); err != nil {
				return fmt.Errorf("publish %s: %w", msg.Subject, err)
			}
		}
		// FlushWithContext refuses a context without a deadline.
		flushCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.conn.FlushWithContext(flushCtx); err != nil {
			return fmt.Errorf("flush %d messages: %w", len(msgs), err)
		}
		return nil
	}

	for start := 0; start < len(msgs); start += publishWindow {
		end := min(start+publishWindow, len(msgs))
		if err := s.publishWindow(ctx, msgs[start:end], ids[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) publishWindow(ctx context.Context, msgs []*nats.Msg, ids []string) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	futures := make([]jetstream.PubAckFuture, len(msgs))
	for i, msg := range msgs {
		f, err := s.js.PublishMsgAsync(msg,
			jetstream.WithMsgID(ids[i]),
			jetstream.WithRetryAttempts(0),
		)
		if err != nil {
			return fmt.Errorf("publish %s: %w", msg.Subject, err)
		}
		futures[i] = f
	}

	for i, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("publish %s (event %s): %w", msgs[i].Subject, ids[i], err)
		case <-ctx.Done():
			return fmt.Errorf("await publish acks: %w", ctx.Err())
		}
	}
	return nil
}

func (s *Sink) message(event domain.CDCEvent) (*nats.Msg, error) {
	subject, err := s.subject(event)
	if err != nil {
		return nil, err
	}

	data, err := sonic.Marshal(payload{
		EventID:               event.ID,
		Operation:             event.Operation.String(),
		Schema:                event.Schema,
		Table:                 event.Table,
		OldData:               event.OldData,
		NewData:               event.NewData,
		UnchangedToastColumns: event.UnchangedToastColumns,
		Metadata: metadata{
			LSN:           event.Metadata.LSN,
			TransactionID: event.Metadata.TransactionID,
			Timestamp:     event.Metadata.Timestamp,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal payload for event %s: %w", event.ID, err)
	}
	return &nats.Msg{Subject: subject, Data: data}, nil
}

func (s *Sink) subject(event domain.CDCEvent) (string, error) {
	if s.resolver == nil {
		return SanitizeSubject(fmt.Sprintf("%s.%s.%s",
			s.config.SubjectPrefix, event.Schema, event.Table)), nil
	}
	subject, err := s.resolver.GenerateKey(event)
	if err != nil {
		return "", fmt.Errorf("build subject for %s: %w", event.FullTableName(), err)
	}
	return SanitizeSubject(subject), nil
}

// SanitizeSubject makes a generated subject safe to publish to. NATS rejects
// publishes to subjects containing wildcards or whitespace, and identifiers
// in PostgreSQL may contain both, so those characters become underscores.
// Dots are left alone: they are the pattern's own token separator.
func SanitizeSubject(subject string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '*', '>', ' ', '\t', '\n', '\r':
			return '_'
		}
		return r
	}, subject)
}
