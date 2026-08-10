package outbox

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Config carries the outbox sink's runtime configuration, assembled from the
// sink YAML plus the Redis and database connections it publishes through.
type Config struct {
	Schema             string
	Table              string
	StreamPrefix       string
	DefaultTopic       string
	DeleteAfterPublish bool
	Columns            config.OutboxColumns
	RedisURL           string
	MaxStreamLen       int64
	// DatabaseURL is a non-replication connection string, required only
	// when DeleteAfterPublish is set.
	DatabaseURL string
}

// Sink implements the transactional outbox pattern: INSERTs into the outbox
// table are published to a Redis stream named by the row's topic column
// (<prefix>:<topic>) instead of being mirrored as table data. Applications
// write domain events to the outbox in the same transaction as their business
// writes; GTC turns them into reliably-delivered messages.
type Sink struct {
	cfg    Config
	client *redis.Client
	pool   *pgxpool.Pool // nil unless DeleteAfterPublish
	logger *slog.Logger
}

var _ domain.Sink = (*Sink)(nil)

func NewSink(cfg Config, logger *slog.Logger) (*Sink, error) {
	if cfg.RedisURL == "" {
		return nil, fmt.Errorf("outbox sink requires REDIS_URL to be set")
	}
	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("outbox: invalid redis URL: %w", err)
	}

	return &Sink{
		cfg:    cfg,
		client: redis.NewClient(opts),
		logger: logger.With(slog.String("component", "outbox_sink")),
	}, nil
}

func (s *Sink) Name() string { return "outbox" }

func (s *Sink) Initialize(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("outbox: redis ping failed: %w", err)
	}

	if s.cfg.DeleteAfterPublish {
		pool, err := pgxpool.New(ctx, s.cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("outbox: connect for delete-after-publish: %w", err)
		}
		s.pool = pool
	}

	s.logger.Info("outbox sink initialized",
		slog.String("table", s.cfg.Schema+"."+s.cfg.Table),
		slog.String("stream_prefix", s.cfg.StreamPrefix),
		slog.Bool("delete_after_publish", s.cfg.DeleteAfterPublish),
	)
	return nil
}

func (s *Sink) Process(ctx context.Context, event domain.CDCEvent) error {
	if event.Schema != s.cfg.Schema || event.Table != s.cfg.Table {
		return nil
	}
	// Only new outbox entries become messages. Updates and deletes are
	// housekeeping (including our own delete-after-publish), and READ
	// events from a backfill would republish historical messages.
	if event.Operation != domain.OperationInsert {
		return nil
	}

	streamKey, fields, err := buildMessage(s.cfg, event)
	if err != nil {
		return err
	}

	if err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: s.cfg.MaxStreamLen,
		Approx: true,
		Values: fields,
	}).Err(); err != nil {
		return fmt.Errorf("outbox: publish to %s: %w", streamKey, err)
	}

	s.logger.Debug("outbox message published",
		slog.String("stream", streamKey),
		slog.Any("id", fields["id"]),
	)

	if s.cfg.DeleteAfterPublish {
		s.deleteRow(ctx, event)
	}
	return nil
}

// buildMessage resolves the destination stream and message fields for one
// outbox row. Split out for testability.
func buildMessage(cfg Config, event domain.CDCEvent) (string, map[string]any, error) {
	row := event.NewData

	topic := stringValue(row[cfg.Columns.Topic])
	if topic == "" {
		topic = cfg.DefaultTopic
	}
	if topic == "" {
		return "", nil, fmt.Errorf(
			"outbox row has no %q value and no default_topic is configured (event %s)",
			cfg.Columns.Topic, event.ID)
	}

	payload, ok := row[cfg.Columns.Payload]
	if !ok || payload == nil {
		return "", nil, fmt.Errorf(
			"outbox row has no %q value (event %s)", cfg.Columns.Payload, event.ID)
	}
	payloadStr, err := payloadString(payload)
	if err != nil {
		return "", nil, fmt.Errorf("outbox: serialize payload (event %s): %w", event.ID, err)
	}

	id := stringValue(row[cfg.Columns.ID])
	if id == "" {
		id = event.ID
	}

	fields := map[string]any{
		"id":      id,
		"payload": payloadStr,
	}
	if v := stringValue(row[cfg.Columns.EventType]); v != "" {
		fields["type"] = v
	}
	if v := stringValue(row[cfg.Columns.Key]); v != "" {
		fields["key"] = v
	}

	return cfg.StreamPrefix + ":" + topic, fields, nil
}

func (s *Sink) deleteRow(ctx context.Context, event domain.CDCEvent) {
	id, ok := event.NewData[s.cfg.Columns.ID]
	if !ok || id == nil {
		s.logger.Warn("cannot delete published outbox row, no id column",
			slog.String("event_id", event.ID))
		return
	}

	// Deletion is housekeeping: the message is already published, so a
	// failure here must not fail (and redeliver) the event. Leftover rows
	// are harmless and can be swept by any cleanup job.
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s = $1",
		pgx.Identifier{s.cfg.Schema}.Sanitize(),
		pgx.Identifier{s.cfg.Table}.Sanitize(),
		pgx.Identifier{s.cfg.Columns.ID}.Sanitize(),
	)
	if _, err := s.pool.Exec(ctx, query, fmt.Sprintf("%v", id)); err != nil {
		s.logger.Warn("failed to delete published outbox row",
			slog.Any("id", id),
			slog.String("error", err.Error()),
		)
	}
}

func (s *Sink) Shutdown(context.Context) error {
	if s.pool != nil {
		s.pool.Close()
	}
	return s.client.Close()
}

func (s *Sink) HealthCheck(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func stringValue(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func payloadString(payload any) (string, error) {
	switch p := payload.(type) {
	case string:
		return p, nil
	case []byte:
		return string(p), nil
	default:
		// jsonb columns decode to maps/slices; re-serialize them.
		data, err := sonic.Marshal(p)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
}
