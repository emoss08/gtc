package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

// publishTimeout bounds a publish so a slow Redis cannot stall the WAL
// stream; schema notifications are advisory and a drop is reported loudly.
const publishTimeout = 5 * time.Second

// RedisPublisher writes schema changes to a single Redis stream,
// "<prefix>:schema", so consumers can subscribe to DDL without parsing every
// table's row stream. Ordering relative to row events is conveyed by the LSN
// carried in each entry.
type RedisPublisher struct {
	client *redis.Client
	stream string
	maxLen int64
	logger *slog.Logger
}

var _ ports.SchemaObserver = (*RedisPublisher)(nil)

type RedisPublisherParams struct {
	URL    string
	Prefix string
	MaxLen int64
	Logger *slog.Logger
}

func NewRedisPublisher(p RedisPublisherParams) (*RedisPublisher, error) {
	opts, err := redis.ParseURL(p.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}
	return &RedisPublisher{
		client: redis.NewClient(opts),
		stream: p.Prefix + ":schema",
		maxLen: p.MaxLen,
		logger: p.Logger.With(slog.String("component", "schema_publisher")),
	}, nil
}

// Stream is the Redis stream key schema changes are written to.
func (p *RedisPublisher) Stream() string { return p.stream }

func (p *RedisPublisher) OnSchemaChange(ctx context.Context, change domain.SchemaChange) {
	payload, err := json.Marshal(change)
	if err != nil {
		p.fail("marshal schema change", change, err)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		MaxLen: p.maxLen,
		Approx: true,
		Values: map[string]any{
			"table":    change.FullTableName(),
			"lsn":      change.LSN,
			"breaking": fmt.Sprint(change.Breaking()),
			"payload":  string(payload),
		},
	}).Err(); err != nil {
		p.fail("publish schema change", change, err)
		return
	}

	p.logger.Debug("schema change published",
		slog.String("stream", p.stream),
		slog.String("table", change.FullTableName()),
	)
}

// fail reports a dropped notification. Schema changes are best-effort by
// design (see ports.SchemaObserver), so this never propagates: it must not
// tear down replication, and the change survives in the in-memory history
// and the metrics either way.
func (p *RedisPublisher) fail(what string, change domain.SchemaChange, err error) {
	metrics.SchemaPublishErrors.WithLabelValues("redis").Inc()
	p.logger.Error("schema change notification dropped",
		slog.String("operation", what),
		slog.String("table", change.FullTableName()),
		slog.String("lsn", change.LSN),
		slog.String("error", err.Error()),
	)
}

func (p *RedisPublisher) Close() error { return p.client.Close() }
