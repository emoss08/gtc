package redisjson

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/emoss08/gtc/internal/adapters/secondary/redis"
	"github.com/emoss08/gtc/internal/core/domain"
)

type JSONSink struct {
	*redis.BaseSink
}

var _ domain.Sink = (*JSONSink)(nil)

type JSONSinkParams struct {
	Config      Config
	KeyResolver *redis.KeyResolver
	Logger      *slog.Logger
}

func NewSink(p JSONSinkParams) (*JSONSink, error) {
	base, err := redis.NewBaseSink(redis.BaseSinkParams{
		Name: "redis-json",
		Config: redis.BaseConfig{
			URL:         p.Config.URL,
			Prefix:      p.Config.Prefix,
			KeyResolver: p.KeyResolver,
		},
		Logger: p.Logger,
	})
	if err != nil {
		return nil, err
	}

	return &JSONSink{
		BaseSink: base,
	}, nil
}

func (s *JSONSink) Process(ctx context.Context, event domain.CDCEvent) error {
	if !s.ShouldProcess(event) {
		s.Logger.Debug("skipping event, table not configured",
			slog.String("table", event.FullTableName()),
			slog.String("event_id", event.ID),
		)
		return nil
	}

	key, err := s.GenerateKey(event)
	if err != nil {
		return err
	}

	switch event.Operation {
	case domain.OperationInsert, domain.OperationUpdate:
		return s.setJSON(ctx, key, event)
	case domain.OperationDelete:
		return s.deleteKey(ctx, key, event)
	case domain.OperationTruncate:
		s.Logger.Warn("truncate operation not supported for redis json sink",
			slog.String("table", event.FullTableName()),
		)
		return nil
	default:
		s.Logger.Warn("unknown operation",
			slog.String("operation", event.Operation.String()),
			slog.String("event_id", event.ID),
		)
		return nil
	}
}

func (s *JSONSink) setJSON(ctx context.Context, key string, event domain.CDCEvent) error {
	data := event.NewData
	if data == nil {
		s.Logger.Warn("no new data for insert/update",
			slog.String("event_id", event.ID),
			slog.String("key", key),
		)
		return nil
	}

	jsonData, err := sonic.Marshal(data)
	if err != nil {
		s.Logger.Error("failed to marshal data",
			slog.String("error", err.Error()),
			slog.String("event_id", event.ID),
		)
		return fmt.Errorf("marshal data: %w", err)
	}

	// When an UPDATE omits unchanged TOAST columns, a full JSON.SET would
	// erase their previously stored values, so merge into the existing
	// document instead. Note JSON.MERGE (RFC 7386) deletes fields whose
	// incoming value is null, which matches a column being set to NULL.
	command := "JSON.SET"
	if event.Operation == domain.OperationUpdate && len(event.UnchangedToastColumns) > 0 {
		command = "JSON.MERGE"
	}

	if err := s.Client.Do(ctx, command, key, "$", string(jsonData)).Err(); err != nil {
		s.Logger.Error("failed to write json",
			slog.String("error", err.Error()),
			slog.String("command", command),
			slog.String("key", key),
			slog.String("event_id", event.ID),
		)
		return fmt.Errorf("%s: %w", command, err)
	}

	s.Logger.Debug("json set",
		slog.String("key", key),
		slog.String("event_id", event.ID),
		slog.String("operation", event.Operation.String()),
	)

	return nil
}

func (s *JSONSink) deleteKey(ctx context.Context, key string, event domain.CDCEvent) error {
	if err := s.Client.Del(ctx, key).Err(); err != nil {
		s.Logger.Error("failed to delete key",
			slog.String("error", err.Error()),
			slog.String("key", key),
			slog.String("event_id", event.ID),
		)
		return fmt.Errorf("delete key: %w", err)
	}

	s.Logger.Debug("key deleted",
		slog.String("key", key),
		slog.String("event_id", event.ID),
	)

	return nil
}
