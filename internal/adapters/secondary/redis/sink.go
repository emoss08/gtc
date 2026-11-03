package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/redis/go-redis/v9"
)

type StreamSink struct {
	*BaseSink
	maxStreamLen int64
}

var _ domain.Sink = (*StreamSink)(nil)

type StreamSinkParams struct {
	Config      Config
	KeyResolver *KeyResolver
	Logger      *slog.Logger
}

func NewSink(p StreamSinkParams) (*StreamSink, error) {
	base, err := NewBaseSink(BaseSinkParams{
		Name: "redis-stream",
		Config: BaseConfig{
			URL:         p.Config.URL,
			Prefix:      p.Config.StreamPrefix,
			KeyResolver: p.KeyResolver,
		},
		Logger: p.Logger,
	})
	if err != nil {
		return nil, err
	}

	return &StreamSink{
		BaseSink:     base,
		maxStreamLen: p.Config.MaxStreamLen,
	}, nil
}

// Process appends the batch to its streams in one pipelined round trip.
// Entries keep stream order because a pipeline runs on one connection.
func (s *StreamSink) Process(ctx context.Context, events []domain.CDCEvent) error {
	pipe := s.Client.Pipeline()
	queued := 0

	for i := range events {
		event := &events[i]
		if !s.ShouldProcess(*event) {
			continue
		}

		streamKey, err := s.GenerateKey(*event)
		if err != nil {
			return domain.Permanent(err)
		}

		payload, err := sonic.Marshal(map[string]any{
			"operation": event.Operation.String(),
			// Several tables may share one stream; schema and table say which
			// table each entry came from.
			"schema":   event.Schema,
			"table":    event.Table,
			"old_data": event.OldData,
			"new_data": event.NewData,
			// Columns absent from new_data because their TOASTed value was
			// unchanged; consumers must not treat them as deleted.
			"unchanged_toast_columns": event.UnchangedToastColumns,
			"metadata":                event.Metadata,
		})
		if err != nil {
			return domain.Permanent(fmt.Errorf("marshal payload for event %s: %w", event.ID, err))
		}

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: s.maxStreamLen,
			Approx: true,
			Values: []any{"event_id", event.ID, "payload", payload},
		})
		queued++
	}

	if queued == 0 {
		return nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("append %d stream entries: %w", queued, err)
	}
	return nil
}
