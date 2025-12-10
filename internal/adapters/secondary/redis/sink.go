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

func (s *StreamSink) Initialize(ctx context.Context) error {
	s.Logger.Debug("initializing redis stream sink",
		slog.String("prefix", s.Config.Prefix),
		slog.Int64("max_stream_len", s.maxStreamLen),
	)

	if err := s.Client.Ping(ctx).Err(); err != nil {
		s.Logger.Error("failed to connect to redis", slog.String("error", err.Error()))
		return err
	}

	s.Logger.Info("redis stream sink initialized")
	return nil
}

func (s *StreamSink) Process(ctx context.Context, event domain.CDCEvent) error {
	if !s.ShouldProcess(event) {
		s.Logger.Debug("skipping event, table not configured",
			slog.String("table", event.FullTableName()),
			slog.String("event_id", event.ID),
		)
		return nil
	}

	streamKey, err := s.GenerateKey(event)
	if err != nil {
		return err
	}

	payload, err := sonic.Marshal(map[string]any{
		"operation": event.Operation.String(),
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"metadata":  event.Metadata,
	})
	if err != nil {
		s.Logger.Error("failed to marshal payload",
			slog.String("error", err.Error()),
			slog.String("event_id", event.ID),
		)
		return fmt.Errorf("marshal payload: %w", err)
	}

	if err := s.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: s.maxStreamLen,
		Approx: true,
		Values: map[string]any{
			"event_id": event.ID,
			"payload":  string(payload),
		},
	}).Err(); err != nil {
		s.Logger.Error("failed to add to stream",
			slog.String("error", err.Error()),
			slog.String("stream", streamKey),
			slog.String("event_id", event.ID),
		)
		return err
	}

	s.Logger.Debug("event added to stream",
		slog.String("stream", streamKey),
		slog.String("event_id", event.ID),
		slog.String("operation", event.Operation.String()),
	)

	return nil
}
