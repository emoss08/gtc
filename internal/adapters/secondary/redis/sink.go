package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/redis/go-redis/v9"
)

type Sink struct {
	client *redis.Client
	config Config
	logger *slog.Logger
}

func NewSink(cfg Config, logger *slog.Logger) (*Sink, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}

	return &Sink{
		client: redis.NewClient(opts),
		config: cfg,
		logger: logger.With(slog.String("component", "redis_sink")),
	}, nil
}

func (s *Sink) Name() string {
	return "redis"
}

func (s *Sink) Initialize(ctx context.Context) error {
	s.logger.Debug("initializing redis sink",
		slog.String("stream_prefix", s.config.StreamPrefix),
		slog.Int64("max_stream_len", s.config.MaxStreamLen),
	)

	if err := s.client.Ping(ctx).Err(); err != nil {
		s.logger.Error("failed to connect to redis", slog.String("error", err.Error()))
		return err
	}

	s.logger.Info("redis sink initialized")
	return nil
}

func (s *Sink) Process(ctx context.Context, event domain.CDCEvent) error {
	streamKey := fmt.Sprintf("%s:%s:%s", s.config.StreamPrefix, event.Schema, event.Table)

	payload, err := json.Marshal(map[string]any{
		"operation": event.Operation.String(),
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"metadata":  event.Metadata,
	})
	if err != nil {
		s.logger.Error("failed to marshal payload",
			slog.String("error", err.Error()),
			slog.String("event_id", event.ID),
		)
		return fmt.Errorf("marshal payload: %w", err)
	}

	if err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: s.config.MaxStreamLen,
		Approx: true,
		Values: map[string]any{
			"event_id": event.ID,
			"payload":  string(payload),
		},
	}).Err(); err != nil {
		s.logger.Error("failed to add to stream",
			slog.String("error", err.Error()),
			slog.String("stream", streamKey),
			slog.String("event_id", event.ID),
		)
		return err
	}

	s.logger.Debug("event added to stream",
		slog.String("stream", streamKey),
		slog.String("event_id", event.ID),
		slog.String("operation", event.Operation.String()),
	)

	return nil
}

func (s *Sink) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down redis sink")

	if err := s.client.Close(); err != nil {
		s.logger.Error("failed to close redis connection", slog.String("error", err.Error()))
		return err
	}

	s.logger.Info("redis sink shutdown complete")
	return nil
}

func (s *Sink) HealthCheck(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}
