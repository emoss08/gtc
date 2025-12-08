package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/redis/go-redis/v9"
)

type BaseConfig struct {
	URL         string
	Prefix      string
	KeyResolver *KeyResolver
}

type BaseSinkParams struct {
	Name   string
	Config BaseConfig
	Logger *slog.Logger
}

type BaseSink struct {
	Client *redis.Client
	Config BaseConfig
	Logger *slog.Logger
	name   string
}

func NewBaseSink(p BaseSinkParams) (*BaseSink, error) {
	opts, err := redis.ParseURL(p.Config.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}

	return &BaseSink{
		Client: redis.NewClient(opts),
		Config: p.Config,
		Logger: p.Logger.With(slog.String("component", p.Name)),
		name:   p.Name,
	}, nil
}

func (s *BaseSink) Name() string {
	return s.name
}

func (s *BaseSink) Initialize(ctx context.Context) error {
	s.Logger.Debug("initializing sink",
		slog.String("prefix", s.Config.Prefix),
	)

	if err := s.Client.Ping(ctx).Err(); err != nil {
		s.Logger.Error("failed to connect to redis", slog.String("error", err.Error()))
		return err
	}

	s.Logger.Info("sink initialized")
	return nil
}

func (s *BaseSink) Shutdown(ctx context.Context) error {
	s.Logger.Info("shutting down sink")

	if err := s.Client.Close(); err != nil {
		s.Logger.Error("failed to close redis connection", slog.String("error", err.Error()))
		return err
	}

	s.Logger.Info("sink shutdown complete")
	return nil
}

func (s *BaseSink) HealthCheck(ctx context.Context) error {
	return s.Client.Ping(ctx).Err()
}

func (s *BaseSink) GenerateKey(event domain.CDCEvent) (string, error) {
	key, err := s.Config.KeyResolver.GenerateKey(event)
	if err != nil {
		s.Logger.Error("failed to generate key",
			slog.String("error", err.Error()),
			slog.String("event_id", event.ID),
			slog.String("table", event.FullTableName()),
		)
		return "", fmt.Errorf("generate key: %w", err)
	}
	return key, nil
}

var _ domain.Sink = (*BaseSink)(nil)

func (s *BaseSink) Process(ctx context.Context, event domain.CDCEvent) error {
	panic("BaseSink.Process must be overridden")
}
