package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

type SinkRegistry interface {
	Register(sink domain.Sink) error
	Unregister(name string) error
	Get(name string) (domain.Sink, error)
	All() []domain.Sink
	InitializeAll(ctx context.Context) error
	ShutdownAll(ctx context.Context) error
	HealthCheckAll(ctx context.Context) map[string]error
}
