package domain

import (
	"context"
	"time"
)

type Sink interface {
	Name() string
	Initialize(ctx context.Context) error
	Process(ctx context.Context, event CDCEvent) error
	Shutdown(ctx context.Context) error
	HealthCheck(ctx context.Context) error
}

type SinkResult struct {
	SinkName string
	Success  bool
	Error    error
	Duration time.Duration
}
