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

type BatchingSink interface {
	Sink
	ProcessBatch(ctx context.Context, events []CDCEvent) error
	FlushTimeout() time.Duration
	MaxBatchSize() int
}

func SupportsBatching(s Sink) bool {
	_, ok := s.(BatchingSink)
	return ok
}

type DeadLetterEntry struct {
	Event     CDCEvent
	SinkName  string
	Error     error
	Attempts  int
	Timestamp time.Time
}
