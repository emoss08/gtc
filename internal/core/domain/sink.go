package domain

import (
	"context"
	"time"
)

// Sink delivers change events to one destination.
//
// Process receives a batch of events in stream order and must apply them in
// that order: two events for the same row may share a batch, and the later
// one wins. A nil error means every event in the batch is durably applied;
// any error means the whole batch is redelivered (at-least-once), so sinks
// must tolerate duplicates. A single event is a batch of one.
//
// Errors caused by the data itself rather than by the destination (an
// unrenderable key, a document the destination rejects) should be wrapped
// with Permanent: they are not retried and do not trip the circuit breaker,
// so the dead-letter layer can isolate the offending event quickly.
type Sink interface {
	Name() string
	Initialize(ctx context.Context) error
	Process(ctx context.Context, events []CDCEvent) error
	Shutdown(ctx context.Context) error
	HealthCheck(ctx context.Context) error
}

type SinkResult struct {
	SinkName string
	Success  bool
	Error    error
	Duration time.Duration
}

// BatchLimiter is implemented by sinks whose unit of delivery is smaller than
// a batch — a webhook posts one request per event, so its timeout and retry
// budget belong to one event, not to thousands. The pipeline splits batches
// to this size before the dead-letter and retry layers see them. Zero or
// less means no limit.
type BatchLimiter interface {
	MaxBatchEvents() int
}

// MaxBatchEvents returns the sink's batch limit, or 0 when it has none.
func MaxBatchEvents(s Sink) int {
	if l, ok := s.(BatchLimiter); ok {
		return l.MaxBatchEvents()
	}
	return 0
}

// Router is implemented by sinks that know which events they consume. Wants
// reports whether the sink might process events of op for schema.table; a sink
// that cannot tell must answer true. The backfill uses it to skip tables no
// sink would receive rows for.
type Router interface {
	Wants(schema, table string, op Operation) bool
}

// Wants asks the sink whether it might consume op events of schema.table.
// Sinks that do not implement Router are assumed to want everything.
func Wants(s Sink, schema, table string, op Operation) bool {
	if r, ok := s.(Router); ok {
		return r.Wants(schema, table, op)
	}
	return true
}
