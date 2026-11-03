package dlq

import (
	"context"
	"errors"

	"github.com/emoss08/gtc/internal/core/domain"
)

// Sink decorates a resilience-wrapped sink with dead-lettering: an event
// that keeps failing the same sink is parked so the pipeline can advance,
// while sink outages (open circuit) still stall — a down sink is not a
// poison event, and diverting the whole stream to the DLQ would be wrong.
//
// A batch that fails is isolated by bisection: halves are retried in order,
// the halves that succeed are delivered, and recursion narrows the failure to
// single events, which are parked. A single poison row therefore costs
// O(log n) sink calls rather than stalling the stream or forcing the batch
// through one-at-a-time delivery. A replay re-sends what already succeeded,
// which sinks tolerate (at-least-once).
//
// When isolation starts depends on the failure: a permanent (data) error is
// isolated immediately. A transient error is first given Threshold full
// cycles (resilience retries plus a stream replay each), counted against the
// batch's first event — stable across replays, because the stream resumes at
// the last confirmed position — since it more likely means a sick sink than a
// bad row.
type Sink struct {
	inner   domain.Sink
	manager *Manager
}

var _ domain.Sink = (*Sink)(nil)

func (s *Sink) Name() string                          { return s.inner.Name() }
func (s *Sink) Initialize(ctx context.Context) error  { return s.inner.Initialize(ctx) }
func (s *Sink) Shutdown(ctx context.Context) error    { return s.inner.Shutdown(ctx) }
func (s *Sink) HealthCheck(ctx context.Context) error { return s.inner.HealthCheck(ctx) }
func (s *Sink) MaxBatchEvents() int                   { return domain.MaxBatchEvents(s.inner) }
func (s *Sink) Wants(schema, table string, op domain.Operation) bool {
	return domain.Wants(s.inner, schema, table, op)
}

func (s *Sink) Process(ctx context.Context, events []domain.CDCEvent) error {
	if len(events) == 0 {
		return nil
	}

	err := s.inner.Process(ctx, events)
	if err == nil {
		s.manager.clearFailures(s.inner.Name(), events)
		return nil
	}
	if stalls(ctx, err) {
		return err
	}

	if !domain.IsPermanent(err) {
		if _, _, reached := s.manager.recordFailure(s.inner.Name(), events[0].ID); !reached {
			// Propagate: the stream tears down and replays, giving the
			// batch another full resilience cycle.
			return err
		}
	}
	return s.isolate(ctx, events, err)
}

// isolate delivers what it can of a batch known to fail with err and parks
// the events that fail on their own.
func (s *Sink) isolate(ctx context.Context, events []domain.CDCEvent, err error) error {
	if len(events) == 1 {
		return s.park(ctx, events[0], err)
	}

	mid := len(events) / 2
	for _, half := range [][]domain.CDCEvent{events[:mid], events[mid:]} {
		herr := s.inner.Process(ctx, half)
		if herr == nil {
			s.manager.clearFailures(s.inner.Name(), half)
			continue
		}
		if stalls(ctx, herr) {
			return herr
		}
		if ierr := s.isolate(ctx, half, herr); ierr != nil {
			return ierr
		}
	}
	return nil
}

func (s *Sink) park(ctx context.Context, event domain.CDCEvent, cause error) error {
	attempts, firstFailed := s.manager.failureInfo(s.inner.Name(), event.ID)
	if parkErr := s.manager.park(ctx, s.inner.Name(), event, cause, attempts, firstFailed); parkErr != nil {
		// Cannot park (DLQ full or Redis down): keep stalling rather than
		// lose the event.
		return errors.Join(cause, parkErr)
	}
	// Parked: the WAL position can advance past it.
	return nil
}

// stalls reports failures that must never park anything: an open circuit
// means the sink is down and a timeout means it is slow — neither says an
// event is poison — and a done context means shutdown. They propagate so the
// batch is redelivered later.
func stalls(ctx context.Context, err error) bool {
	return errors.Is(err, domain.ErrCircuitOpen) ||
		errors.Is(err, context.DeadlineExceeded) ||
		ctx.Err() != nil
}
