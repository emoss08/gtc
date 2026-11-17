package dlq

import (
	"context"
	"errors"

	"github.com/emoss08/gtc/internal/core/domain"
)

// Sink decorates a resilience-wrapped sink with dead-lettering: an event the
// sink rejects is parked so the pipeline can advance, while a sink that is
// down stalls the pipeline and parks nothing — a down sink is not a poison
// event, and diverting the stream to the DLQ while it is down would lose
// those changes from the sink until someone retried them by hand.
//
// The one rule: only a failure about the event may park it. An error that
// domain.IsUnavailable recognizes (the sink marked it, an open circuit, a
// timeout, a transport failure, or a failing health check the resilience
// layer found) is about the sink; it is never counted toward the threshold
// and never isolated. It propagates, and the CDC service holds the batch and
// retries it until the sink is back.
//
// A batch that fails for its data is isolated by bisection: halves are
// retried in order, the halves that succeed are delivered, and recursion
// narrows the failure to single events, which are parked. A single poison
// row therefore costs O(log n) sink calls rather than stalling the stream or
// forcing the batch through one-at-a-time delivery. A replay re-sends what
// already succeeded, which sinks tolerate (at-least-once). If the sink goes
// down part-way, isolation stops and the batch stalls whole.
//
// When isolation starts depends on the failure: a permanent (data) error is
// isolated immediately. Any other error from a sink that is up is first
// given Threshold full cycles (resilience retries plus a stream replay each),
// counted against the batch's first event — stable across replays, because
// the stream resumes at the last confirmed position.
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
// the events that fail on their own. It stops at the first failure that is
// about the sink rather than the data.
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

// stalls reports failures that must never park anything: the sink is down,
// slow or refusing every write (domain.IsUnavailable), or the context is done
// (shutdown). They propagate so the batch is held and delivered later.
func stalls(ctx context.Context, err error) bool {
	return ctx.Err() != nil || domain.IsUnavailable(err)
}
