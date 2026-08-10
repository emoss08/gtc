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
type Sink struct {
	inner   domain.Sink
	manager *Manager
}

var _ domain.Sink = (*Sink)(nil)

func (s *Sink) Name() string                          { return s.inner.Name() }
func (s *Sink) Initialize(ctx context.Context) error  { return s.inner.Initialize(ctx) }
func (s *Sink) Shutdown(ctx context.Context) error    { return s.inner.Shutdown(ctx) }
func (s *Sink) HealthCheck(ctx context.Context) error { return s.inner.HealthCheck(ctx) }

func (s *Sink) Process(ctx context.Context, event domain.CDCEvent) error {
	err := s.inner.Process(ctx, event)
	if err == nil {
		s.manager.clearFailure(s.inner.Name(), event.ID)
		return nil
	}

	// An open circuit means the sink is down, not that this event is
	// poison. Propagate so the pipeline stalls until the sink recovers.
	if errors.Is(err, domain.ErrCircuitOpen) {
		return err
	}

	attempts, firstFailed, thresholdReached := s.manager.recordFailure(s.inner.Name(), event.ID)
	if !thresholdReached {
		// Propagate: the stream tears down and replays, giving the event
		// another full resilience cycle before it is considered poison.
		return err
	}

	if parkErr := s.manager.park(ctx, s.inner.Name(), event, err, attempts, firstFailed); parkErr != nil {
		// Cannot park (DLQ full or Redis down): keep stalling rather than
		// lose the event.
		return errors.Join(err, parkErr)
	}

	// Parked: report success so the WAL position can advance past it.
	return nil
}
