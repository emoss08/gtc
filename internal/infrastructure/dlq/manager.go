package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

// Manager owns the dead-letter queue: it counts repeated failures of the
// same event per sink, parks poison events once a threshold is reached, and
// serves the triage API (list/retry/discard). Retries are delivered to the
// sink layer registered by the decorator — below transforms, so a parked
// event is never re-transformed (masking is not idempotent).
type Manager struct {
	store        Store
	threshold    int
	retryTimeout time.Duration
	logger       *slog.Logger

	mu       sync.Mutex
	failures map[string]failureRecord // key: "<sink>:<event_id>"
	sinks    map[string]domain.Sink   // sink name -> resilience-wrapped sink
}

type failureRecord struct {
	count int
	first time.Time
}

var _ ports.DLQManager = (*Manager)(nil)

type ManagerParams struct {
	Store Store
	// Threshold is how many times the same event must fail a sink (one
	// full resilience cycle each, across stream replays) before parking.
	Threshold    int
	RetryTimeout time.Duration
	Logger       *slog.Logger
}

func NewManager(p ManagerParams) *Manager {
	if p.Threshold <= 0 {
		p.Threshold = 3
	}
	if p.RetryTimeout <= 0 {
		p.RetryTimeout = 10 * time.Second
	}
	return &Manager{
		store:        p.Store,
		threshold:    p.Threshold,
		retryTimeout: p.RetryTimeout,
		logger:       p.Logger.With(slog.String("component", "dlq")),
		failures:     make(map[string]failureRecord),
		sinks:        make(map[string]domain.Sink),
	}
}

// WrapSink decorates a (resilience-wrapped) sink with dead-lettering and
// registers it as the retry target for its entries.
func (m *Manager) WrapSink(inner domain.Sink) domain.Sink {
	m.mu.Lock()
	m.sinks[inner.Name()] = inner
	m.mu.Unlock()
	return &Sink{inner: inner, manager: m}
}

// recordFailure increments the failure count for (sink, event) and reports
// whether the parking threshold has been reached.
func (m *Manager) recordFailure(sinkName, eventID string) (int, time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := sinkName + ":" + eventID
	rec := m.failures[key]
	if rec.count == 0 {
		rec.first = time.Now().UTC()
	}
	rec.count++
	m.failures[key] = rec
	return rec.count, rec.first, rec.count >= m.threshold
}

func (m *Manager) clearFailure(sinkName, eventID string) {
	m.mu.Lock()
	delete(m.failures, sinkName+":"+eventID)
	m.mu.Unlock()
}

func (m *Manager) park(
	ctx context.Context,
	sinkName string,
	event domain.CDCEvent,
	cause error,
	attempts int,
	firstFailed time.Time,
) error {
	entry := domain.DeadLetterEntry{
		ID:            sinkName + ":" + event.ID,
		SinkName:      sinkName,
		Schema:        event.Schema,
		Table:         event.Table,
		Operation:     event.Operation.String(),
		LSN:           event.Metadata.LSN,
		Error:         cause.Error(),
		Attempts:      attempts,
		FirstFailedAt: firstFailed,
		LastFailedAt:  time.Now().UTC(),
		Event:         event,
	}

	if err := m.store.Park(ctx, entry); err != nil {
		return err
	}

	m.clearFailure(sinkName, event.ID)
	metrics.DLQParkedTotal.WithLabelValues(sinkName).Inc()
	m.updateDepthGauge(ctx)

	m.logger.Warn("event parked in dead-letter queue",
		slog.String("entry_id", entry.ID),
		slog.String("sink", sinkName),
		slog.String("table", entry.Schema+"."+entry.Table),
		slog.String("error", entry.Error),
		slog.Int("attempts", attempts),
	)
	return nil
}

// --- ports.DLQManager ---

func (m *Manager) List(ctx context.Context, limit int) ([]domain.DeadLetterEntry, error) {
	return m.store.List(ctx, limit)
}

func (m *Manager) Len(ctx context.Context) (int64, error) {
	return m.store.Len(ctx)
}

func (m *Manager) Retry(ctx context.Context, id string) error {
	entry, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}

	m.mu.Lock()
	sink, ok := m.sinks[entry.SinkName]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("sink %q is not active in this instance", entry.SinkName)
	}

	retryCtx, cancel := context.WithTimeout(ctx, m.retryTimeout)
	defer cancel()

	if procErr := sink.Process(retryCtx, entry.Event); procErr != nil {
		entry.Attempts++
		entry.Error = procErr.Error()
		entry.LastFailedAt = time.Now().UTC()
		if parkErr := m.store.Park(ctx, *entry); parkErr != nil {
			m.logger.Error("failed to update dead-letter entry after retry",
				slog.String("entry_id", id), slog.String("error", parkErr.Error()))
		}
		metrics.DLQRetriedTotal.WithLabelValues(entry.SinkName, "failure").Inc()
		return fmt.Errorf("retry failed: %w", procErr)
	}

	if err := m.store.Remove(ctx, id); err != nil {
		return fmt.Errorf("retry succeeded but entry removal failed: %w", err)
	}
	metrics.DLQRetriedTotal.WithLabelValues(entry.SinkName, "success").Inc()
	m.updateDepthGauge(ctx)

	m.logger.Info("dead-letter entry retried successfully",
		slog.String("entry_id", id), slog.String("sink", entry.SinkName))
	return nil
}

func (m *Manager) RetryAll(ctx context.Context) (domain.RetryResult, error) {
	entries, err := m.store.List(ctx, 0)
	if err != nil {
		return domain.RetryResult{}, err
	}

	result := domain.RetryResult{}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		result.Retried++
		if err := m.Retry(ctx, entry.ID); err != nil {
			result.Failed++
		} else {
			result.Succeeded++
		}
	}
	return result, nil
}

func (m *Manager) Discard(ctx context.Context, id string) error {
	if err := m.store.Remove(ctx, id); err != nil {
		return err
	}
	m.updateDepthGauge(ctx)
	m.logger.Info("dead-letter entry discarded", slog.String("entry_id", id))
	return nil
}

func (m *Manager) updateDepthGauge(ctx context.Context) {
	if n, err := m.store.Len(ctx); err == nil {
		metrics.DLQEntries.Set(float64(n))
	}
}
