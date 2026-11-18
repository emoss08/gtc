package services

import (
	"log/slog"
	"sync"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

// stallLogInterval is how often a sink that stays unavailable is reported
// again after the first warning.
const stallLogInterval = time.Minute

// stalls tracks the sinks the pipeline is waiting for, for readiness, the
// cdc_sink_stalled gauge and the log: one warning when a sink stalls, one a
// minute while it stays down, and one line when it is back.
type stalls struct {
	logger *slog.Logger

	mu     sync.Mutex
	bySink map[string]*stall
}

type stall struct {
	since    time.Time
	attempts int
	err      string
	loggedAt time.Time
}

func newStalls(logger *slog.Logger) *stalls {
	return &stalls{logger: logger, bySink: make(map[string]*stall)}
}

// record notes a failed delivery to an unavailable sink. The first one of an
// outage starts the stall; a stall that continues across stream replays keeps
// its start.
func (t *stalls) record(sink string, err error) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	st, ok := t.bySink[sink]
	if !ok {
		st = &stall{since: now, loggedAt: now}
		t.bySink[sink] = st
		metrics.SinkStalled.WithLabelValues(sink).Set(1)
		t.logger.Warn("sink unavailable: holding the stream position until it recovers; nothing is dead-lettered",
			slog.String("sink", sink),
			slog.String("error", err.Error()),
		)
	} else {
		metrics.SinkStallRetries.WithLabelValues(sink).Inc()
	}
	st.attempts++
	st.err = err.Error()

	if now.Sub(st.loggedAt) >= stallLogInterval {
		st.loggedAt = now
		t.logger.Warn("sink still unavailable; stream held",
			slog.String("sink", sink),
			slog.Duration("stalled_for", now.Sub(st.since).Round(time.Second)),
			slog.Int("attempts", st.attempts),
			slog.String("error", st.err),
		)
	}
}

// clear ends a sink's stall: it took a batch, or answered with a failure that
// is about the data rather than its availability.
func (t *stalls) clear(sink string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, ok := t.bySink[sink]
	if !ok {
		return
	}
	delete(t.bySink, sink)
	metrics.SinkStalled.WithLabelValues(sink).Set(0)
	t.logger.Info("sink available again; stream resumed",
		slog.String("sink", sink),
		slog.Duration("stalled_for", time.Since(st.since).Round(time.Millisecond)),
		slog.Int("attempts", st.attempts),
	)
}

func (t *stalls) list(order []string) []domain.SinkStall {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.bySink) == 0 {
		return nil
	}
	out := make([]domain.SinkStall, 0, len(t.bySink))
	for _, name := range order {
		if st, ok := t.bySink[name]; ok {
			out = append(out, domain.SinkStall{
				Sink:     name,
				Since:    st.since,
				Attempts: st.attempts,
				Error:    st.err,
			})
		}
	}
	return out
}
