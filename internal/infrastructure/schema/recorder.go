// Package schema records and publishes DDL changes detected on published
// tables: an in-memory history for the dashboard and API, and an optional
// Redis stream for downstream consumers that need to react to them.
package schema

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

// historyCapacity bounds the in-memory schema history. DDL is rare, so this
// covers a long window while staying trivially small.
const historyCapacity = 200

// Recorder keeps recent schema changes, counts them, and logs them —
// breaking changes at WARN so they surface in ordinary log alerting.
type Recorder struct {
	mu      sync.RWMutex
	history []domain.SchemaChange
	logger  *slog.Logger
}

var _ ports.SchemaObserver = (*Recorder)(nil)

func NewRecorder(logger *slog.Logger) *Recorder {
	return &Recorder{
		history: make([]domain.SchemaChange, 0, historyCapacity),
		logger:  logger.With(slog.String("component", "schema")),
	}
}

func (r *Recorder) OnSchemaChange(_ context.Context, change domain.SchemaChange) {
	for _, kind := range change.Kinds {
		metrics.SchemaChangesTotal.WithLabelValues(change.Schema, change.Table, kind).Inc()
	}

	attrs := []any{
		slog.String("table", change.FullTableName()),
		slog.String("kinds", strings.Join(change.Kinds, ",")),
		slog.String("change", change.Summary()),
		slog.String("lsn", change.LSN),
	}
	if change.Breaking() {
		// Consumers holding documents for this table may need attention:
		// data was removed, retyped, or is addressed differently now.
		r.logger.Warn("breaking schema change detected", attrs...)
	} else {
		r.logger.Info("schema change detected", attrs...)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.history = append(r.history, change)
	if len(r.history) > historyCapacity {
		r.history = r.history[len(r.history)-historyCapacity:]
	}
}

// History returns the recorded changes, newest first.
func (r *Recorder) History() []domain.SchemaChange {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]domain.SchemaChange, len(r.history))
	for i, change := range r.history {
		out[len(r.history)-1-i] = change
	}
	return out
}

// Observers fans a change out to several observers in order.
type Observers []ports.SchemaObserver

var _ ports.SchemaObserver = (Observers)(nil)

func (o Observers) OnSchemaChange(ctx context.Context, change domain.SchemaChange) {
	for _, observer := range o {
		observer.OnSchemaChange(ctx, change)
	}
}
