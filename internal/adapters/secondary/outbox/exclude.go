package outbox

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

// ExcludeTable wraps a sink so it never processes events for the given
// table. Used to keep the raw outbox table out of the data-mirroring sinks —
// its rows are messages, not data.
func ExcludeTable(inner domain.Sink, schema, table string) domain.Sink {
	return &excludingSink{Sink: inner, schema: schema, table: table}
}

type excludingSink struct {
	domain.Sink
	schema, table string
}

func (s *excludingSink) MaxBatchEvents() int { return domain.MaxBatchEvents(s.Sink) }

func (s *excludingSink) Wants(schema, table string, op domain.Operation) bool {
	if schema == s.schema && table == s.table {
		return false
	}
	return domain.Wants(s.Sink, schema, table, op)
}

func (s *excludingSink) Process(ctx context.Context, events []domain.CDCEvent) error {
	kept := events
	for i := range events {
		if events[i].Schema == s.schema && events[i].Table == s.table {
			// Copy only when something is actually dropped.
			kept = make([]domain.CDCEvent, 0, len(events)-1)
			kept = append(kept, events[:i]...)
			for _, e := range events[i+1:] {
				if e.Schema != s.schema || e.Table != s.table {
					kept = append(kept, e)
				}
			}
			break
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return s.Sink.Process(ctx, kept)
}
