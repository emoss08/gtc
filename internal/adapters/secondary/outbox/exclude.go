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

func (s *excludingSink) Process(ctx context.Context, event domain.CDCEvent) error {
	if event.Schema == s.schema && event.Table == s.table {
		return nil
	}
	return s.Sink.Process(ctx, event)
}
