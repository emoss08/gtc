package transform

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/infrastructure/config"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

// Sink decorates a domain.Sink with declarative transforms: a sink-level
// chain applied to every table, then a per-table chain. Each sink gets its
// own transformed copy of the event, so e.g. the stream sink can carry full
// rows while the search sink receives masked ones.
type Sink struct {
	inner  domain.Sink
	global *Chain
	tables map[string]*Chain // keyed as written in config: "schema.table" or "table"
	logger *slog.Logger
}

var _ domain.Sink = (*Sink)(nil)

// NewSink compiles the sink's transform specs and wraps inner. It returns
// inner unchanged when no transforms are configured.
func NewSink(
	inner domain.Sink,
	global *config.TransformSpec,
	tables map[string]config.TransformSpec,
	logger *slog.Logger,
) (domain.Sink, error) {
	var globalChain *Chain
	if global != nil {
		var err error
		globalChain, err = Compile(*global)
		if err != nil {
			return nil, fmt.Errorf("sink %s transform: %w", inner.Name(), err)
		}
	}

	tableChains := make(map[string]*Chain, len(tables))
	for name, spec := range tables {
		chain, err := Compile(spec)
		if err != nil {
			return nil, fmt.Errorf("sink %s, table %s transform: %w", inner.Name(), name, err)
		}
		if chain != nil {
			tableChains[name] = chain
		}
	}

	if globalChain == nil && len(tableChains) == 0 {
		return inner, nil
	}

	return &Sink{
		inner:  inner,
		global: globalChain,
		tables: tableChains,
		logger: logger.With(
			slog.String("component", "transform"),
			slog.String("sink", inner.Name()),
		),
	}, nil
}

func (s *Sink) Name() string                         { return s.inner.Name() }
func (s *Sink) Initialize(ctx context.Context) error { return s.inner.Initialize(ctx) }
func (s *Sink) Shutdown(ctx context.Context) error   { return s.inner.Shutdown(ctx) }
func (s *Sink) HealthCheck(ctx context.Context) error {
	return s.inner.HealthCheck(ctx)
}

func (s *Sink) Process(ctx context.Context, event domain.CDCEvent) error {
	event, keep, err := s.global.Apply(event)
	if err != nil {
		return err
	}
	if keep {
		event, keep, err = s.tableChain(event).Apply(event)
		if err != nil {
			return err
		}
	}
	if !keep {
		metrics.EventsFiltered.WithLabelValues(s.inner.Name(), event.Schema, event.Table).Inc()
		s.logger.Debug("event filtered out",
			slog.String("table", event.FullTableName()),
			slog.String("event_id", event.ID),
		)
		return nil
	}

	return s.inner.Process(ctx, event)
}

func (s *Sink) tableChain(event domain.CDCEvent) *Chain {
	if chain, ok := s.tables[event.Schema+"."+event.Table]; ok {
		return chain
	}
	return s.tables[event.Table] // nil is a valid no-op Chain
}
