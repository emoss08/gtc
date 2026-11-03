package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
	"github.com/emoss08/gtc/internal/infrastructure/metrics"
)

type CDCServiceConfig struct {
	ParallelSinks  bool
	ExcludedTables map[string]struct{}
}

type cdcService struct {
	config    CDCServiceConfig
	walReader ports.WALReader
	registry  ports.SinkRegistry
	logger    *slog.Logger
	wg        sync.WaitGroup
	shutdown  chan struct{}
	stopOnce  sync.Once
}

type CDCServiceParams struct {
	WALReader ports.WALReader
	Registry  ports.SinkRegistry
	Logger    *slog.Logger
	Config    CDCServiceConfig
}

func NewCDCService(params CDCServiceParams) ports.CDCService {
	return &cdcService{
		config:    params.Config,
		walReader: params.WALReader,
		registry:  params.Registry,
		logger:    params.Logger.With(slog.String("component", "cdc_service")),
		shutdown:  make(chan struct{}),
	}
}

func (s *cdcService) Start(ctx context.Context) error {
	sinks := s.registry.All()
	sinkNames := make([]string, len(sinks))
	for i, sink := range sinks {
		sinkNames[i] = sink.Name()
	}

	excludedTables := make([]string, 0, len(s.config.ExcludedTables))
	for table := range s.config.ExcludedTables {
		excludedTables = append(excludedTables, table)
	}

	s.logger.Info("starting CDC service",
		slog.Bool("parallel_sinks", s.config.ParallelSinks),
		slog.Int("sink_count", len(sinks)),
		slog.Any("sinks", sinkNames),
		slog.Int("excluded_table_count", len(excludedTables)),
		slog.Any("excluded_tables", excludedTables),
	)

	s.logger.Debug("initializing sinks")
	if err := s.registry.InitializeAll(ctx); err != nil {
		s.logger.Error("failed to initialize sinks", slog.String("error", err.Error()))
		return err
	}
	s.logger.Info("all sinks initialized", slog.Int("count", len(sinks)))
	metrics.ActiveSinks.Set(float64(len(sinks)))

	return s.walReader.Start(ctx, s.handleBatch)
}

func (s *cdcService) Stop(ctx context.Context) error {
	s.logger.Info("stopping CDC service")
	s.stopOnce.Do(func() { close(s.shutdown) })

	s.logger.Debug("waiting for in-flight events to complete")
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Debug("all in-flight events completed")
	case <-ctx.Done():
		s.logger.Warn("shutdown timeout exceeded, some events may not have completed")
	}

	if err := s.walReader.Stop(ctx); err != nil {
		s.logger.Error("failed to stop WAL reader", slog.String("error", err.Error()))
	}

	s.logger.Debug("shutting down sinks")
	if err := s.registry.ShutdownAll(ctx); err != nil {
		s.logger.Error("failed to shutdown sinks", slog.String("error", err.Error()))
		return err
	}

	s.logger.Info("CDC service stopped")
	return nil
}

// handleBatch fans a batch out to all sinks. It returns an error when any
// sink fails so the WAL reader does not confirm the batch's positions and the
// events are redelivered after reconnect — sinks must tolerate duplicates
// (at-least-once delivery).
func (s *cdcService) handleBatch(ctx context.Context, events []domain.CDCEvent) error {
	s.wg.Add(1)
	defer s.wg.Done()

	select {
	case <-s.shutdown:
		return domain.ErrShuttingDown
	default:
	}

	events = s.admit(events)
	if len(events) == 0 {
		return nil
	}

	metrics.BatchSize.Observe(float64(len(events)))
	metrics.InFlightEvents.Add(float64(len(events)))
	defer metrics.InFlightEvents.Sub(float64(len(events)))

	results := s.ProcessBatch(ctx, events)

	var errs []error
	for _, r := range results {
		if r.Success {
			continue
		}
		s.logger.Error("sink processing failed",
			slog.String("sink", r.SinkName),
			slog.String("error", r.Error.Error()),
			slog.Int("events", len(events)),
			slog.String("first_event_id", events[0].ID),
			slog.String("last_lsn", events[len(events)-1].Metadata.LSN),
			slog.Duration("duration", r.Duration),
		)
		errs = append(errs, fmt.Errorf("sink %s: %w", r.SinkName, r.Error))
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", domain.ErrSinkProcessFailed, errors.Join(errs...))
	}
	return nil
}

type tableOp struct {
	schema, table, op string
}

// admit counts the batch per table and operation and drops events of
// excluded tables, copying only when something is dropped.
func (s *cdcService) admit(events []domain.CDCEvent) []domain.CDCEvent {
	received := make(map[tableOp]int, 4)
	excluded := make(map[tableOp]int)
	kept := events
	copied := false

	for i := range events {
		e := &events[i]
		key := tableOp{e.Schema, e.Table, e.Operation.String()}
		received[key]++

		if s.isTableExcluded(e.Schema, e.Table) {
			excluded[tableOp{schema: e.Schema, table: e.Table}]++
			if !copied {
				kept = append(make([]domain.CDCEvent, 0, len(events)), events[:i]...)
				copied = true
			}
			continue
		}
		if copied {
			kept = append(kept, *e)
		}
	}

	for k, n := range received {
		metrics.EventsReceived.WithLabelValues(k.schema, k.table, k.op).Add(float64(n))
	}
	for k, n := range excluded {
		metrics.EventsExcluded.WithLabelValues(k.schema, k.table).Add(float64(n))
	}
	return kept
}

func (s *cdcService) ProcessBatch(ctx context.Context, events []domain.CDCEvent) []domain.SinkResult {
	sinks := s.registry.All()
	results := make([]domain.SinkResult, len(sinks))

	if len(sinks) == 0 {
		s.logger.Warn("no sinks registered, events will not be persisted",
			slog.Int("events", len(events)),
		)
		return results
	}

	if s.config.ParallelSinks && len(sinks) > 1 {
		var wg sync.WaitGroup
		for i, sink := range sinks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = s.processSink(ctx, sink, events)
			}()
		}
		wg.Wait()
	} else {
		for i, sink := range sinks {
			results[i] = s.processSink(ctx, sink, events)
		}
	}

	return results
}

// processSink delivers the batch to one sink. Attempts are bounded by the
// resilience layer's per-attempt timeout; the stream's keepalives continue
// while sinks work, so a long delivery cannot time the replication
// connection out.
func (s *cdcService) processSink(
	ctx context.Context,
	sink domain.Sink,
	events []domain.CDCEvent,
) domain.SinkResult {
	start := time.Now()
	err := deliver(ctx, sink, events)
	duration := time.Since(start)

	metrics.SinkBatchDuration.WithLabelValues(sink.Name()).Observe(duration.Seconds())
	if err != nil {
		metrics.EventsProcessed.WithLabelValues(sink.Name(), "failure").Add(float64(len(events)))
		metrics.SinkErrors.WithLabelValues(sink.Name(), classifyError(err)).Inc()
	} else {
		metrics.EventsProcessed.WithLabelValues(sink.Name(), "success").Add(float64(len(events)))
	}

	return domain.SinkResult{
		SinkName: sink.Name(),
		Success:  err == nil,
		Error:    err,
		Duration: duration,
	}
}

// deliver hands the batch over in pieces no larger than the sink's own
// delivery unit, in order, stopping at the first failure.
func deliver(ctx context.Context, sink domain.Sink, events []domain.CDCEvent) error {
	limit := domain.MaxBatchEvents(sink)
	if limit <= 0 || len(events) <= limit {
		return sink.Process(ctx, events)
	}
	for start := 0; start < len(events); start += limit {
		if err := sink.Process(ctx, events[start:min(start+limit, len(events))]); err != nil {
			return err
		}
	}
	return nil
}

func classifyError(err error) string {
	switch {
	case errors.Is(err, domain.ErrCircuitOpen):
		return "circuit_open"
	case domain.IsPermanent(err):
		return "permanent"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "other"
	}
}

func (s *cdcService) isTableExcluded(schema, table string) bool {
	if len(s.config.ExcludedTables) == 0 {
		return false
	}

	if _, ok := s.config.ExcludedTables[table]; ok {
		return true
	}
	_, ok := s.config.ExcludedTables[schema+"."+table]
	return ok
}
