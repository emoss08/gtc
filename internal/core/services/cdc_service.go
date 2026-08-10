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
	"github.com/sourcegraph/conc/pool"
)

type CDCServiceConfig struct {
	ParallelSinks  bool
	ProcessTimeout time.Duration
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
		slog.Duration("process_timeout", s.config.ProcessTimeout),
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

	return s.walReader.Start(ctx, s.handleEvent)
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

// handleEvent fans an event out to all sinks. It returns an error when any
// sink fails so the WAL reader does not confirm the transaction's LSN and the
// event is redelivered after reconnect — sinks must tolerate duplicates
// (at-least-once delivery).
func (s *cdcService) handleEvent(ctx context.Context, event domain.CDCEvent) error {
	s.wg.Add(1)
	defer s.wg.Done()

	select {
	case <-s.shutdown:
		s.logger.Debug("rejecting event due to shutdown",
			slog.String("event_id", event.ID),
		)
		return domain.ErrShuttingDown
	default:
	}

	metrics.EventsReceived.WithLabelValues(event.Schema, event.Table, event.Operation.String()).Inc()

	if s.isTableExcluded(event.Schema, event.Table) {
		metrics.EventsExcluded.WithLabelValues(event.Schema, event.Table).Inc()
		s.logger.Debug("skipping excluded table",
			slog.String("table", event.FullTableName()),
			slog.String("event_id", event.ID),
		)
		return nil
	}

	metrics.InFlightEvents.Inc()
	defer metrics.InFlightEvents.Dec()

	s.logger.Debug("processing event",
		slog.String("operation", event.Operation.String()),
		slog.String("table", event.FullTableName()),
		slog.String("lsn", event.Metadata.LSN),
		slog.Int("xid", int(event.Metadata.TransactionID)),
	)

	results := s.ProcessEvent(ctx, event)

	var errs []error
	for _, r := range results {
		if r.Success {
			s.logger.Debug("sink processed event",
				slog.String("sink", r.SinkName),
				slog.String("event_id", event.ID),
				slog.Duration("duration", r.Duration),
			)
		} else {
			s.logger.Error("sink processing failed",
				slog.String("sink", r.SinkName),
				slog.String("error", r.Error.Error()),
				slog.String("event_id", event.ID),
				slog.Duration("duration", r.Duration),
			)
			errs = append(errs, fmt.Errorf("sink %s: %w", r.SinkName, r.Error))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", domain.ErrSinkProcessFailed, errors.Join(errs...))
	}
	return nil
}

func (s *cdcService) ProcessEvent(ctx context.Context, event domain.CDCEvent) []domain.SinkResult {
	sinks := s.registry.All()
	results := make([]domain.SinkResult, len(sinks))

	if len(sinks) == 0 {
		s.logger.Warn("no sinks registered, event will not be persisted",
			slog.String("event_id", event.ID),
		)
		return results
	}

	if s.config.ParallelSinks {
		p := pool.New().WithContext(ctx)
		for i, sink := range sinks {
			idx, snk := i, sink
			p.Go(func(ctx context.Context) error {
				results[idx] = s.processSingleSink(ctx, snk, event)
				return nil
			})
		}
		_ = p.Wait()
	} else {
		for i, sink := range sinks {
			results[i] = s.processSingleSink(ctx, sink, event)
		}
	}

	return results
}

func (s *cdcService) processSingleSink(
	ctx context.Context,
	sink domain.Sink,
	event domain.CDCEvent,
) domain.SinkResult {
	start := time.Now()

	ctx, cancel := context.WithTimeout(ctx, s.config.ProcessTimeout)
	defer cancel()

	err := sink.Process(ctx, event)
	duration := time.Since(start)

	metrics.EventProcessingDuration.WithLabelValues(sink.Name()).Observe(duration.Seconds())
	if err != nil {
		metrics.EventsProcessed.WithLabelValues(sink.Name(), "failure").Inc()
		metrics.SinkErrors.WithLabelValues(sink.Name(), classifyError(err)).Inc()
	} else {
		metrics.EventsProcessed.WithLabelValues(sink.Name(), "success").Inc()
	}

	return domain.SinkResult{
		SinkName: sink.Name(),
		Success:  err == nil,
		Error:    err,
		Duration: duration,
	}
}

func classifyError(err error) string {
	switch {
	case errors.Is(err, domain.ErrCircuitOpen):
		return "circuit_open"
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

	fullName := schema + "." + table
	if _, ok := s.config.ExcludedTables[fullName]; ok {
		return true
	}

	if _, ok := s.config.ExcludedTables[table]; ok {
		return true
	}

	return false
}
