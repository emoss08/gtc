package meilisearch

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/meilisearch/meilisearch-go"
)

type Sink struct {
	client meilisearch.ServiceManager
	mapper *TableMapper
	config Config
	logger *slog.Logger
}

var _ domain.Sink = (*Sink)(nil)

type SinkParams struct {
	Config Config
	Mapper *TableMapper
	Logger *slog.Logger
}

func NewSink(p SinkParams) *Sink {
	return &Sink{
		client: meilisearch.New(p.Config.URL, meilisearch.WithAPIKey(p.Config.APIKey)),
		mapper: p.Mapper,
		config: p.Config,
		logger: p.Logger.With(slog.String("component", "meilisearch_sink")),
	}
}

func (s *Sink) Name() string {
	return "meilisearch"
}

func (s *Sink) Initialize(ctx context.Context) error {
	s.logger.Debug("initializing meilisearch sink")

	if _, err := s.client.Health(); err != nil {
		s.logger.Error("failed to connect to meilisearch", slog.String("error", err.Error()))
		return err
	}

	s.logger.Info("meilisearch sink initialized")
	return nil
}

func (s *Sink) Process(ctx context.Context, event domain.CDCEvent) error {
	indexName, shouldProcess := s.mapper.GetIndex(event.Schema, event.Table)
	if !shouldProcess {
		s.logger.Debug("skipping event, table not mapped",
			slog.String("table", event.FullTableName()),
			slog.String("event_id", event.ID),
		)
		return nil
	}

	index := s.client.Index(indexName)

	switch event.Operation {
	case domain.OperationInsert, domain.OperationUpdate:
		if event.NewData == nil {
			s.logger.Debug("skipping event, no new data",
				slog.String("event_id", event.ID),
			)
			return nil
		}

		if _, err := index.AddDocuments([]map[string]any{event.NewData}, nil); err != nil {
			s.logger.Error("failed to add document",
				slog.String("error", err.Error()),
				slog.String("index", indexName),
				slog.String("event_id", event.ID),
			)
			return fmt.Errorf("add document: %w", err)
		}

		s.logger.Debug("document added/updated",
			slog.String("index", indexName),
			slog.String("operation", event.Operation.String()),
			slog.String("event_id", event.ID),
		)

	case domain.OperationDelete:
		if event.OldData == nil {
			s.logger.Debug("skipping delete, no old data",
				slog.String("event_id", event.ID),
			)
			return nil
		}

		id, ok := event.OldData["id"]
		if !ok {
			s.logger.Debug("skipping delete, no id field",
				slog.String("event_id", event.ID),
			)
			return nil
		}

		if _, err := index.DeleteDocument(fmt.Sprintf("%v", id)); err != nil {
			s.logger.Error("failed to delete document",
				slog.String("error", err.Error()),
				slog.String("index", indexName),
				slog.Any("document_id", id),
			)
			return fmt.Errorf("delete document: %w", err)
		}

		s.logger.Debug("document deleted",
			slog.String("index", indexName),
			slog.Any("document_id", id),
		)

	case domain.OperationTruncate:
		if _, err := index.DeleteAllDocuments(); err != nil {
			s.logger.Error("failed to delete all documents",
				slog.String("error", err.Error()),
				slog.String("index", indexName),
			)
			return fmt.Errorf("delete all documents: %w", err)
		}

		s.logger.Info("all documents deleted (truncate)",
			slog.String("index", indexName),
		)
	}

	return nil
}

func (s *Sink) Shutdown(_ context.Context) error {
	s.logger.Info("shutting down meilisearch sink")
	s.client.Close()
	s.logger.Info("meilisearch sink shutdown complete")
	return nil
}

func (s *Sink) HealthCheck(ctx context.Context) error {
	_, err := s.client.Health()
	return err
}
