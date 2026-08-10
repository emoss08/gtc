package meilisearch

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/meilisearch/meilisearch-go"
)

// taskPollInterval is how often the sink polls Meilisearch for task
// completion; the overall wait is bounded by the caller's context.
const taskPollInterval = 50 * time.Millisecond

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

	if _, err := s.client.HealthWithContext(ctx); err != nil {
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

		docs := []map[string]any{event.NewData}
		var task *meilisearch.TaskInfo
		var err error
		if event.Operation == domain.OperationInsert {
			task, err = index.AddDocumentsWithContext(ctx, docs, nil)
		} else {
			// Partial update: fields omitted from NewData (e.g. unchanged
			// TOAST columns) keep their previously indexed values instead
			// of being wiped by a full document replacement.
			task, err = index.UpdateDocumentsWithContext(ctx, docs, nil)
		}
		if err != nil {
			s.logger.Error("failed to write document",
				slog.String("error", err.Error()),
				slog.String("index", indexName),
				slog.String("event_id", event.ID),
			)
			return fmt.Errorf("write document: %w", err)
		}

		if err := s.awaitTask(ctx, task, indexName, event.ID); err != nil {
			return err
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

		task, err := index.DeleteDocumentWithContext(ctx, fmt.Sprintf("%v", id))
		if err != nil {
			s.logger.Error("failed to delete document",
				slog.String("error", err.Error()),
				slog.String("index", indexName),
				slog.Any("document_id", id),
			)
			return fmt.Errorf("delete document: %w", err)
		}

		if err := s.awaitTask(ctx, task, indexName, event.ID); err != nil {
			return err
		}

		s.logger.Debug("document deleted",
			slog.String("index", indexName),
			slog.Any("document_id", id),
		)

	case domain.OperationTruncate:
		task, err := index.DeleteAllDocumentsWithContext(ctx)
		if err != nil {
			s.logger.Error("failed to delete all documents",
				slog.String("error", err.Error()),
				slog.String("index", indexName),
			)
			return fmt.Errorf("delete all documents: %w", err)
		}

		if err := s.awaitTask(ctx, task, indexName, event.ID); err != nil {
			return err
		}

		s.logger.Info("all documents deleted (truncate)",
			slog.String("index", indexName),
		)
	}

	return nil
}

// awaitTask blocks until the asynchronous Meilisearch task finishes and
// surfaces indexing failures (bad primary key, schema errors) that a plain
// enqueue would silently swallow.
func (s *Sink) awaitTask(
	ctx context.Context,
	taskInfo *meilisearch.TaskInfo,
	indexName, eventID string,
) error {
	task, err := s.client.WaitForTaskWithContext(ctx, taskInfo.TaskUID, taskPollInterval)
	if err != nil {
		return fmt.Errorf("wait for meilisearch task: %w", err)
	}

	if task.Status != meilisearch.TaskStatusSucceeded {
		s.logger.Error("meilisearch task failed",
			slog.String("index", indexName),
			slog.String("event_id", eventID),
			slog.String("status", string(task.Status)),
			slog.String("task_error", task.Error.Message),
		)
		return fmt.Errorf("meilisearch task %d failed: %s (%s)",
			task.UID, task.Error.Message, task.Error.Code)
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
	_, err := s.client.HealthWithContext(ctx)
	return err
}
