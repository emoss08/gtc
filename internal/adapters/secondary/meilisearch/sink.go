package meilisearch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
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

	if err := s.applyIndexSettings(ctx); err != nil {
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
	primaryKey := s.mapper.PrimaryKey(event.Schema, event.Table)
	docOpts := &meilisearch.DocumentOptions{PrimaryKey: &primaryKey}

	switch event.Operation {
	case domain.OperationInsert, domain.OperationUpdate, domain.OperationRead:
		if event.NewData == nil {
			s.logger.Debug("skipping event, no new data",
				slog.String("event_id", event.ID),
			)
			return nil
		}

		docs := []map[string]any{event.NewData}
		var task *meilisearch.TaskInfo
		var err error
		if event.Operation == domain.OperationUpdate {
			// Partial update: fields omitted from NewData (e.g. unchanged
			// TOAST columns) keep their previously indexed values instead
			// of being wiped by a full document replacement.
			task, err = index.UpdateDocumentsWithContext(ctx, docs, docOpts)
		} else {
			task, err = index.AddDocumentsWithContext(ctx, docs, docOpts)
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

		id, ok := event.OldData[primaryKey]
		if !ok {
			s.logger.Debug("skipping delete, no primary key field",
				slog.String("primary_key", primaryKey),
				slog.String("event_id", event.ID),
			)
			return nil
		}

		task, err := index.DeleteDocumentWithContext(ctx, fmt.Sprintf("%v", id), nil)
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
		task, err := index.DeleteAllDocumentsWithContext(ctx, nil)
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

// applyIndexSettings creates each configured index with its primary key and
// applies its searchable and filterable attributes. Settings that already
// match are left alone, so a restart does not trigger a reindex.
func (s *Sink) applyIndexSettings(ctx context.Context) error {
	settings, err := s.mapper.IndexSettings()
	if err != nil {
		return err
	}

	for indexName, want := range settings {
		if err := s.ensureIndex(ctx, indexName, want.PrimaryKey); err != nil {
			return err
		}

		index := s.client.Index(indexName)

		if len(want.SearchableAttributes) > 0 {
			current, getErr := index.GetSearchableAttributesWithContext(ctx)
			if getErr != nil {
				return fmt.Errorf("get searchable attributes of %q: %w", indexName, getErr)
			}
			if current == nil || !slices.Equal(*current, want.SearchableAttributes) {
				attrs := append([]string(nil), want.SearchableAttributes...)
				task, updateErr := index.UpdateSearchableAttributesWithContext(ctx, &attrs)
				if updateErr != nil {
					return fmt.Errorf("update searchable attributes of %q: %w", indexName, updateErr)
				}
				if awaitErr := s.awaitTask(ctx, task, indexName, ""); awaitErr != nil {
					return awaitErr
				}
			}
		}

		if len(want.FilterableAttributes) > 0 {
			current, getErr := index.GetFilterableAttributesWithContext(ctx)
			if getErr != nil {
				return fmt.Errorf("get filterable attributes of %q: %w", indexName, getErr)
			}
			if !sameFilterable(current, want.FilterableAttributes) {
				attrs := make([]any, 0, len(want.FilterableAttributes))
				for _, attr := range want.FilterableAttributes {
					attrs = append(attrs, attr)
				}
				task, updateErr := index.UpdateFilterableAttributesWithContext(ctx, &attrs)
				if updateErr != nil {
					return fmt.Errorf("update filterable attributes of %q: %w", indexName, updateErr)
				}
				if awaitErr := s.awaitTask(ctx, task, indexName, ""); awaitErr != nil {
					return awaitErr
				}
			}
		}

		s.logger.Info("meilisearch index ready",
			slog.String("index", indexName),
			slog.String("primary_key", want.PrimaryKey),
		)
	}

	return nil
}

// ensureIndex creates the index with the primary key when it does not exist,
// and refuses to start against an index keyed by a different field: every
// write would fail.
func (s *Sink) ensureIndex(ctx context.Context, indexName, primaryKey string) error {
	existing, err := s.client.GetIndexWithContext(ctx, indexName)
	if err == nil {
		if existing.PrimaryKey != "" && existing.PrimaryKey != primaryKey {
			return fmt.Errorf(
				"meilisearch index %q has primary key %q, config wants %q",
				indexName, existing.PrimaryKey, primaryKey,
			)
		}
		return nil
	}

	var apiErr *meilisearch.Error
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return fmt.Errorf("get meilisearch index %q: %w", indexName, err)
	}

	task, err := s.client.CreateIndexWithContext(ctx, &meilisearch.IndexConfig{
		Uid:        indexName,
		PrimaryKey: primaryKey,
	})
	if err != nil {
		return fmt.Errorf("create meilisearch index %q: %w", indexName, err)
	}
	return s.awaitTask(ctx, task, indexName, "")
}

// sameFilterable reports whether the index's filterable attributes are
// exactly the wanted plain attribute names.
func sameFilterable(current *[]any, want []string) bool {
	if current == nil || len(*current) != len(want) {
		return false
	}
	for i, attr := range *current {
		name, ok := attr.(string)
		if !ok || name != want[i] {
			return false
		}
	}
	return true
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
