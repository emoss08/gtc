package meilisearch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/meilisearch/meilisearch-go"
)

// taskPollInterval is how often the sink polls Meilisearch for task
// completion; the overall wait is bounded by the caller's context. One poll
// covers every task of a batch.
const taskPollInterval = 20 * time.Millisecond

type Sink struct {
	client meilisearch.ServiceManager
	mapper *TableMapper
	config Config
	logger *slog.Logger

	mu      sync.Mutex
	pending *inflight
}

var _ domain.Sink = (*Sink)(nil)

type SinkParams struct {
	Config Config
	Mapper *TableMapper
	Logger *slog.Logger
}

func NewSink(p SinkParams) *Sink {
	return &Sink{
		client: meilisearch.New(p.Config.URL,
			meilisearch.WithAPIKey(p.Config.APIKey),
			meilisearch.WithCustomJsonMarshaler(sonic.Marshal),
			meilisearch.WithCustomJsonUnmarshaler(sonic.Unmarshal),
		),
		mapper: p.Mapper,
		config: p.Config,
		logger: p.Logger.With(slog.String("component", "meilisearch_sink")),
	}
}

func (s *Sink) Name() string {
	return "meilisearch"
}

func (s *Sink) Wants(schema, table string, _ domain.Operation) bool {
	_, ok := s.mapper.GetIndex(schema, table)
	return ok
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

// Process applies a batch of events with as few Meilisearch tasks as the
// batch allows: per index, consecutive events of the same kind (replace,
// partial update, delete) collapse into one task, and every task is enqueued
// before any is awaited. Meilisearch processes an index's tasks in enqueue
// order and auto-batches consecutive compatible ones, so per-key ordering
// holds while the round trip is paid once per batch rather than per row.
func (s *Sink) Process(ctx context.Context, events []domain.CDCEvent) error {
	if len(events) == 0 {
		return nil
	}
	key := batchKey(events)

	tasks := s.resumable(key)
	if tasks == nil {
		plan, err := s.plan(events)
		if err != nil {
			return err
		}
		if len(plan.order) == 0 {
			return nil
		}

		tasks = make([]enqueuedTask, 0, len(plan.order))
		for _, indexName := range plan.order {
			index := s.client.Index(indexName)
			for _, run := range plan.runs[indexName] {
				info, err := s.enqueue(ctx, index, run)
				if err != nil {
					return fmt.Errorf("meilisearch %s on %q: %w", run.kind, indexName, classifyAPIError(err))
				}
				tasks = append(tasks, enqueuedTask{uid: info.TaskUID, index: indexName, run: run})
			}
		}
		s.remember(key, tasks)
	}

	err := s.awaitTasks(ctx, tasks)
	if err == nil || errors.Is(err, errTaskFailed) || errors.Is(err, errTasksLost) {
		s.forget(key)
	}
	return err
}

var (
	// errTaskFailed marks an error reporting a finished, failed task, as
	// opposed to an interrupted wait.
	errTaskFailed = errors.New("meilisearch task failed")
	// errTasksLost means Meilisearch no longer knows a task of the batch
	// (restarted without its data, or pruned its queue); the batch must be
	// enqueued again rather than waited on forever.
	errTasksLost = errors.New("meilisearch tasks lost")
)

// inflight remembers the tasks of a batch whose wait was interrupted (attempt
// timeout, stream replay). Meilisearch still applies them, so when the same
// batch comes back the sink resumes waiting instead of enqueuing it again:
// a slow index then finishes the work once rather than falling further
// behind with every retry.
type inflight struct {
	key   string
	tasks []enqueuedTask
}

func batchKey(events []domain.CDCEvent) string {
	return fmt.Sprintf("%d\x1f%s\x1f%s", len(events), events[0].ID, events[len(events)-1].ID)
}

func (s *Sink) resumable(key string) []enqueuedTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending != nil && s.pending.key == key {
		return s.pending.tasks
	}
	return nil
}

func (s *Sink) remember(key string, tasks []enqueuedTask) {
	s.mu.Lock()
	s.pending = &inflight{key: key, tasks: tasks}
	s.mu.Unlock()
}

func (s *Sink) forget(key string) {
	s.mu.Lock()
	if s.pending != nil && s.pending.key == key {
		s.pending = nil
	}
	s.mu.Unlock()
}

type runKind int

const (
	runReplace runKind = iota // full document replacement (insert, read)
	runPartial                // partial update (update; omitted TOAST columns keep their values)
	runDelete                 // delete by primary key
	runClear                  // delete every document (truncate)
)

func (k runKind) String() string {
	switch k {
	case runReplace:
		return "add documents"
	case runPartial:
		return "update documents"
	case runDelete:
		return "delete documents"
	default:
		return "delete all documents"
	}
}

// run is one Meilisearch task's worth of consecutive same-kind events for an
// index. Documents are deduplicated by primary key so a row changed several
// times in one batch is sent once, in its final state.
type run struct {
	kind       runKind
	primaryKey string
	docs       []map[string]any
	ids        []string
	pos        map[string]int // primary key value -> index into docs or ids
	eventIDs   []string
}

type batchPlan struct {
	order []string // indexes in first-seen order
	runs  map[string][]*run
}

type enqueuedTask struct {
	uid   int64
	index string
	run   *run
}

func (s *Sink) plan(events []domain.CDCEvent) (*batchPlan, error) {
	p := &batchPlan{runs: make(map[string][]*run)}

	for i := range events {
		event := &events[i]
		indexName, ok := s.mapper.GetIndex(event.Schema, event.Table)
		if !ok {
			continue
		}
		primaryKey := s.mapper.PrimaryKey(event.Schema, event.Table)

		switch event.Operation {
		case domain.OperationInsert, domain.OperationRead, domain.OperationUpdate:
			if event.NewData == nil {
				continue
			}
			id, ok := event.NewData[primaryKey]
			if !ok || id == nil {
				return nil, domain.Permanent(fmt.Errorf(
					"event %s for %s has no value for meilisearch primary key %q",
					event.ID, event.FullTableName(), primaryKey))
			}
			kind := runReplace
			if event.Operation == domain.OperationUpdate {
				kind = runPartial
			}
			r := p.runFor(indexName, kind, primaryKey)
			r.addDoc(fmt.Sprint(id), event.NewData)
			r.eventIDs = append(r.eventIDs, event.ID)

		case domain.OperationDelete:
			if event.OldData == nil {
				continue
			}
			id, ok := event.OldData[primaryKey]
			if !ok || id == nil {
				// The replica identity does not carry the key: nothing
				// can be addressed. Same outcome as before batching.
				s.logger.Debug("skipping delete, no primary key field",
					slog.String("primary_key", primaryKey),
					slog.String("event_id", event.ID))
				continue
			}
			r := p.runFor(indexName, runDelete, primaryKey)
			r.addID(fmt.Sprint(id))
			r.eventIDs = append(r.eventIDs, event.ID)

		case domain.OperationTruncate:
			r := p.runFor(indexName, runClear, primaryKey)
			r.eventIDs = append(r.eventIDs, event.ID)
		}
	}
	return p, nil
}

// runFor returns the index's trailing run when it has the same kind, or
// starts a new one: runs never reorder events across kinds.
func (p *batchPlan) runFor(indexName string, kind runKind, primaryKey string) *run {
	runs, seen := p.runs[indexName]
	if !seen {
		p.order = append(p.order, indexName)
	}
	if n := len(runs); n > 0 && runs[n-1].kind == kind && kind != runClear {
		return runs[n-1]
	}
	r := &run{kind: kind, primaryKey: primaryKey, pos: make(map[string]int)}
	p.runs[indexName] = append(runs, r)
	return r
}

func (r *run) addDoc(id string, data map[string]any) {
	if at, dup := r.pos[id]; dup {
		if r.kind == runPartial {
			// Two partial updates of one document compose: later
			// fields win, fields only the earlier one carried survive.
			merged := make(map[string]any, len(r.docs[at])+len(data))
			for k, v := range r.docs[at] {
				merged[k] = v
			}
			for k, v := range data {
				merged[k] = v
			}
			r.docs[at] = merged
			return
		}
		r.docs[at] = data
		return
	}
	r.pos[id] = len(r.docs)
	r.docs = append(r.docs, data)
}

func (r *run) addID(id string) {
	if _, dup := r.pos[id]; dup {
		return
	}
	r.pos[id] = len(r.ids)
	r.ids = append(r.ids, id)
}

func (s *Sink) enqueue(ctx context.Context, index meilisearch.IndexManager, r *run) (*meilisearch.TaskInfo, error) {
	opts := &meilisearch.DocumentOptions{PrimaryKey: &r.primaryKey}
	switch r.kind {
	case runReplace:
		return index.AddDocumentsWithContext(ctx, r.docs, opts)
	case runPartial:
		return index.UpdateDocumentsWithContext(ctx, r.docs, opts)
	case runDelete:
		return index.DeleteDocumentsWithContext(ctx, r.ids, nil)
	default:
		return index.DeleteAllDocumentsWithContext(ctx, nil)
	}
}

// awaitTasks polls every task of the batch with one request per tick until
// all have finished, then reports the first failure.
func (s *Sink) awaitTasks(ctx context.Context, tasks []enqueuedTask) error {
	uids := make([]int64, len(tasks))
	byUID := make(map[int64]enqueuedTask, len(tasks))
	for i, t := range tasks {
		uids[i] = t.uid
		byUID[t.uid] = t
	}

	ticker := time.NewTicker(taskPollInterval)
	defer ticker.Stop()

	for {
		result, err := s.client.GetTasksWithContext(ctx, &meilisearch.TasksQuery{
			UIDS:  uids,
			Limit: int64(len(uids)),
		})
		if err != nil {
			return fmt.Errorf("poll meilisearch tasks: %w", err)
		}

		if len(result.Results) < len(uids) {
			return fmt.Errorf("%w: %d of %d tasks not found", errTasksLost,
				len(uids)-len(result.Results), len(uids))
		}

		finished := 0
		var failed *meilisearch.Task
		for i := range result.Results {
			task := &result.Results[i]
			switch task.Status {
			case meilisearch.TaskStatusSucceeded:
				finished++
			case meilisearch.TaskStatusFailed, meilisearch.TaskStatusCanceled:
				finished++
				if failed == nil || task.UID < failed.UID {
					failed = task
				}
			}
		}

		if finished == len(uids) {
			if failed == nil {
				return nil
			}
			t := byUID[failed.UID]
			s.logger.Error("meilisearch task failed",
				slog.String("index", t.index),
				slog.String("operation", t.run.kind.String()),
				slog.Int("events", len(t.run.eventIDs)),
				slog.String("status", string(failed.Status)),
				slog.String("task_error", failed.Error.Message),
			)
			err := fmt.Errorf("%w: task %d (%s on %q, %d events) %s: %s (%s)",
				errTaskFailed, failed.UID, t.run.kind, t.index, len(t.run.eventIDs),
				failed.Status, failed.Error.Message, failed.Error.Code)
			// invalid_request failures are about the documents (bad id,
			// bad field shape): retrying the same payload cannot pass.
			if failed.Error.Type == "invalid_request" {
				return domain.Permanent(err)
			}
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for meilisearch tasks: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// classifyAPIError marks request rejections caused by the payload as
// permanent; transport failures, throttling and server errors stay
// retryable.
func classifyAPIError(err error) error {
	var apiErr *meilisearch.Error
	if errors.As(err, &apiErr) && apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 &&
		apiErr.StatusCode != http.StatusRequestTimeout &&
		apiErr.StatusCode != http.StatusTooManyRequests &&
		apiErr.StatusCode != http.StatusUnauthorized &&
		apiErr.StatusCode != http.StatusForbidden {
		return domain.Permanent(err)
	}
	return err
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
