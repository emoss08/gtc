// Package clickhouse mirrors change events into ClickHouse for analytics.
//
// Each source table becomes a ReplacingMergeTree keyed by the source primary
// key and versioned by LSN, so replays are idempotent, a live change always
// beats a backfilled one for the same row, and deletes become tombstones
// rather than disappearing silently. Target tables are created — and extended
// when a column is added upstream — from the source catalog, so the sink
// stays plug-and-play.
package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pglogrepl"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/emoss08/gtc/internal/core/ports"
)

// TableResolver decides which tables are mirrored and what each is called in
// ClickHouse. It is satisfied by the shared sink config without coupling this
// package to Redis.
type TableResolver interface {
	ShouldProcess(schema, table string) bool
	// GetKeyPattern returns a configured target table name, or "" to derive
	// one from the source table.
	GetKeyPattern(schema, table string) string
}

type Sink struct {
	conn         driver.Conn
	config       Config
	resolver     TableResolver
	introspector *Introspector
	logger       *slog.Logger

	mu sync.RWMutex
	// tables caches the mirrors already created, keyed by "schema.table".
	tables map[string]*tableSchema
	// skipped remembers tables that cannot be mirrored (no primary key), so
	// the failure is reported once instead of on every event.
	skipped map[string]struct{}
}

var (
	_ domain.Sink          = (*Sink)(nil)
	_ ports.SchemaObserver = (*Sink)(nil)
)

type SinkParams struct {
	Config       Config
	Resolver     TableResolver
	Introspector *Introspector
	Logger       *slog.Logger
}

func NewSink(p SinkParams) *Sink {
	return &Sink{
		config:       p.Config,
		resolver:     p.Resolver,
		introspector: p.Introspector,
		logger:       p.Logger.With(slog.String("component", "clickhouse")),
		tables:       make(map[string]*tableSchema),
		skipped:      make(map[string]struct{}),
	}
}

func (s *Sink) Name() string { return "clickhouse" }

func (s *Sink) Initialize(ctx context.Context) error {
	opts, err := clickhouse.ParseDSN(s.config.URL)
	if err != nil {
		return fmt.Errorf("invalid CLICKHOUSE_URL: %w", err)
	}
	opts.Settings = clickhouse.Settings{}
	if s.config.AsyncInsert {
		// Let the server coalesce the many small inserts a change stream
		// produces; without this every event becomes its own part.
		opts.Settings["async_insert"] = 1
		opts.Settings["wait_for_async_insert"] = boolToInt(s.config.WaitForInsert)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("connect to ClickHouse: %w", err)
	}
	s.conn = conn

	pingCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping ClickHouse: %w", err)
	}

	if s.config.AutoCreateTables {
		if err := conn.Exec(pingCtx, fmt.Sprintf(
			"CREATE DATABASE IF NOT EXISTS %s", quoteIdent(s.config.Database))); err != nil {
			return fmt.Errorf("create database %q: %w", s.config.Database, err)
		}
	}

	if !s.config.WaitForInsert {
		s.logger.Warn("CLICKHOUSE_WAIT_FOR_INSERT is false: the WAL position " +
			"advances past rows that are still buffered in the server, so a " +
			"ClickHouse crash can lose them")
	}
	s.logger.Info("sink initialized",
		slog.String("database", s.config.Database),
		slog.Bool("async_insert", s.config.AsyncInsert),
		slog.Bool("wait_for_insert", s.config.WaitForInsert),
	)
	return nil
}

func (s *Sink) Shutdown(ctx context.Context) error {
	var err error
	if s.conn != nil {
		err = s.conn.Close()
	}
	if s.introspector != nil {
		if closeErr := s.introspector.Close(ctx); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func (s *Sink) HealthCheck(ctx context.Context) error {
	if s.conn == nil {
		return fmt.Errorf("not connected to ClickHouse")
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	return s.conn.Ping(ctx)
}

func (s *Sink) Process(ctx context.Context, event domain.CDCEvent) error {
	if s.resolver != nil && !s.resolver.ShouldProcess(event.Schema, event.Table) {
		s.logger.Debug("skipping event, table not configured",
			slog.String("table", event.FullTableName()),
			slog.String("event_id", event.ID),
		)
		return nil
	}
	// A truncate has no row to mirror, and emptying the target would race
	// with rows still in flight; report it instead of guessing.
	if event.Operation == domain.OperationTruncate {
		s.logger.Warn("TRUNCATE is not mirrored to ClickHouse; the target table "+
			"still holds the truncated rows",
			slog.String("table", event.FullTableName()))
		return nil
	}

	schema, err := s.tableFor(ctx, event.Schema, event.Table)
	if err != nil {
		return err
	}
	if schema == nil {
		return nil // unmirrorable table, already reported
	}

	values, err := s.rowValues(ctx, schema, event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	if s.config.AsyncInsert {
		if err := s.conn.AsyncInsert(ctx, schema.insertDDL(),
			s.config.WaitForInsert, values...); err != nil {
			return fmt.Errorf("insert into %s: %w", schema.qualified(), err)
		}
	} else if err := s.conn.Exec(ctx, schema.insertDDL(), values...); err != nil {
		return fmt.Errorf("insert into %s: %w", schema.qualified(), err)
	}

	s.logger.Debug("row mirrored",
		slog.String("table", schema.qualified()),
		slog.String("event_id", event.ID),
		slog.String("operation", event.Operation.String()),
	)
	return nil
}

// rowValues builds the insert arguments for one event, in the column order of
// the mirror.
func (s *Sink) rowValues(
	ctx context.Context,
	schema *tableSchema,
	event domain.CDCEvent,
) ([]any, error) {
	deleted := event.Operation == domain.OperationDelete
	data := event.NewData
	if deleted {
		// A delete carries only the replica identity; the tombstone keeps
		// the key and zeroes the rest.
		data = event.OldData
	}

	// An update that left a TOASTed column untouched omits it from the WAL.
	// ReplacingMergeTree replaces whole rows, so writing without it would
	// blank a value that did not change: carry the stored one forward.
	carried := map[string]any{}
	if len(event.UnchangedToastColumns) > 0 && !deleted {
		var err error
		carried, err = s.currentValues(ctx, schema, data, event.UnchangedToastColumns)
		if err != nil {
			return nil, err
		}
	}

	values := make([]any, 0, len(schema.columns)+3)
	for _, col := range schema.columns {
		switch value, present := data[col.Name]; {
		case present:
			values = append(values, coerce(col, value))
		case carried[col.Name] != nil:
			values = append(values, carried[col.Name])
		default:
			values = append(values, zeroValue(col))
		}
	}

	values = append(values, lsnVersion(event.Metadata.LSN), boolToUint8(deleted), time.Now().UTC())
	return values, nil
}

// currentValues reads columns from the existing mirrored row so unchanged
// TOAST values survive a row replacement.
func (s *Sink) currentValues(
	ctx context.Context,
	schema *tableSchema,
	data map[string]any,
	columns []string,
) (map[string]any, error) {
	// Keep names and quoted identifiers in step: a column the mirror does
	// not have is skipped, and the result mapping must not shift because
	// of it.
	names := make([]string, 0, len(columns))
	wanted := make([]string, 0, len(columns))
	for _, name := range columns {
		if _, known := schema.byName[name]; known {
			names = append(names, name)
			wanted = append(wanted, quoteIdent(name))
		}
	}
	if len(wanted) == 0 {
		return map[string]any{}, nil
	}

	where := make([]string, 0, len(schema.keys))
	args := make([]any, 0, len(schema.keys))
	for _, key := range schema.keys {
		value, ok := data[key.Name]
		if !ok {
			// Without the full key the stored row cannot be located; the
			// caller falls back to zero values.
			return map[string]any{}, nil
		}
		where = append(where, quoteIdent(key.Name)+" = ?")
		args = append(args, coerce(key, value))
	}

	query := fmt.Sprintf("SELECT %s FROM %s FINAL WHERE %s LIMIT 1",
		strings.Join(wanted, ", "), schema.qualified(), strings.Join(where, " AND "))

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read unchanged TOAST columns from %s: %w",
			schema.qualified(), err)
	}
	defer rows.Close()

	out := map[string]any{}
	if !rows.Next() {
		// No prior row (first sighting of this key): nothing to carry.
		return out, rows.Err()
	}
	scanned := make([]any, len(wanted))
	for i := range scanned {
		var v any
		scanned[i] = &v
	}
	if err := rows.Scan(scanned...); err != nil {
		return nil, fmt.Errorf("scan unchanged TOAST columns: %w", err)
	}
	for i, name := range names {
		if ptr, ok := scanned[i].(*any); ok {
			out[name] = *ptr
		}
	}
	return out, rows.Err()
}

// tableFor returns the mirror for a source table, creating it on first sight.
func (s *Sink) tableFor(ctx context.Context, schema, table string) (*tableSchema, error) {
	key := schema + "." + table

	s.mu.RLock()
	cached, ok := s.tables[key]
	_, skip := s.skipped[key]
	s.mu.RUnlock()
	if ok {
		return cached, nil
	}
	if skip {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if cached, ok := s.tables[key]; ok {
		return cached, nil
	}
	if _, skip := s.skipped[key]; skip {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	cols, err := s.introspector.Columns(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	ts, err := newTableSchema(s.config.Database, s.targetName(schema, table), cols)
	if err != nil {
		// Not a transient failure: the same event would fail forever, and
		// stalling the whole pipeline over one unmirrorable table is worse
		// than mirroring the rest and saying so loudly.
		s.skipped[key] = struct{}{}
		s.logger.Error("table cannot be mirrored to ClickHouse",
			slog.String("table", key), slog.String("reason", err.Error()))
		return nil, nil
	}

	if s.config.AutoCreateTables {
		if err := s.conn.Exec(ctx, ts.createDDL()); err != nil {
			return nil, fmt.Errorf("create %s: %w", ts.qualified(), err)
		}
	}

	s.tables[key] = ts
	s.logger.Info("mirroring table",
		slog.String("source", key),
		slog.String("target", ts.qualified()),
		slog.Int("columns", len(ts.columns)),
	)
	return ts, nil
}

// targetName is the ClickHouse table for a source table: the configured name
// if there is one, otherwise "<prefix><schema>_<table>" (bare "<prefix><table>"
// for the public schema, which is the common case).
func (s *Sink) targetName(schema, table string) string {
	if s.resolver != nil {
		if configured := s.resolver.GetKeyPattern(schema, table); configured != "" {
			return configured
		}
	}
	if schema == "public" {
		return s.config.TablePrefix + table
	}
	return s.config.TablePrefix + schema + "_" + table
}

// OnSchemaChange keeps the mirror in step with upstream DDL. Added columns
// are added to the target; the rest are reported, because dropping or
// retyping a column in ClickHouse would destroy history that the mirror
// exists to preserve.
func (s *Sink) OnSchemaChange(ctx context.Context, change domain.SchemaChange) {
	key := change.Schema + "." + change.Table

	s.mu.RLock()
	ts, mirrored := s.tables[key]
	s.mu.RUnlock()
	if !mirrored {
		// Not mirrored yet: the table is introspected fresh on its first
		// event, which picks the change up anyway.
		return
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	if len(change.AddedColumns) > 0 {
		cols, err := s.introspector.Columns(ctx, change.Schema, change.Table)
		if err != nil {
			s.logger.Error("cannot re-introspect table after schema change",
				slog.String("table", key), slog.Any("error", err))
			return
		}
		updated, err := newTableSchema(s.config.Database, ts.table, cols)
		if err != nil {
			s.logger.Error("table is no longer mirrorable after schema change",
				slog.String("table", key), slog.Any("error", err))
			return
		}
		for _, added := range change.AddedColumns {
			col, ok := updated.byName[added.Name]
			if !ok {
				continue
			}
			if err := s.conn.Exec(ctx, updated.addColumnDDL(col)); err != nil {
				s.logger.Error("failed to add column to ClickHouse mirror",
					slog.String("table", ts.qualified()),
					slog.String("column", col.Name),
					slog.Any("error", err))
				return
			}
			s.logger.Info("added column to ClickHouse mirror",
				slog.String("table", ts.qualified()),
				slog.String("column", col.Name),
				slog.String("type", clickhouseType(col)))
		}
		s.mu.Lock()
		s.tables[key] = updated
		s.mu.Unlock()
	}

	for _, dropped := range change.DroppedColumns {
		s.logger.Warn("column dropped upstream; kept in the ClickHouse mirror "+
			"so history is preserved (drop it manually to reclaim space)",
			slog.String("table", ts.qualified()),
			slog.String("column", dropped.Name))
	}
	for _, changed := range change.ChangedColumns {
		s.logger.Warn("column type changed upstream; the ClickHouse mirror keeps "+
			"the original type and new values are coerced into it "+
			"(migrate the target manually if that is lossy)",
			slog.String("table", ts.qualified()),
			slog.String("column", changed.Name),
			slog.String("from", changed.From.Type),
			slog.String("to", changed.To.Type))
	}
	if change.PreviousTable != "" {
		s.logger.Warn("table renamed upstream; the ClickHouse mirror keeps its "+
			"old name and new rows continue to land there",
			slog.String("target", ts.qualified()),
			slog.String("previous", change.PreviousSchema+"."+change.PreviousTable),
			slog.String("current", key))
	}
}

// lsnVersion turns an LSN into the row version. Parsing failures fall back to
// 0 so a row is still written; it simply loses to any versioned row.
func lsnVersion(lsn string) uint64 {
	parsed, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return 0
	}
	return uint64(parsed)
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
