package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/emoss08/gtc/internal/adapters/primary/wal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is one backfilled row: decoded column values plus a canonical
// primary-key string used to match the row against live stream events.
type Row struct {
	Key    string
	Values map[string]any
}

// Chunk is the result of one keyset-paginated select.
type Chunk struct {
	Rows []Row
	// LastCursor holds the raw text values of the last row's primary key,
	// used as the next chunk's starting point (and persisted for resume).
	LastCursor []string
}

// DB abstracts the source-database operations the coordinator needs, so the
// chunking state machine is unit-testable without PostgreSQL.
type DB interface {
	EmitWatermark(ctx context.Context, payload []byte) error
	PrimaryKey(ctx context.Context, schema, table string) ([]string, error)
	SelectChunk(ctx context.Context, schema, table string, pkCols, cursor []string, limit int) (*Chunk, error)
	PublicationTables(ctx context.Context, publication string) ([][2]string, error)

	EnsureStateTable(ctx context.Context) error
	LoadState(ctx context.Context) (map[string]TableState, error)
	SaveState(ctx context.Context, table string, cursor []string, done bool) error
	ClearState(ctx context.Context, table string) error

	Close()
}

// TableState is the persisted resume point for one table.
type TableState struct {
	Cursor []string
	Done   bool
}

type pgDB struct {
	pool       *pgxpool.Pool
	stateTable string // "" disables persistence
	typeMap    *pgtype.Map
}

// NonReplicationURL strips the replication parameter from a PostgreSQL URL so
// the backfill can open regular connections against the same database.
func NonReplicationURL(databaseURL string) string {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return databaseURL
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	return u.String()
}

func NewDB(ctx context.Context, databaseURL, stateTable string) (DB, error) {
	cfg, err := pgxpool.ParseConfig(NonReplicationURL(databaseURL))
	if err != nil {
		return nil, fmt.Errorf("parse backfill database URL: %w", err)
	}
	cfg.MaxConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect for backfill: %w", err)
	}

	return &pgDB{pool: pool, stateTable: stateTable, typeMap: pgtype.NewMap()}, nil
}

func (d *pgDB) Close() {
	d.pool.Close()
}

func (d *pgDB) EmitWatermark(ctx context.Context, payload []byte) error {
	_, err := d.pool.Exec(ctx,
		"SELECT pg_logical_emit_message(false, $1, $2::text)",
		wal.WatermarkPrefix, string(payload),
	)
	return err
}

func (d *pgDB) PrimaryKey(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`,
		quoteIdent(schema), quoteIdent(table),
	)
	if err != nil {
		return nil, fmt.Errorf("primary key lookup for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func (d *pgDB) SelectChunk(
	ctx context.Context,
	schema, table string,
	pkCols, cursor []string,
	limit int,
) (*Chunk, error) {
	quotedPK := make([]string, len(pkCols))
	for i, col := range pkCols {
		quotedPK[i] = quoteIdent(col)
	}
	pkList := strings.Join(quotedPK, ", ")

	var sb strings.Builder
	args := make([]any, 0, len(cursor))
	fmt.Fprintf(&sb, "SELECT * FROM %s.%s", quoteIdent(schema), quoteIdent(table))
	if len(cursor) > 0 {
		placeholders := make([]string, len(cursor))
		for i, c := range cursor {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, c)
		}
		fmt.Fprintf(&sb, " WHERE (%s) > (%s)", pkList, strings.Join(placeholders, ", "))
	}
	fmt.Fprintf(&sb, " ORDER BY %s LIMIT %d", pkList, limit)

	// Simple protocol forces text-format results so rows decode through the
	// same path as streamed WAL tuples, producing identical Go values.
	queryArgs := append([]any{pgx.QueryExecModeSimpleProtocol}, args...)
	rows, err := d.pool.Query(ctx, sb.String(), queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("select chunk from %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	colIdx := make(map[string]int, len(fields))
	for i, f := range fields {
		colIdx[f.Name] = i
	}
	for _, col := range pkCols {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("primary key column %q missing from %s.%s select", col, schema, table)
		}
	}

	chunk := &Chunk{}
	for rows.Next() {
		raw := rows.RawValues()
		values := make(map[string]any, len(fields))
		for i, f := range fields {
			if raw[i] == nil {
				values[f.Name] = nil
				continue
			}
			val, decErr := wal.DecodeText(d.typeMap, f.DataTypeOID, raw[i])
			if decErr != nil {
				values[f.Name] = string(raw[i])
			} else {
				values[f.Name] = val
			}
		}

		keyParts := make([]string, len(pkCols))
		cursorParts := make([]string, len(pkCols))
		for i, col := range pkCols {
			keyParts[i] = fmt.Sprintf("%v", values[col])
			cursorParts[i] = string(raw[colIdx[col]])
		}

		chunk.Rows = append(chunk.Rows, Row{
			Key:    strings.Join(keyParts, "\x1f"),
			Values: values,
		})
		chunk.LastCursor = cursorParts
	}
	return chunk, rows.Err()
}

func (d *pgDB) PublicationTables(ctx context.Context, publication string) ([][2]string, error) {
	rows, err := d.pool.Query(ctx,
		"SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY schemaname, tablename",
		publication,
	)
	if err != nil {
		return nil, fmt.Errorf("list publication tables: %w", err)
	}
	defer rows.Close()

	var tables [][2]string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		tables = append(tables, [2]string{schema, table})
	}
	return tables, rows.Err()
}

func (d *pgDB) EnsureStateTable(ctx context.Context) error {
	if d.stateTable == "" {
		return nil
	}
	_, err := d.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			table_name text PRIMARY KEY,
			cursor     jsonb,
			done       boolean NOT NULL DEFAULT false,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`, quoteIdent(d.stateTable)))
	return err
}

func (d *pgDB) LoadState(ctx context.Context) (map[string]TableState, error) {
	state := make(map[string]TableState)
	if d.stateTable == "" {
		return state, nil
	}

	rows, err := d.pool.Query(ctx, fmt.Sprintf(
		"SELECT table_name, cursor, done FROM %s", quoteIdent(d.stateTable)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var cursorJSON []byte
		var done bool
		if err := rows.Scan(&name, &cursorJSON, &done); err != nil {
			return nil, err
		}
		ts := TableState{Done: done}
		if len(cursorJSON) > 0 {
			if err := json.Unmarshal(cursorJSON, &ts.Cursor); err != nil {
				return nil, fmt.Errorf("corrupt cursor for %s: %w", name, err)
			}
		}
		state[name] = ts
	}
	return state, rows.Err()
}

func (d *pgDB) SaveState(ctx context.Context, table string, cursor []string, done bool) error {
	if d.stateTable == "" {
		return nil
	}
	cursorJSON, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (table_name, cursor, done, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (table_name)
		DO UPDATE SET cursor = $2, done = $3, updated_at = now()`,
		quoteIdent(d.stateTable)),
		table, cursorJSON, done,
	)
	return err
}

func (d *pgDB) ClearState(ctx context.Context, table string) error {
	if d.stateTable == "" {
		return nil
	}
	_, err := d.pool.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE table_name = $1", quoteIdent(d.stateTable)), table)
	return err
}

func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
