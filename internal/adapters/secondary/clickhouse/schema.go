package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// introspectQuery reads a table's columns, their types, and which of them
// form the primary key. ClickHouse needs real types to create the mirror,
// and CDC events carry only decoded values — so the shape comes from the
// source catalog rather than from guessing at runtime.
const introspectQuery = `
SELECT
    a.attname                                        AS name,
    t.typname                                        AS pg_type,
    NOT a.attnotnull                                 AS nullable,
    COALESCE(information_schema._pg_numeric_precision(a.atttypid, a.atttypmod), 0) AS precision,
    COALESCE(information_schema._pg_numeric_scale(a.atttypid, a.atttypmod), 0)     AS scale,
    COALESCE(k.is_key, false)                        AS part_of_key
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN (
    SELECT unnest(i.indkey) AS attnum, true AS is_key
    FROM pg_index i
    JOIN pg_class c2 ON c2.oid = i.indrelid
    JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
    WHERE n2.nspname = $1 AND c2.relname = $2 AND i.indisprimary
) k ON k.attnum = a.attnum
WHERE n.nspname = $1
  AND c.relname = $2
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum`

// Introspector reads table shapes from the source database.
type Introspector struct {
	conn *pgx.Conn
}

func NewIntrospector(ctx context.Context, databaseURL string) (*Introspector, error) {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to source database for schema introspection: %w", err)
	}
	return &Introspector{conn: conn}, nil
}

func (i *Introspector) Close(ctx context.Context) error {
	if i.conn == nil {
		return nil
	}
	return i.conn.Close(ctx)
}

func (i *Introspector) Columns(ctx context.Context, schema, table string) ([]Column, error) {
	rows, err := i.conn.Query(ctx, introspectQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("introspect %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []Column
	for rows.Next() {
		var c Column
		if err := rows.Scan(&c.Name, &c.PGType, &c.Nullable, &c.Precision, &c.Scale, &c.PartOfKey); err != nil {
			return nil, fmt.Errorf("scan column of %s.%s: %w", schema, table, err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read columns of %s.%s: %w", schema, table, err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %s.%s has no columns or does not exist", schema, table)
	}
	return cols, nil
}

// tableSchema is a mirrored table: the source columns plus where they live in
// ClickHouse.
type tableSchema struct {
	database string
	table    string
	columns  []Column
	keys     []Column
	byName   map[string]Column
}

func newTableSchema(database, table string, cols []Column) (*tableSchema, error) {
	ts := &tableSchema{
		database: database,
		table:    table,
		columns:  cols,
		byName:   make(map[string]Column, len(cols)),
	}
	for _, c := range cols {
		ts.byName[c.Name] = c
		if c.PartOfKey {
			ts.keys = append(ts.keys, c)
		}
	}
	if len(ts.keys) == 0 {
		// Without a key there is no ORDER BY, so ReplacingMergeTree cannot
		// collapse versions of a row and DELETEs cannot address one.
		return nil, fmt.Errorf(
			"table has no primary key; ClickHouse mirroring needs one to " +
				"deduplicate row versions (add a primary key, or exclude the " +
				"table with CDC_EXCLUDED_TABLES)")
	}
	return ts, nil
}

func (t *tableSchema) qualified() string {
	return quoteIdent(t.database) + "." + quoteIdent(t.table)
}

// createDDL builds the target table. ReplacingMergeTree(_version, _deleted)
// keeps only the newest version of each key and lets OPTIMIZE ... CLEANUP
// drop tombstones.
func (t *tableSchema) createDDL() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", t.qualified())
	for _, c := range t.columns {
		fmt.Fprintf(&b, "    %s %s,\n", quoteIdent(c.Name), clickhouseType(c))
	}
	fmt.Fprintf(&b, "    %s UInt64,\n", quoteIdent(ColVersion))
	fmt.Fprintf(&b, "    %s UInt8 DEFAULT 0,\n", quoteIdent(ColDeleted))
	fmt.Fprintf(&b, "    %s DateTime64(3) DEFAULT now64(3)\n", quoteIdent(ColSyncedAt))
	fmt.Fprintf(&b, ") ENGINE = ReplacingMergeTree(%s, %s)\nORDER BY (%s)",
		quoteIdent(ColVersion), quoteIdent(ColDeleted), t.orderBy())
	return b.String()
}

func (t *tableSchema) orderBy() string {
	names := make([]string, 0, len(t.keys))
	for _, k := range t.keys {
		names = append(names, quoteIdent(k.Name))
	}
	return strings.Join(names, ", ")
}

// addColumnDDL extends an existing mirror with a column added upstream.
func (t *tableSchema) addColumnDDL(col Column) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
		t.qualified(), quoteIdent(col.Name), clickhouseType(col))
}

// insertDDL builds the INSERT for every column of the mirror.
func (t *tableSchema) insertDDL() string {
	names := make([]string, 0, len(t.columns)+3)
	placeholders := make([]string, 0, len(t.columns)+3)
	for _, c := range t.columns {
		names = append(names, quoteIdent(c.Name))
		placeholders = append(placeholders, "?")
	}
	for _, c := range []string{ColVersion, ColDeleted, ColSyncedAt} {
		names = append(names, quoteIdent(c))
		placeholders = append(placeholders, "?")
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		t.qualified(), strings.Join(names, ", "), strings.Join(placeholders, ", "))
}
