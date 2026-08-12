package clickhouse

import (
	"strings"
	"testing"
	"time"
)

func col(name, pgType string, nullable, key bool) Column {
	return Column{Name: name, PGType: pgType, Nullable: nullable, PartOfKey: key}
}

func TestTypeMapping(t *testing.T) {
	cases := []struct {
		column Column
		want   string
	}{
		{col("id", "int4", false, true), "Int32"},
		{col("big", "int8", false, false), "Int64"},
		{col("small", "int2", true, false), "Nullable(Int16)"},
		{col("ratio", "float8", false, false), "Float64"},
		{col("flag", "bool", true, false), "Nullable(Bool)"},
		{col("name", "text", false, false), "String"},
		{col("uid", "uuid", false, true), "UUID"},
		{col("day", "date", true, false), "Nullable(Date32)"},
		{col("ts", "timestamp", false, false), "DateTime64(6)"},
		{col("tstz", "timestamptz", false, false), "DateTime64(6, 'UTC')"},
		// No ClickHouse equivalent: kept exact as text rather than coerced.
		{col("tags", "_text", true, false), "Nullable(String)"},
		{col("doc", "jsonb", true, false), "Nullable(String)"},
		{col("custom", "my_enum", false, false), "String"},
	}
	for _, tc := range cases {
		if got := clickhouseType(tc.column); got != tc.want {
			t.Errorf("clickhouseType(%s %s) = %q, want %q",
				tc.column.Name, tc.column.PGType, got, tc.want)
		}
	}
}

func TestNumericMapping(t *testing.T) {
	constrained := Column{Name: "price", PGType: "numeric", Precision: 10, Scale: 2}
	if got := clickhouseType(constrained); got != "Decimal(10, 2)" {
		t.Errorf("constrained numeric = %q, want Decimal(10, 2)", got)
	}
	// An unconstrained numeric has arbitrary precision; Decimal would
	// silently truncate it, so it stays exact as text.
	if got := clickhouseType(Column{Name: "amount", PGType: "numeric"}); got != "String" {
		t.Errorf("unconstrained numeric = %q, want String", got)
	}
	if got := clickhouseType(Column{Name: "huge", PGType: "numeric", Precision: 200}); got != "String" {
		t.Errorf("out-of-range numeric = %q, want String", got)
	}
}

// Key columns order the table, and ClickHouse forbids a Nullable ORDER BY
// column, so they must never be wrapped even if PostgreSQL says nullable.
func TestKeyColumnsAreNeverNullable(t *testing.T) {
	if got := clickhouseType(col("id", "int4", true, true)); got != "Int32" {
		t.Errorf("nullable key column = %q, want Int32", got)
	}
}

func mustSchema(t *testing.T, cols ...Column) *tableSchema {
	t.Helper()
	ts, err := newTableSchema("gtc", "users", cols)
	if err != nil {
		t.Fatalf("newTableSchema: %v", err)
	}
	return ts
}

func TestCreateDDL(t *testing.T) {
	ts := mustSchema(t,
		col("id", "int4", false, true),
		col("email", "text", true, false),
	)
	ddl := ts.createDDL()

	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `gtc`.`users`",
		"`id` Int32",
		"`email` Nullable(String)",
		"`_version` UInt64",
		"`_deleted` UInt8 DEFAULT 0",
		"ENGINE = ReplacingMergeTree(`_version`, `_deleted`)",
		"ORDER BY (`id`)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q:\n%s", want, ddl)
		}
	}
}

func TestCompositeKeyOrdersByAllKeyColumns(t *testing.T) {
	ts := mustSchema(t,
		col("tenant", "text", false, true),
		col("id", "int8", false, true),
		col("payload", "text", true, false),
	)
	if !strings.Contains(ts.createDDL(), "ORDER BY (`tenant`, `id`)") {
		t.Errorf("composite key not in ORDER BY:\n%s", ts.createDDL())
	}
}

// Without a key there is no ORDER BY, so row versions cannot be collapsed and
// deletes cannot address a row: the table must be rejected, not mirrored
// wrongly.
func TestTableWithoutPrimaryKeyIsRejected(t *testing.T) {
	_, err := newTableSchema("gtc", "logs", []Column{col("message", "text", true, false)})
	if err == nil {
		t.Fatal("a table without a primary key must be rejected")
	}
	if !strings.Contains(err.Error(), "primary key") {
		t.Errorf("error should explain the missing primary key: %v", err)
	}
}

func TestInsertDDLCoversEveryColumn(t *testing.T) {
	ts := mustSchema(t,
		col("id", "int4", false, true),
		col("email", "text", true, false),
	)
	insert := ts.insertDDL()

	if got := strings.Count(insert, "?"); got != 5 {
		t.Errorf("placeholder count = %d, want 5 (2 columns + version/deleted/synced_at)", got)
	}
	for _, want := range []string{"`id`", "`email`", "`_version`", "`_deleted`", "`_synced_at`"} {
		if !strings.Contains(insert, want) {
			t.Errorf("INSERT missing %s:\n%s", want, insert)
		}
	}
}

func TestAddColumnDDL(t *testing.T) {
	ts := mustSchema(t, col("id", "int4", false, true))
	ddl := ts.addColumnDDL(col("nickname", "text", true, false))
	want := "ALTER TABLE `gtc`.`users` ADD COLUMN IF NOT EXISTS `nickname` Nullable(String)"
	if ddl != want {
		t.Errorf("addColumnDDL = %q, want %q", ddl, want)
	}
}

func TestQuoteIdentEscapesBackticks(t *testing.T) {
	if got := quoteIdent("we`ird"); got != "`we``ird`" {
		t.Errorf("quoteIdent = %q", got)
	}
}

func TestCoerce(t *testing.T) {
	cases := []struct {
		column Column
		in     any
		want   any
	}{
		{col("id", "int4", false, true), int32(7), int32(7)},
		{col("id", "int4", false, true), int64(7), int32(7)},
		{col("big", "int8", false, false), int32(7), int64(7)},
		{col("f", "float4", false, false), float64(1.5), float32(1.5)},
		{col("b", "bool", false, false), true, true},
		{col("s", "text", false, false), "hello", "hello"},
		{col("s", "text", false, false), []byte("bytes"), "bytes"},
		// No ClickHouse equivalent: rendered the way PostgreSQL would.
		{col("tags", "_text", false, false), "{a,b}", "{a,b}"},
		{col("n", "numeric", false, false), "12.34", "12.34"},
		{col("anything", "text", true, false), nil, nil},
	}
	for _, tc := range cases {
		if got := coerce(tc.column, tc.in); got != tc.want {
			t.Errorf("coerce(%s, %#v) = %#v, want %#v", tc.column.PGType, tc.in, got, tc.want)
		}
	}

	ts := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	if got := coerce(col("t", "timestamptz", false, false), ts); got != ts {
		t.Errorf("timestamps must pass through unchanged: %v", got)
	}
}

// A tombstone writes only the key, so every other column needs a value the
// column type accepts — Nullable ones get NULL, the rest a typed zero.
func TestZeroValue(t *testing.T) {
	if got := zeroValue(col("email", "text", true, false)); got != nil {
		t.Errorf("nullable column zero = %#v, want nil", got)
	}
	if got := zeroValue(col("name", "text", false, false)); got != "" {
		t.Errorf("non-null text zero = %#v, want \"\"", got)
	}
	if got := zeroValue(col("count", "int8", false, false)); got != int64(0) {
		t.Errorf("non-null int8 zero = %#v, want int64(0)", got)
	}
	if got := zeroValue(col("flag", "bool", false, false)); got != false {
		t.Errorf("non-null bool zero = %#v, want false", got)
	}
}

func TestTargetNameDerivation(t *testing.T) {
	s := &Sink{config: Config{Database: "gtc"}}
	if got := s.targetName("public", "users"); got != "users" {
		t.Errorf("public schema target = %q, want users", got)
	}
	if got := s.targetName("billing", "invoices"); got != "billing_invoices" {
		t.Errorf("non-public schema target = %q, want billing_invoices", got)
	}

	s.config.TablePrefix = "src_"
	if got := s.targetName("public", "users"); got != "src_users" {
		t.Errorf("prefixed target = %q, want src_users", got)
	}
}

func TestLSNVersionOrdersEvents(t *testing.T) {
	earlier := lsnVersion("0/1000")
	later := lsnVersion("0/2000")
	if earlier == 0 || later <= earlier {
		t.Errorf("LSN versions must increase: %d then %d", earlier, later)
	}
	// An unparsable LSN must not panic; it simply loses to versioned rows.
	if got := lsnVersion("not-an-lsn"); got != 0 {
		t.Errorf("unparsable LSN = %d, want 0", got)
	}
}
