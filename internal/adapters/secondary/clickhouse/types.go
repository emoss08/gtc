package clickhouse

import (
	"fmt"
	"strings"
	"time"
)

// Bookkeeping columns added to every mirrored table.
const (
	// ColVersion carries the event's LSN. ReplacingMergeTree keeps the row
	// with the highest version, which is what makes replay idempotent and
	// makes a live change win over a backfilled one for the same key.
	ColVersion = "_version"
	// ColDeleted marks tombstones; ReplacingMergeTree's is_deleted column.
	ColDeleted = "_deleted"
	// ColSyncedAt is when GTC wrote the row, for freshness monitoring.
	ColSyncedAt = "_synced_at"
)

// Column is one column of a source table, as introspected from PostgreSQL.
type Column struct {
	Name string
	// PGType is the PostgreSQL type name (information_schema.udt_name).
	PGType   string
	Nullable bool
	// Precision and Scale are set for numeric columns.
	Precision int
	Scale     int
	PartOfKey bool
}

// clickhouseType maps a PostgreSQL type to the ClickHouse column type used
// for the mirror.
//
// Types that have no exact ClickHouse counterpart (arbitrary-precision
// numerics beyond Decimal's range, arrays, JSON, ranges, user-defined types)
// map to String rather than to something lossy: GTC already carries their
// exact PostgreSQL text form, and a query can cast what it needs.
func clickhouseType(col Column) string {
	base := baseType(col)
	// Key columns order the table and cannot be Nullable.
	if col.Nullable && !col.PartOfKey {
		return "Nullable(" + base + ")"
	}
	return base
}

func baseType(col Column) string {
	switch strings.ToLower(col.PGType) {
	case "bool":
		return "Bool"
	case "int2":
		return "Int16"
	case "int4":
		return "Int32"
	case "int8":
		return "Int64"
	case "float4":
		return "Float32"
	case "float8":
		return "Float64"
	case "numeric":
		// Decimal only when the declared precision fits; an unconstrained
		// numeric(no precision) has none and stays exact as text.
		if col.Precision > 0 && col.Precision <= 76 {
			return fmt.Sprintf("Decimal(%d, %d)", col.Precision, col.Scale)
		}
		return "String"
	case "uuid":
		return "UUID"
	case "date":
		return "Date32"
	case "timestamp":
		return "DateTime64(6)"
	case "timestamptz":
		return "DateTime64(6, 'UTC')"
	default:
		return "String"
	}
}

// coerce converts a decoded PostgreSQL value into something the ClickHouse
// driver accepts for the given column.
//
// GTC decodes WAL tuples with pgtype, so values already arrive as Go types
// for the common cases; this handles the remainder — anything destined for a
// String column, and the driver's insistence on exact integer widths.
func coerce(col Column, value any) any {
	if value == nil {
		return nil
	}

	switch strings.ToLower(col.PGType) {
	case "bool":
		if b, ok := value.(bool); ok {
			return b
		}
		return fmt.Sprint(value) == "true"
	case "int2":
		return int16(toInt64(value))
	case "int4":
		return int32(toInt64(value))
	case "int8":
		return toInt64(value)
	case "float4":
		return float32(toFloat64(value))
	case "float8":
		return toFloat64(value)
	case "date", "timestamp", "timestamptz":
		if t, ok := value.(time.Time); ok {
			return t
		}
		return value
	case "uuid":
		return fmt.Sprint(value)
	}

	// String columns: keep text as-is, render everything else (arrays,
	// JSON, numerics, unknown types) the way PostgreSQL would.
	if s, ok := value.(string); ok {
		return s
	}
	if b, ok := value.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(value)
}

func toInt64(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	}
	var parsed int64
	_, _ = fmt.Sscan(fmt.Sprint(value), &parsed)
	return parsed
}

func toFloat64(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case int:
		return float64(v)
	}
	var parsed float64
	_, _ = fmt.Sscan(fmt.Sprint(value), &parsed)
	return parsed
}

// zeroValue is what a tombstone (or a row missing a column) writes for a
// non-key column, since ReplacingMergeTree replaces whole rows.
func zeroValue(col Column) any {
	if col.Nullable && !col.PartOfKey {
		return nil
	}
	switch strings.ToLower(col.PGType) {
	case "bool":
		return false
	case "int2":
		return int16(0)
	case "int4":
		return int32(0)
	case "int8":
		return int64(0)
	case "float4":
		return float32(0)
	case "float8":
		return float64(0)
	case "date", "timestamp", "timestamptz":
		return time.Unix(0, 0).UTC()
	case "uuid":
		return "00000000-0000-0000-0000-000000000000"
	case "numeric":
		if col.Precision > 0 && col.Precision <= 76 {
			return 0
		}
		return ""
	default:
		return ""
	}
}

// quoteIdent quotes a ClickHouse identifier.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
