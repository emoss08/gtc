package domain

import (
	"fmt"
	"strings"
	"time"
)

// Schema-change kinds, used as metric labels and in the JSON payload.
const (
	SchemaChangeColumnAdded       = "column_added"
	SchemaChangeColumnDropped     = "column_dropped"
	SchemaChangeColumnTypeChanged = "column_type_changed"
	SchemaChangeKeyChanged        = "key_changed"
	SchemaChangeReplicaIdentity   = "replica_identity_changed"
	SchemaChangeTableRenamed      = "table_renamed"
)

// ColumnDef describes one column of a published table.
type ColumnDef struct {
	Name string `json:"name"`
	// Type is the PostgreSQL type name (e.g. "int4", "text"), resolved from
	// TypeOID; it falls back to "oid:<n>" for types the client cannot name.
	Type    string `json:"type"`
	TypeOID uint32 `json:"type_oid"`
	// PartOfKey reports membership in the replica identity — the columns
	// PostgreSQL sends as the old tuple on UPDATE and DELETE.
	PartOfKey bool `json:"part_of_key"`
}

// ColumnTypeChange records a column whose type changed in place.
type ColumnTypeChange struct {
	Name string    `json:"name"`
	From ColumnDef `json:"from"`
	To   ColumnDef `json:"to"`
}

// SchemaChange is a DDL change detected on a published table.
//
// PostgreSQL's logical decoding does not emit DDL statements. It does re-send
// a relation description whenever a published table's shape changes, just
// before the next row change for that table, so GTC reports the *effect* of
// the DDL (columns added, dropped, retyped, replica identity, renames) rather
// than the statement text. Several statements that land between two row
// changes are therefore reported as one combined change.
type SchemaChange struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	// RelationID is PostgreSQL's table OID; it survives renames, which is
	// how a rename is told apart from a new table.
	RelationID uint32 `json:"relation_id"`
	// PreviousSchema and PreviousTable are set only when the table was
	// renamed or moved between schemas.
	PreviousSchema string `json:"previous_schema,omitempty"`
	PreviousTable  string `json:"previous_table,omitempty"`

	AddedColumns   []ColumnDef        `json:"added_columns,omitempty"`
	DroppedColumns []ColumnDef        `json:"dropped_columns,omitempty"`
	ChangedColumns []ColumnTypeChange `json:"changed_columns,omitempty"`

	// KeyColumns is the replica identity after the change; PreviousKeyColumns
	// is what it was before.
	KeyColumns         []string `json:"key_columns,omitempty"`
	PreviousKeyColumns []string `json:"previous_key_columns,omitempty"`
	// ReplicaIdentity is PostgreSQL's setting: "d" (default), "n" (nothing),
	// "f" (full), or "i" (index).
	ReplicaIdentity         string `json:"replica_identity"`
	PreviousReplicaIdentity string `json:"previous_replica_identity,omitempty"`

	// Kinds lists what changed, in a stable order.
	Kinds []string `json:"kinds"`

	LSN           string    `json:"lsn"`
	TransactionID uint32    `json:"transaction_id"`
	Timestamp     time.Time `json:"timestamp"`
}

func (s *SchemaChange) FullTableName() string {
	return fmt.Sprintf("%s.%s", s.Schema, s.Table)
}

// Breaking reports whether the change can break consumers that already hold
// documents for this table: data disappears (dropped column), is reinterpreted
// (type change), or is addressed differently (key or identity change).
// Adding a column is additive and never breaking.
func (s *SchemaChange) Breaking() bool {
	for _, kind := range s.Kinds {
		switch kind {
		case SchemaChangeColumnDropped,
			SchemaChangeColumnTypeChanged,
			SchemaChangeKeyChanged,
			SchemaChangeReplicaIdentity,
			SchemaChangeTableRenamed:
			return true
		}
	}
	return false
}

// Summary is a one-line human-readable description, used in logs and the
// dashboard.
func (s *SchemaChange) Summary() string {
	var parts []string
	if s.PreviousTable != "" {
		parts = append(parts, fmt.Sprintf("renamed from %s.%s",
			s.PreviousSchema, s.PreviousTable))
	}
	if names := columnNames(s.AddedColumns); len(names) > 0 {
		parts = append(parts, "added "+strings.Join(names, ", "))
	}
	if names := columnNames(s.DroppedColumns); len(names) > 0 {
		parts = append(parts, "dropped "+strings.Join(names, ", "))
	}
	for _, c := range s.ChangedColumns {
		parts = append(parts, fmt.Sprintf("%s %s->%s", c.Name, c.From.Type, c.To.Type))
	}
	if s.PreviousReplicaIdentity != "" {
		parts = append(parts, fmt.Sprintf("replica identity %s->%s",
			s.PreviousReplicaIdentity, s.ReplicaIdentity))
	} else if slicesDiffer(s.PreviousKeyColumns, s.KeyColumns) {
		parts = append(parts, fmt.Sprintf("key [%s]->[%s]",
			strings.Join(s.PreviousKeyColumns, " "), strings.Join(s.KeyColumns, " ")))
	}
	if len(parts) == 0 {
		return "no change"
	}
	return strings.Join(parts, "; ")
}

func columnNames(cols []ColumnDef) []string {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.Name)
	}
	return names
}

func slicesDiffer(a, b []string) bool {
	if len(a) != len(b) {
		return true
	}
	for i := range a {
		if a[i] != b[i] {
			return true
		}
	}
	return false
}
