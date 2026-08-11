package wal

import (
	"slices"
	"strings"
	"testing"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// rel builds a relation description; use key() for columns in the replica
// identity and plain() for the rest.
func rel(namespace, name string, identity uint8, cols ...column) *pglogrepl.RelationMessageV2 {
	msg := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			RelationID:      42,
			Namespace:       namespace,
			RelationName:    name,
			ReplicaIdentity: identity,
		},
	}
	for _, c := range cols {
		var flags uint8
		if c.key {
			flags = 1
		}
		msg.Columns = append(msg.Columns, &pglogrepl.RelationMessageColumn{
			Name:     c.name,
			DataType: c.oid,
			Flags:    flags,
		})
	}
	return msg
}

type column struct {
	name string
	oid  uint32
	key  bool
}

func key(name string, oid uint32) column   { return column{name: name, oid: oid, key: true} }
func plain(name string, oid uint32) column { return column{name: name, oid: oid} }

const identityDefault, identityFull = 'd', 'f'

func diff(prev, cur *pglogrepl.RelationMessageV2) *domain.SchemaChange {
	return NewDecoder().diffRelation(prev, cur, pglogrepl.LSN(0x1000))
}

func TestDiffDetectsAddedColumn(t *testing.T) {
	change := diff(
		rel("public", "users", identityDefault, key("id", pgtype.Int4OID)),
		rel("public", "users", identityDefault, key("id", pgtype.Int4OID), plain("email", pgtype.TextOID)),
	)
	if change == nil {
		t.Fatal("adding a column must be reported")
	}
	if !slices.Equal(change.Kinds, []string{domain.SchemaChangeColumnAdded}) {
		t.Fatalf("kinds = %v", change.Kinds)
	}
	if len(change.AddedColumns) != 1 || change.AddedColumns[0].Name != "email" {
		t.Fatalf("added = %+v", change.AddedColumns)
	}
	if got := change.AddedColumns[0].Type; got != "text" {
		t.Errorf("type name = %q, want text", got)
	}
	if change.Breaking() {
		t.Error("adding a column is additive and must not be flagged breaking")
	}
}

func TestDiffDetectsDroppedAndRetypedColumns(t *testing.T) {
	change := diff(
		rel("public", "users", identityDefault,
			key("id", pgtype.Int4OID), plain("nickname", pgtype.TextOID), plain("age", pgtype.Int4OID)),
		rel("public", "users", identityDefault,
			key("id", pgtype.Int4OID), plain("age", pgtype.Int8OID)),
	)
	if change == nil {
		t.Fatal("dropping and retyping columns must be reported")
	}
	if len(change.DroppedColumns) != 1 || change.DroppedColumns[0].Name != "nickname" {
		t.Errorf("dropped = %+v", change.DroppedColumns)
	}
	if len(change.ChangedColumns) != 1 {
		t.Fatalf("changed = %+v", change.ChangedColumns)
	}
	got := change.ChangedColumns[0]
	if got.Name != "age" || got.From.Type != "int4" || got.To.Type != "int8" {
		t.Errorf("type change = %+v", got)
	}
	if !change.Breaking() {
		t.Error("dropping a column must be flagged breaking")
	}
}

func TestDiffDetectsReplicaIdentityChange(t *testing.T) {
	change := diff(
		rel("public", "users", identityDefault, key("id", pgtype.Int4OID), plain("email", pgtype.TextOID)),
		rel("public", "users", identityFull, key("id", pgtype.Int4OID), key("email", pgtype.TextOID)),
	)
	if change == nil {
		t.Fatal("a replica identity change must be reported")
	}
	if !slices.Contains(change.Kinds, domain.SchemaChangeReplicaIdentity) {
		t.Fatalf("kinds = %v", change.Kinds)
	}
	if change.PreviousReplicaIdentity != "d" || change.ReplicaIdentity != "f" {
		t.Errorf("identity %q -> %q", change.PreviousReplicaIdentity, change.ReplicaIdentity)
	}
	if !slices.Equal(change.KeyColumns, []string{"id", "email"}) {
		t.Errorf("key columns = %v", change.KeyColumns)
	}
	if !change.Breaking() {
		t.Error("a replica identity change alters how rows are addressed; must be breaking")
	}
}

// The identity setting can stay "default" while the primary key itself is
// replaced, which changes how DELETEs identify rows.
func TestDiffDetectsKeyChangeWithSameIdentitySetting(t *testing.T) {
	change := diff(
		rel("public", "users", identityDefault, key("id", pgtype.Int4OID), plain("uuid", pgtype.UUIDOID)),
		rel("public", "users", identityDefault, plain("id", pgtype.Int4OID), key("uuid", pgtype.UUIDOID)),
	)
	if change == nil {
		t.Fatal("a primary key swap must be reported")
	}
	if !slices.Contains(change.Kinds, domain.SchemaChangeKeyChanged) {
		t.Fatalf("kinds = %v", change.Kinds)
	}
	if !change.Breaking() {
		t.Error("a key change must be breaking")
	}
}

func TestDiffDetectsRename(t *testing.T) {
	change := diff(
		rel("public", "users", identityDefault, key("id", pgtype.Int4OID)),
		rel("app", "accounts", identityDefault, key("id", pgtype.Int4OID)),
	)
	if change == nil {
		t.Fatal("a rename must be reported")
	}
	if !slices.Contains(change.Kinds, domain.SchemaChangeTableRenamed) {
		t.Fatalf("kinds = %v", change.Kinds)
	}
	if change.PreviousSchema != "public" || change.PreviousTable != "users" {
		t.Errorf("previous = %s.%s", change.PreviousSchema, change.PreviousTable)
	}
	if change.Schema != "app" || change.Table != "accounts" {
		t.Errorf("current = %s.%s", change.Schema, change.Table)
	}
}

// PostgreSQL re-sends the current relation after a reconnect; an identical
// description is not a DDL change and must stay silent.
func TestDiffIgnoresIdenticalRelation(t *testing.T) {
	cols := []column{key("id", pgtype.Int4OID), plain("email", pgtype.TextOID)}
	if change := diff(
		rel("public", "users", identityDefault, cols...),
		rel("public", "users", identityDefault, cols...),
	); change != nil {
		t.Fatalf("identical relations must not produce a change: %+v", change)
	}
}

// The first relation for a table describes it; there is nothing to diff
// against, so it must not look like a DDL change.
func TestFirstRelationIsNotAChange(t *testing.T) {
	d := NewDecoder()
	msg := rel("public", "users", identityDefault, key("id", pgtype.Int4OID))

	body := relationBody(msg.RelationID, msg.Namespace, msg.RelationName,
		[]relCol{{name: "id", oid: pgtype.Int4OID}})
	result, err := d.decodeWALData(body, 0x1000)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.SchemaChanges) != 0 {
		t.Fatalf("first relation reported as a change: %+v", result.SchemaChanges)
	}
	if _, ok := d.relations[msg.RelationID]; !ok {
		t.Error("relation was not remembered for later diffs")
	}
}

func TestUnknownTypeOIDFallsBackToOID(t *testing.T) {
	change := diff(
		rel("public", "t", identityDefault, key("id", pgtype.Int4OID)),
		rel("public", "t", identityDefault, key("id", pgtype.Int4OID), plain("geom", 999999)),
	)
	if change == nil || len(change.AddedColumns) != 1 {
		t.Fatalf("expected one added column, got %+v", change)
	}
	if got := change.AddedColumns[0].Type; got != "oid:999999" {
		t.Errorf("unknown type = %q, want oid:999999", got)
	}
}

func TestSummaryDescribesTheChange(t *testing.T) {
	change := diff(
		rel("public", "users", identityDefault,
			key("id", pgtype.Int4OID), plain("nickname", pgtype.TextOID), plain("age", pgtype.Int4OID)),
		rel("public", "users", identityDefault,
			key("id", pgtype.Int4OID), plain("age", pgtype.Int8OID), plain("email", pgtype.TextOID)),
	)
	summary := change.Summary()
	for _, want := range []string{"added email", "dropped nickname", "age int4->int8"} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary %q missing %q", summary, want)
		}
	}
}
