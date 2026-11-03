package wal

import (
	"github.com/bytedance/sonic"
	"github.com/jackc/pgx/v5/pgtype"
)

// NewTypeMap returns the type map used to decode column values, both from the
// WAL and from backfill selects, so the two paths produce identical Go values.
// json and jsonb are decoded with sonic: they are the costliest columns to
// decode, and sonic yields the same values as encoding/json (maps, slices,
// float64 numbers) several times faster.
func NewTypeMap() *pgtype.Map {
	m := pgtype.NewMap()

	jsonType := &pgtype.Type{Name: "json", OID: pgtype.JSONOID,
		Codec: &pgtype.JSONCodec{Marshal: sonic.Marshal, Unmarshal: sonic.Unmarshal}}
	jsonbType := &pgtype.Type{Name: "jsonb", OID: pgtype.JSONBOID,
		Codec: &pgtype.JSONBCodec{Marshal: sonic.Marshal, Unmarshal: sonic.Unmarshal}}
	m.RegisterType(jsonType)
	m.RegisterType(jsonbType)
	// Array codecs hold their element type, so the arrays are re-registered
	// to pick up the new element codecs.
	m.RegisterType(&pgtype.Type{Name: "_json", OID: pgtype.JSONArrayOID,
		Codec: &pgtype.ArrayCodec{ElementType: jsonType}})
	m.RegisterType(&pgtype.Type{Name: "_jsonb", OID: pgtype.JSONBArrayOID,
		Codec: &pgtype.ArrayCodec{ElementType: jsonbType}})
	return m
}
