package wal

import (
	"fmt"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map
	txn       txnState
}

// txnState carries Begin-message data so DML events can be stamped with the
// transaction's commit time and XID, and so keepalives are only acknowledged
// outside of an open transaction.
type txnState struct {
	inTxn      bool
	xid        uint32
	commitTime time.Time
	// finalLSN is the transaction's commit position, used to place messages
	// that PostgreSQL sends without a WAL position of their own.
	finalLSN pglogrepl.LSN
}

func NewDecoder() *Decoder {
	return &Decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:   pgtype.NewMap(),
	}
}

type DecodeResult struct {
	Events []domain.CDCEvent
	// SchemaChanges are DDL changes detected by diffing this position's
	// relation descriptions against the previously seen ones.
	SchemaChanges []domain.SchemaChange
	// Message is a logical decoding message (pg_logical_emit_message)
	// found at this stream position; used for backfill watermarks.
	Message *LogicalMessage
	// AckLSN, when non-zero, is a position that is safe to confirm to the
	// server: either the end of a committed transaction whose events have
	// all been emitted, or the server's WAL end from a keepalive received
	// outside of any transaction.
	AckLSN pglogrepl.LSN
	// ServerWALEnd is the server's current WAL end position, used for lag
	// reporting. It must never be confirmed as processed.
	ServerWALEnd pglogrepl.LSN
	// RequestReply is set when the server asked for an immediate standby
	// status update.
	RequestReply bool
}

type LogicalMessage struct {
	Prefix  string
	Content []byte
	LSN     pglogrepl.LSN
}

func (d *Decoder) Decode(rawMsg pgproto3.BackendMessage) (*DecodeResult, error) {
	copyData, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return &DecodeResult{}, nil
	}

	switch copyData.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
		if err != nil {
			return nil, err
		}
		result := &DecodeResult{
			ServerWALEnd: pkm.ServerWALEnd,
			RequestReply: pkm.ReplyRequested,
		}
		// Only acknowledge the server's WAL end while no transaction is in
		// flight; inside a transaction it would confirm events that have
		// not been processed yet.
		if !d.txn.inTxn {
			result.AckLSN = pkm.ServerWALEnd
		}
		return result, nil

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			return nil, err
		}
		result, err := d.decodeWALData(xld.WALData, xld.WALStart)
		if err != nil {
			return nil, err
		}
		result.ServerWALEnd = xld.ServerWALEnd
		return result, nil
	}

	return &DecodeResult{}, nil
}

func (d *Decoder) decodeWALData(walData []byte, lsn pglogrepl.LSN) (*DecodeResult, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return nil, err
	}

	result := &DecodeResult{}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		d.txn.inTxn = true
		d.txn.xid = msg.Xid
		d.txn.commitTime = msg.CommitTime
		d.txn.finalLSN = msg.FinalLSN

	case *pglogrepl.CommitMessage:
		d.txn.inTxn = false
		// The whole transaction has been decoded and every event handed to
		// the handler synchronously, so its end position is safe to confirm.
		result.AckLSN = msg.TransactionEndLSN

	case *pglogrepl.RelationMessageV2:
		// PostgreSQL re-sends a relation description whenever a published
		// table's shape changed, just before the next row change for it.
		// Diffing against the previous one is how GTC detects DDL.
		if prev, ok := d.relations[msg.RelationID]; ok {
			if change := d.diffRelation(prev, msg, lsn); change != nil {
				result.SchemaChanges = append(result.SchemaChanges, *change)
			}
		}
		d.relations[msg.RelationID] = msg

	case *pglogrepl.InsertMessageV2:
		rel, err := d.relation(msg.RelationID)
		if err != nil {
			return nil, err
		}
		newData, _ := d.decodeTuple(msg.Tuple, rel)
		result.Events = append(result.Events, d.newEvent(lsn, domain.OperationInsert, rel, nil, newData, nil))

	case *pglogrepl.UpdateMessageV2:
		rel, err := d.relation(msg.RelationID)
		if err != nil {
			return nil, err
		}
		newData, unchangedToast := d.decodeTuple(msg.NewTuple, rel)
		var oldData map[string]any
		if msg.OldTuple != nil {
			oldData, _ = d.decodeTuple(msg.OldTuple, rel)
		}
		result.Events = append(result.Events, d.newEvent(lsn, domain.OperationUpdate, rel, oldData, newData, unchangedToast))

	case *pglogrepl.DeleteMessageV2:
		rel, err := d.relation(msg.RelationID)
		if err != nil {
			return nil, err
		}
		oldData, _ := d.decodeTuple(msg.OldTuple, rel)
		result.Events = append(result.Events, d.newEvent(lsn, domain.OperationDelete, rel, oldData, nil, nil))

	case *pglogrepl.TruncateMessageV2:
		for i, relID := range msg.RelationIDs {
			rel, err := d.relation(relID)
			if err != nil {
				return nil, err
			}
			event := d.newEvent(lsn, domain.OperationTruncate, rel, nil, nil, nil)
			event.ID = fmt.Sprintf("%s-%d", lsn, i)
			result.Events = append(result.Events, event)
		}

	case *pglogrepl.LogicalDecodingMessageV2:
		result.Message = &LogicalMessage{
			Prefix:  msg.Prefix,
			Content: msg.Content,
			LSN:     msg.LSN,
		}

	case *pglogrepl.StreamStartMessageV2, *pglogrepl.StreamStopMessageV2,
		*pglogrepl.StreamCommitMessageV2, *pglogrepl.StreamAbortMessageV2:
		// Streaming of in-progress transactions is not requested; receiving
		// one of these means the plugin arguments and reality disagree.
		return nil, fmt.Errorf("unexpected streamed-transaction message %T (streaming is disabled)", msg)
	}

	return result, nil
}

// diffRelation reports what changed between two descriptions of the same
// table, or nil when they are identical (PostgreSQL re-sends an unchanged
// relation after a reconnect, and that is not a DDL change).
func (d *Decoder) diffRelation(
	prev, cur *pglogrepl.RelationMessageV2,
	lsn pglogrepl.LSN,
) *domain.SchemaChange {
	// PostgreSQL sends relation descriptions without a WAL position of their
	// own. Fall back to the commit position of the transaction they arrived
	// in, so the change can still be ordered against row events.
	if lsn == 0 {
		lsn = d.txn.finalLSN
	}
	change := domain.SchemaChange{
		Schema:             cur.Namespace,
		Table:              cur.RelationName,
		RelationID:         cur.RelationID,
		KeyColumns:         keyColumns(cur),
		PreviousKeyColumns: keyColumns(prev),
		ReplicaIdentity:    string(cur.ReplicaIdentity),
		LSN:                lsn.String(),
		TransactionID:      d.txn.xid,
		Timestamp:          d.txn.commitTime,
	}

	if prev.Namespace != cur.Namespace || prev.RelationName != cur.RelationName {
		change.PreviousSchema = prev.Namespace
		change.PreviousTable = prev.RelationName
		change.Kinds = append(change.Kinds, domain.SchemaChangeTableRenamed)
	}

	prevCols := make(map[string]*pglogrepl.RelationMessageColumn, len(prev.Columns))
	for i := range prev.Columns {
		prevCols[prev.Columns[i].Name] = prev.Columns[i]
	}
	curCols := make(map[string]*pglogrepl.RelationMessageColumn, len(cur.Columns))
	for i := range cur.Columns {
		curCols[cur.Columns[i].Name] = cur.Columns[i]
	}

	// Iterate the relation's own column order so output is deterministic.
	for i := range cur.Columns {
		col := cur.Columns[i]
		before, existed := prevCols[col.Name]
		if !existed {
			change.AddedColumns = append(change.AddedColumns, d.columnDef(col))
			continue
		}
		if before.DataType != col.DataType {
			change.ChangedColumns = append(change.ChangedColumns, domain.ColumnTypeChange{
				Name: col.Name,
				From: d.columnDef(before),
				To:   d.columnDef(col),
			})
		}
	}
	for i := range prev.Columns {
		col := prev.Columns[i]
		if _, stillThere := curCols[col.Name]; !stillThere {
			change.DroppedColumns = append(change.DroppedColumns, d.columnDef(col))
		}
	}

	if len(change.AddedColumns) > 0 {
		change.Kinds = append(change.Kinds, domain.SchemaChangeColumnAdded)
	}
	if len(change.DroppedColumns) > 0 {
		change.Kinds = append(change.Kinds, domain.SchemaChangeColumnDropped)
	}
	if len(change.ChangedColumns) > 0 {
		change.Kinds = append(change.Kinds, domain.SchemaChangeColumnTypeChanged)
	}
	if prev.ReplicaIdentity != cur.ReplicaIdentity {
		change.PreviousReplicaIdentity = string(prev.ReplicaIdentity)
		change.Kinds = append(change.Kinds, domain.SchemaChangeReplicaIdentity)
	} else if !equalStrings(change.PreviousKeyColumns, change.KeyColumns) {
		// The identity setting is unchanged but its columns are not: the
		// primary key or the identity index itself was altered.
		change.Kinds = append(change.Kinds, domain.SchemaChangeKeyChanged)
	}

	if len(change.Kinds) == 0 {
		return nil
	}
	return &change
}

func (d *Decoder) columnDef(col *pglogrepl.RelationMessageColumn) domain.ColumnDef {
	return domain.ColumnDef{
		Name:      col.Name,
		Type:      d.typeName(col.DataType),
		TypeOID:   col.DataType,
		PartOfKey: col.Flags&1 == 1,
	}
}

// typeName resolves a type OID to its PostgreSQL name, falling back to the
// raw OID for types the client does not know (extensions, user-defined types).
func (d *Decoder) typeName(oid uint32) string {
	if dt, ok := d.typeMap.TypeForOID(oid); ok {
		return dt.Name
	}
	return fmt.Sprintf("oid:%d", oid)
}

// keyColumns returns the columns PostgreSQL marks as the replica identity.
func keyColumns(rel *pglogrepl.RelationMessageV2) []string {
	var keys []string
	for i := range rel.Columns {
		if rel.Columns[i].Flags&1 == 1 {
			keys = append(keys, rel.Columns[i].Name)
		}
	}
	return keys
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (d *Decoder) relation(relationID uint32) (*pglogrepl.RelationMessageV2, error) {
	rel, ok := d.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("no relation message received for relation ID %d", relationID)
	}
	return rel, nil
}

func (d *Decoder) newEvent(
	lsn pglogrepl.LSN,
	op domain.Operation,
	rel *pglogrepl.RelationMessageV2,
	oldData, newData map[string]any,
	unchangedToast []string,
) domain.CDCEvent {
	return domain.CDCEvent{
		ID:                    lsn.String(),
		Operation:             op,
		Schema:                rel.Namespace,
		Table:                 rel.RelationName,
		OldData:               oldData,
		NewData:               newData,
		UnchangedToastColumns: unchangedToast,
		Metadata: domain.EventMetadata{
			LSN:           lsn.String(),
			TransactionID: d.txn.xid,
			Timestamp:     d.txn.commitTime,
		},
	}
}

// decodeTuple converts a tuple into a column map. Columns whose value is an
// unchanged TOAST datum (not included in the WAL record) are omitted from the
// map and reported separately so sinks can avoid overwriting real values.
func (d *Decoder) decodeTuple(
	tuple *pglogrepl.TupleData,
	rel *pglogrepl.RelationMessageV2,
) (map[string]any, []string) {
	if tuple == nil {
		return nil, nil
	}

	values := make(map[string]any, len(tuple.Columns))
	var unchangedToast []string
	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			values[colName] = nil
		case pglogrepl.TupleDataTypeToast:
			unchangedToast = append(unchangedToast, colName)
		case pglogrepl.TupleDataTypeText:
			val, err := d.decodeTextColumn(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				values[colName] = string(col.Data)
			} else {
				values[colName] = val
			}
		}
	}

	return values, unchangedToast
}

func (d *Decoder) decodeTextColumn(data []byte, dataType uint32) (any, error) {
	return DecodeText(d.typeMap, dataType, data)
}

// DecodeText converts a column's Postgres text representation into a Go value
// suitable for JSON serialization. It is shared by the WAL decoder and the
// backfill chunk reader so streamed and backfilled rows use identical types.
func DecodeText(typeMap *pgtype.Map, dataType uint32, data []byte) (any, error) {
	// pgtype decodes some types into Go values that do not serialize to
	// JSON the way consumers expect (UUID becomes [16]byte, numeric becomes
	// a struct). Postgres's text representation is already exact and
	// readable for these, so pass it through untouched.
	switch dataType {
	case pgtype.UUIDOID, pgtype.NumericOID:
		return string(data), nil
	}
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
