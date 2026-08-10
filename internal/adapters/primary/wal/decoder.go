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
}

func NewDecoder() *Decoder {
	return &Decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:   pgtype.NewMap(),
	}
}

type DecodeResult struct {
	Events []domain.CDCEvent
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

	case *pglogrepl.CommitMessage:
		d.txn.inTxn = false
		// The whole transaction has been decoded and every event handed to
		// the handler synchronously, so its end position is safe to confirm.
		result.AckLSN = msg.TransactionEndLSN

	case *pglogrepl.RelationMessageV2:
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
