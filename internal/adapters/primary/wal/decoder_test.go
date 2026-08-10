package wal

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// pgEpoch is the PostgreSQL timestamp epoch (2000-01-01).
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func pgTime(t time.Time) uint64 {
	return uint64(t.Sub(pgEpoch).Microseconds())
}

func keepaliveMsg(serverWALEnd pglogrepl.LSN, replyRequested bool) *pgproto3.CopyData {
	buf := []byte{pglogrepl.PrimaryKeepaliveMessageByteID}
	buf = binary.BigEndian.AppendUint64(buf, uint64(serverWALEnd))
	buf = binary.BigEndian.AppendUint64(buf, pgTime(time.Now()))
	if replyRequested {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return &pgproto3.CopyData{Data: buf}
}

func xlogDataMsg(walStart pglogrepl.LSN, walData []byte) *pgproto3.CopyData {
	buf := []byte{pglogrepl.XLogDataByteID}
	buf = binary.BigEndian.AppendUint64(buf, uint64(walStart))
	buf = binary.BigEndian.AppendUint64(buf, uint64(walStart))
	buf = binary.BigEndian.AppendUint64(buf, pgTime(time.Now()))
	return &pgproto3.CopyData{Data: append(buf, walData...)}
}

func beginBody(finalLSN pglogrepl.LSN, commitTime time.Time, xid uint32) []byte {
	buf := []byte{'B'}
	buf = binary.BigEndian.AppendUint64(buf, uint64(finalLSN))
	buf = binary.BigEndian.AppendUint64(buf, pgTime(commitTime))
	buf = binary.BigEndian.AppendUint32(buf, xid)
	return buf
}

func commitBody(commitLSN, endLSN pglogrepl.LSN, commitTime time.Time) []byte {
	buf := []byte{'C', 0}
	buf = binary.BigEndian.AppendUint64(buf, uint64(commitLSN))
	buf = binary.BigEndian.AppendUint64(buf, uint64(endLSN))
	buf = binary.BigEndian.AppendUint64(buf, pgTime(commitTime))
	return buf
}

func relationBody(relID uint32, namespace, name string, cols []relCol) []byte {
	buf := []byte{'R'}
	buf = binary.BigEndian.AppendUint32(buf, relID)
	buf = append(buf, namespace...)
	buf = append(buf, 0)
	buf = append(buf, name...)
	buf = append(buf, 0)
	buf = append(buf, 'd') // replica identity default
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(cols)))
	for _, c := range cols {
		buf = append(buf, 0) // flags
		buf = append(buf, c.name...)
		buf = append(buf, 0)
		buf = binary.BigEndian.AppendUint32(buf, c.oid)
		buf = binary.BigEndian.AppendUint32(buf, 0xFFFFFFFF) // typmod -1
	}
	return buf
}

type relCol struct {
	name string
	oid  uint32
}

type tupleCol struct {
	kind byte // 'n', 'u', or 't'
	data string
}

func tupleBytes(cols []tupleCol) []byte {
	var buf []byte
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(cols)))
	for _, c := range cols {
		buf = append(buf, c.kind)
		if c.kind == 't' {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.data)))
			buf = append(buf, c.data...)
		}
	}
	return buf
}

func insertBody(relID uint32, cols []tupleCol) []byte {
	buf := []byte{'I'}
	buf = binary.BigEndian.AppendUint32(buf, relID)
	buf = append(buf, 'N')
	return append(buf, tupleBytes(cols)...)
}

func updateBody(relID uint32, cols []tupleCol) []byte {
	buf := []byte{'U'}
	buf = binary.BigEndian.AppendUint32(buf, relID)
	buf = append(buf, 'N')
	return append(buf, tupleBytes(cols)...)
}

func decodeAt(t *testing.T, d *Decoder, lsn pglogrepl.LSN, body []byte) *DecodeResult {
	t.Helper()
	result, err := d.Decode(xlogDataMsg(lsn, body))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	return result
}

func TestKeepaliveAckOnlyOutsideTransaction(t *testing.T) {
	d := NewDecoder()

	// Outside a transaction the server's WAL end is safe to acknowledge.
	result, err := d.Decode(keepaliveMsg(100, true))
	if err != nil {
		t.Fatalf("decode keepalive: %v", err)
	}
	if result.AckLSN != 100 {
		t.Errorf("expected AckLSN 100 outside txn, got %d", result.AckLSN)
	}
	if !result.RequestReply {
		t.Error("expected RequestReply to be set")
	}

	// Inside a transaction it must not be acknowledged.
	decodeAt(t, d, 200, beginBody(300, time.Now(), 42))
	result, err = d.Decode(keepaliveMsg(250, false))
	if err != nil {
		t.Fatalf("decode keepalive: %v", err)
	}
	if result.AckLSN != 0 {
		t.Errorf("expected no AckLSN inside txn, got %d", result.AckLSN)
	}

	// Commit acknowledges the transaction end LSN and reopens keepalive acks.
	result = decodeAt(t, d, 300, commitBody(300, 310, time.Now()))
	if result.AckLSN != 310 {
		t.Errorf("expected AckLSN 310 from commit, got %d", result.AckLSN)
	}
	result, err = d.Decode(keepaliveMsg(400, false))
	if err != nil {
		t.Fatalf("decode keepalive: %v", err)
	}
	if result.AckLSN != 400 {
		t.Errorf("expected AckLSN 400 after commit, got %d", result.AckLSN)
	}
}

func TestInsertEventCarriesTransactionMetadata(t *testing.T) {
	d := NewDecoder()
	commitTime := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	decodeAt(t, d, 10, relationBody(1, "public", "users", []relCol{
		{"id", pgtype.Int8OID},
		{"name", pgtype.TextOID},
	}))
	decodeAt(t, d, 20, beginBody(50, commitTime, 777))
	result := decodeAt(t, d, 30, insertBody(1, []tupleCol{
		{'t', "1"},
		{'t', "alice"},
	}))

	if len(result.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(result.Events))
	}
	event := result.Events[0]
	if event.Operation != domain.OperationInsert {
		t.Errorf("expected insert, got %s", event.Operation)
	}
	if event.Schema != "public" || event.Table != "users" {
		t.Errorf("unexpected table: %s.%s", event.Schema, event.Table)
	}
	if event.Metadata.TransactionID != 777 {
		t.Errorf("expected xid 777, got %d", event.Metadata.TransactionID)
	}
	if !event.Metadata.Timestamp.Equal(commitTime) {
		t.Errorf("expected commit time %v, got %v", commitTime, event.Metadata.Timestamp)
	}
	if event.NewData["id"] != int64(1) || event.NewData["name"] != "alice" {
		t.Errorf("unexpected data: %#v", event.NewData)
	}
	if result.AckLSN != 0 {
		t.Errorf("DML must not produce an AckLSN, got %d", result.AckLSN)
	}
}

func TestUpdateOmitsUnchangedToastColumns(t *testing.T) {
	d := NewDecoder()

	decodeAt(t, d, 10, relationBody(1, "public", "docs", []relCol{
		{"id", pgtype.Int8OID},
		{"body", pgtype.TextOID},
		{"note", pgtype.TextOID},
	}))
	decodeAt(t, d, 20, beginBody(50, time.Now(), 1))
	result := decodeAt(t, d, 30, updateBody(1, []tupleCol{
		{'t', "7"},
		{'u', ""}, // unchanged TOAST value, not present in WAL
		{'n', ""},
	}))

	if len(result.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(result.Events))
	}
	event := result.Events[0]
	if _, present := event.NewData["body"]; present {
		t.Error("unchanged toast column must be omitted from NewData")
	}
	if len(event.UnchangedToastColumns) != 1 || event.UnchangedToastColumns[0] != "body" {
		t.Errorf("expected [body], got %v", event.UnchangedToastColumns)
	}
	if val, present := event.NewData["note"]; !present || val != nil {
		t.Errorf("null column must be present as nil, got %#v", event.NewData)
	}
	if event.NewData["id"] != int64(7) {
		t.Errorf("expected id 7, got %#v", event.NewData["id"])
	}
}

func TestUnknownRelationIsAnError(t *testing.T) {
	d := NewDecoder()
	decodeAt(t, d, 10, beginBody(50, time.Now(), 1))

	_, err := d.Decode(xlogDataMsg(20, insertBody(99, []tupleCol{{'t', "1"}})))
	if err == nil {
		t.Fatal("expected error for unknown relation, got nil")
	}
}

func TestDecodeTextColumnJSONFriendlyTypes(t *testing.T) {
	d := NewDecoder()

	uuid, err := d.decodeTextColumn([]byte("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), pgtype.UUIDOID)
	if err != nil {
		t.Fatalf("uuid decode: %v", err)
	}
	if uuid != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" {
		t.Errorf("uuid must stay a string, got %#v", uuid)
	}

	num, err := d.decodeTextColumn([]byte("12345.6789"), pgtype.NumericOID)
	if err != nil {
		t.Fatalf("numeric decode: %v", err)
	}
	if num != "12345.6789" {
		t.Errorf("numeric must stay a string, got %#v", num)
	}

	i, err := d.decodeTextColumn([]byte("42"), pgtype.Int4OID)
	if err != nil {
		t.Fatalf("int decode: %v", err)
	}
	if i != int32(42) {
		t.Errorf("expected int32(42), got %#v", i)
	}
}

func TestNonCopyDataMessagesAreIgnored(t *testing.T) {
	d := NewDecoder()
	result, err := d.Decode(&pgproto3.NoticeResponse{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Events) != 0 || result.AckLSN != 0 {
		t.Errorf("expected empty result, got %+v", result)
	}
}

func TestLogicalMessageDecodesToWatermark(t *testing.T) {
	d := NewDecoder()

	// Message: 'M' + flags(1) + lsn(8) + prefix cstring + content len(4) + content.
	content := []byte(`{"slot":"s","chunk":"c1","mark":"low"}`)
	body := []byte{'M', 0}
	body = binary.BigEndian.AppendUint64(body, 500)
	body = append(body, WatermarkPrefix...)
	body = append(body, 0)
	body = binary.BigEndian.AppendUint32(body, uint32(len(content)))
	body = append(body, content...)

	result := decodeAt(t, d, 500, body)
	if result.Message == nil {
		t.Fatal("expected a logical message")
	}
	if result.Message.Prefix != WatermarkPrefix {
		t.Errorf("expected prefix %q, got %q", WatermarkPrefix, result.Message.Prefix)
	}
	if string(result.Message.Content) != string(content) {
		t.Errorf("content mismatch: %s", result.Message.Content)
	}
	if len(result.Events) != 0 || result.AckLSN != 0 {
		t.Errorf("logical message must not produce events or acks: %+v", result)
	}
}

func TestStreamedMessagesAreRejected(t *testing.T) {
	d := NewDecoder()
	// Stream Start: 'S' + xid + first-segment flag.
	body := []byte{'S'}
	body = binary.BigEndian.AppendUint32(body, 5)
	body = append(body, 1)

	if _, err := d.Decode(xlogDataMsg(10, body)); err == nil {
		t.Fatal("expected error for streamed-transaction message")
	}
}
