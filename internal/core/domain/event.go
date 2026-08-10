package domain

import (
	"fmt"
	"time"
)

type Operation int

const (
	OperationInsert Operation = iota
	OperationUpdate
	OperationDelete
	OperationTruncate
	// OperationRead is a row emitted by a backfill (initial snapshot or
	// replay), not by a live change. Sinks treat it like an insert of the
	// row's current state.
	OperationRead
)

func (o Operation) String() string {
	switch o {
	case OperationInsert:
		return "INSERT"
	case OperationUpdate:
		return "UPDATE"
	case OperationDelete:
		return "DELETE"
	case OperationTruncate:
		return "TRUNCATE"
	case OperationRead:
		return "READ"
	default:
		return "UNKNOWN"
	}
}

type EventMetadata struct {
	LSN           string
	TransactionID uint32
	Timestamp     time.Time
}

type CDCEvent struct {
	ID        string
	Operation Operation
	Schema    string
	Table     string
	OldData   map[string]any
	NewData   map[string]any
	// UnchangedToastColumns lists columns omitted from NewData because
	// their TOASTed value was unchanged and not included in the WAL record.
	// Sinks that replace whole documents must not treat the omission as a
	// deletion of those fields.
	UnchangedToastColumns []string
	Metadata              EventMetadata
}

func (e *CDCEvent) FullTableName() string {
	return fmt.Sprintf("%s.%s", e.Schema, e.Table)
}
