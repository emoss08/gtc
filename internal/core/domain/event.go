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
	Metadata  EventMetadata
}

func (e *CDCEvent) FullTableName() string {
	return fmt.Sprintf("%s.%s", e.Schema, e.Table)
}
