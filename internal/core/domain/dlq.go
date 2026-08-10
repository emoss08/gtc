package domain

import (
	"encoding/json"
	"time"
)

// DeadLetterEntry is a poison event parked after repeatedly failing one
// sink. The full (already-transformed) event is preserved so it can be
// retried against that sink exactly as it would have been delivered.
type DeadLetterEntry struct {
	ID            string    `json:"id"` // "<sink>:<event_id>"
	SinkName      string    `json:"sink"`
	Schema        string    `json:"schema"`
	Table         string    `json:"table"`
	Operation     string    `json:"operation"`
	LSN           string    `json:"lsn"`
	Error         string    `json:"error"`
	Attempts      int       `json:"attempts"`
	FirstFailedAt time.Time `json:"first_failed_at"`
	LastFailedAt  time.Time `json:"last_failed_at"`
	Event         CDCEvent  `json:"event"`
}

// RetryResult summarizes a retry-all sweep.
type RetryResult struct {
	Retried   int `json:"retried"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}

// MarshalJSON/UnmarshalJSON round-trip Operation as its string form so DLQ
// entries are readable and survive serialization.
func (o Operation) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.String())
}

func (o *Operation) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "INSERT":
		*o = OperationInsert
	case "UPDATE":
		*o = OperationUpdate
	case "DELETE":
		*o = OperationDelete
	case "TRUNCATE":
		*o = OperationTruncate
	case "READ":
		*o = OperationRead
	default:
		*o = OperationInsert
	}
	return nil
}
