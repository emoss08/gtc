package domain

import "time"

type BackfillState string

const (
	BackfillPending BackfillState = "pending"
	BackfillRunning BackfillState = "running"
	BackfillDone    BackfillState = "done"
	BackfillSkipped BackfillState = "skipped"
	BackfillFailed  BackfillState = "failed"
)

type BackfillTableStatus struct {
	Schema      string        `json:"schema"`
	Table       string        `json:"table"`
	State       BackfillState `json:"state"`
	RowsCopied  int64         `json:"rows_copied"`
	StartedAt   *time.Time    `json:"started_at,omitempty"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
	Error       string        `json:"error,omitempty"`
}
