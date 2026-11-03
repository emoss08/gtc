package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

// WALBatchHandler receives events in stream order. A batch may span several
// transactions and may hold part of one; the reader confirms a position only
// after every event up to it was handled. An error tears the stream down and
// the unconfirmed events are redelivered.
type WALBatchHandler func(ctx context.Context, events []domain.CDCEvent) error

type WALReader interface {
	Start(ctx context.Context, handler WALBatchHandler) error
	Stop(ctx context.Context) error
	CurrentLSN() string
}
