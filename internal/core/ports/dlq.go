package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

// DLQManager is the operational surface of the dead-letter queue, used by
// the HTTP API to triage parked events.
type DLQManager interface {
	List(ctx context.Context, limit int) ([]domain.DeadLetterEntry, error)
	Len(ctx context.Context) (int64, error)
	// Retry re-delivers a parked event to its sink; on success the entry
	// is removed, on failure it is updated in place.
	Retry(ctx context.Context, id string) error
	RetryAll(ctx context.Context) (domain.RetryResult, error)
	Discard(ctx context.Context, id string) error
}
