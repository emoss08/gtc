package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

// SchemaObserver is notified when a published table's shape changes.
//
// Observers are advisory: unlike row events, a failing observer must not tear
// down replication, because the change cannot be re-derived on redelivery (a
// reconnect re-sends the current relation description with nothing to diff it
// against). Implementations report their own failures via metrics and logs.
type SchemaObserver interface {
	OnSchemaChange(ctx context.Context, change domain.SchemaChange)
}
