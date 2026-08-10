package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

// BackfillController is the WAL reader's view of an in-flight backfill. The
// reader drives it from the streaming goroutine so chunk emission interleaves
// with live events at well-defined stream positions (the DBLog watermark
// algorithm).
type BackfillController interface {
	// ObserveEvent is called for every streamed DML event before it is
	// handed to sinks, so rows superseded by the live stream can be
	// dropped from the pending chunk.
	ObserveEvent(event domain.CDCEvent)
	// HandleWatermark is called when a backfill watermark message arrives
	// in the WAL stream. It returns the backfill events (if any) that must
	// be delivered to sinks at this position.
	HandleWatermark(lsn string, content []byte) []domain.CDCEvent
	// WatermarkDelivered is called after every event returned by
	// HandleWatermark was successfully processed by all sinks.
	WatermarkDelivered(content []byte)
	// OnStreamRestart is called when the replication stream is torn down;
	// any in-flight chunk must be retried with fresh watermarks.
	OnStreamRestart()
}

// BackfillManager is the operational surface for triggering and inspecting
// backfills (used by the HTTP API and startup wiring).
type BackfillManager interface {
	// EnqueueTable schedules one table for backfill, resetting any prior
	// progress (this is also the replay primitive).
	EnqueueTable(schema, table string) error
	// EnqueueAll schedules every table in the publication that has not
	// already completed a backfill.
	EnqueueAll(ctx context.Context) error
	Status() []domain.BackfillTableStatus
}
