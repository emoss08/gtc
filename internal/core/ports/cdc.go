package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

type CDCService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	ProcessBatch(ctx context.Context, events []domain.CDCEvent) []domain.SinkResult
	StallReporter
}

// StallReporter lists the sinks the pipeline is currently waiting for, in
// sink order. Readiness is false while any is stalled.
type StallReporter interface {
	Stalls() []domain.SinkStall
}
