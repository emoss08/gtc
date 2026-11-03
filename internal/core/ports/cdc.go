package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

type CDCService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	ProcessBatch(ctx context.Context, events []domain.CDCEvent) []domain.SinkResult
}
