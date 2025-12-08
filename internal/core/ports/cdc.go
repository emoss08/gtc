package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

type CDCService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	ProcessEvent(ctx context.Context, event domain.CDCEvent) []domain.SinkResult
}

type DeadLetterQueue interface {
	Push(ctx context.Context, entry domain.DeadLetterEntry) error
	Pop(ctx context.Context) (*domain.DeadLetterEntry, error)
	Peek(ctx context.Context, limit int) ([]domain.DeadLetterEntry, error)
	Len(ctx context.Context) (int64, error)
	Remove(ctx context.Context, eventID string, sinkName string) error
}
