package ports

import (
	"context"

	"github.com/emoss08/gtc/internal/core/domain"
)

type WALEventHandler func(ctx context.Context, event domain.CDCEvent) error

type WALReader interface {
	Start(ctx context.Context, handler WALEventHandler) error
	Stop(ctx context.Context) error
	CurrentLSN() string
}
