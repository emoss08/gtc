package redisjson

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/emoss08/gtc/internal/adapters/secondary/redis"
	"github.com/emoss08/gtc/internal/core/domain"
)

type JSONSink struct {
	*redis.BaseSink
}

var _ domain.Sink = (*JSONSink)(nil)

type JSONSinkParams struct {
	Config      Config
	KeyResolver *redis.KeyResolver
	Logger      *slog.Logger
}

func NewSink(p JSONSinkParams) (*JSONSink, error) {
	base, err := redis.NewBaseSink(redis.BaseSinkParams{
		Name: "redis-json",
		Config: redis.BaseConfig{
			URL:         p.Config.URL,
			Prefix:      p.Config.Prefix,
			KeyResolver: p.KeyResolver,
		},
		Logger: p.Logger,
	})
	if err != nil {
		return nil, err
	}

	return &JSONSink{
		BaseSink: base,
	}, nil
}

// Process writes the batch in one pipelined round trip. Commands run in
// stream order on one connection, so a row written then deleted in the same
// batch ends up deleted.
func (s *JSONSink) Process(ctx context.Context, events []domain.CDCEvent) error {
	pipe := s.Client.Pipeline()
	queued := 0

	for i := range events {
		event := &events[i]
		if !s.ShouldProcess(*event) {
			continue
		}

		switch event.Operation {
		case domain.OperationInsert, domain.OperationUpdate, domain.OperationRead:
			if event.NewData == nil {
				continue
			}
			key, err := s.GenerateKey(*event)
			if err != nil {
				return domain.Permanent(err)
			}
			jsonData, err := sonic.Marshal(event.NewData)
			if err != nil {
				return domain.Permanent(fmt.Errorf("marshal data for event %s: %w", event.ID, err))
			}
			// When an UPDATE omits unchanged TOAST columns, a full JSON.SET
			// would erase their previously stored values, so merge into the
			// existing document instead. JSON.MERGE (RFC 7386) deletes fields
			// whose incoming value is null, which matches a column set to NULL.
			command := "JSON.SET"
			if event.Operation == domain.OperationUpdate && len(event.UnchangedToastColumns) > 0 {
				command = "JSON.MERGE"
			}
			pipe.Do(ctx, command, key, "$", jsonData)
			queued++

		case domain.OperationDelete:
			key, err := s.GenerateKey(*event)
			if err != nil {
				return domain.Permanent(err)
			}
			pipe.Del(ctx, key)
			queued++

		case domain.OperationTruncate:
			s.Logger.Warn("truncate operation not supported for redis json sink",
				slog.String("table", event.FullTableName()),
			)
		}
	}

	if queued == 0 {
		return nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("write %d json commands: %w", queued, err)
	}
	return nil
}
