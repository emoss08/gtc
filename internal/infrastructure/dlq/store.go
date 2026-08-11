package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/emoss08/gtc/internal/core/domain"
	"github.com/redis/go-redis/v9"
)

// ErrFull is returned when the DLQ has reached its entry cap. Parking then
// fails, which stalls the pipeline instead of silently dropping events.
var ErrFull = errors.New("dead-letter queue is full")

// ErrNotFound is returned for operations on a missing entry.
var ErrNotFound = errors.New("dead-letter entry not found")

// Store persists dead-letter entries. Abstracted so the manager and
// decorator are unit-testable without Redis.
type Store interface {
	// Park inserts or overwrites an entry (parking the same event again
	// after a replay is idempotent).
	Park(ctx context.Context, entry domain.DeadLetterEntry) error
	Get(ctx context.Context, id string) (*domain.DeadLetterEntry, error)
	// List returns entries newest-first, up to limit (<=0 means all).
	List(ctx context.Context, limit int) ([]domain.DeadLetterEntry, error)
	Remove(ctx context.Context, id string) error
	Len(ctx context.Context) (int64, error)
}

// redisStore keeps entries in a hash (id -> JSON) with a sorted-set index
// (id scored by last-failure time) for ordered listing and O(1) length.
type redisStore struct {
	client     *redis.Client
	hashKey    string
	indexKey   string
	maxEntries int64
}

func NewRedisStore(redisURL, prefix string, maxEntries int64) (Store, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("dlq: invalid redis URL: %w", err)
	}
	return &redisStore{
		client:     redis.NewClient(opts),
		hashKey:    prefix + ":dlq:entries",
		indexKey:   prefix + ":dlq:index",
		maxEntries: maxEntries,
	}, nil
}

func (s *redisStore) Park(ctx context.Context, entry domain.DeadLetterEntry) error {
	exists, err := s.client.HExists(ctx, s.hashKey, entry.ID).Result()
	if err != nil {
		return fmt.Errorf("dlq: check entry: %w", err)
	}
	if !exists && s.maxEntries > 0 {
		size, err := s.client.ZCard(ctx, s.indexKey).Result()
		if err != nil {
			return fmt.Errorf("dlq: size check: %w", err)
		}
		if size >= s.maxEntries {
			return fmt.Errorf("%w (%d entries)", ErrFull, size)
		}
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("dlq: marshal entry: %w", err)
	}

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, s.hashKey, entry.ID, data)
	pipe.ZAdd(ctx, s.indexKey, redis.Z{
		Score:  float64(entry.LastFailedAt.UnixMilli()),
		Member: entry.ID,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("dlq: park entry: %w", err)
	}
	return nil
}

func (s *redisStore) Get(ctx context.Context, id string) (*domain.DeadLetterEntry, error) {
	data, err := s.client.HGet(ctx, s.hashKey, id).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("dlq: get entry: %w", err)
	}

	var entry domain.DeadLetterEntry
	if err := json.Unmarshal([]byte(data), &entry); err != nil {
		return nil, fmt.Errorf("dlq: corrupt entry %s: %w", id, err)
	}
	return &entry, nil
}

func (s *redisStore) List(ctx context.Context, limit int) ([]domain.DeadLetterEntry, error) {
	stop := int64(-1)
	if limit > 0 {
		stop = int64(limit) - 1
	}
	ids, err := s.client.ZRevRange(ctx, s.indexKey, 0, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("dlq: list index: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	values, err := s.client.HMGet(ctx, s.hashKey, ids...).Result()
	if err != nil {
		return nil, fmt.Errorf("dlq: fetch entries: %w", err)
	}

	entries := make([]domain.DeadLetterEntry, 0, len(values))
	for _, v := range values {
		str, ok := v.(string)
		if !ok {
			continue // index/hash drift; skip rather than fail the listing
		}
		var entry domain.DeadLetterEntry
		if err := json.Unmarshal([]byte(str), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *redisStore) Remove(ctx context.Context, id string) error {
	pipe := s.client.TxPipeline()
	del := pipe.HDel(ctx, s.hashKey, id)
	pipe.ZRem(ctx, s.indexKey, id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("dlq: remove entry: %w", err)
	}
	if del.Val() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *redisStore) Len(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.indexKey).Result()
}
