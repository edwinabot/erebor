package risk

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const haltHashKey = "erebor:live:halt"

// HaltStore persists and retrieves the halt flag for a session.
// Implementations must be safe for concurrent use.
type HaltStore interface {
	// SetHalted marks the given session as halted. Idempotent.
	SetHalted(ctx context.Context, sessionID string) error
	// IsHalted reports whether the given session was previously halted.
	IsHalted(ctx context.Context, sessionID string) (bool, error)
}

// RedisHaltStore persists halt flags in the Redis hash erebor:live:halt.
// Field name = sessionID, value = "1".
type RedisHaltStore struct {
	client *redis.Client
}

// NewRedisHaltStore returns a HaltStore backed by the given Redis client.
func NewRedisHaltStore(client *redis.Client) *RedisHaltStore {
	return &RedisHaltStore{client: client}
}

func (s *RedisHaltStore) SetHalted(ctx context.Context, sessionID string) error {
	if err := s.client.HSet(ctx, haltHashKey, sessionID, "1").Err(); err != nil {
		return fmt.Errorf("persist halt for session %s: %w", sessionID, err)
	}
	return nil
}

func (s *RedisHaltStore) IsHalted(ctx context.Context, sessionID string) (bool, error) {
	val, err := s.client.HGet(ctx, haltHashKey, sessionID).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check halt for session %s: %w", sessionID, err)
	}
	return val == "1", nil
}

// NoopHaltStore is a HaltStore that never persists and always reports not-halted.
// Use in tests and when no Redis is available.
type NoopHaltStore struct{}

func (NoopHaltStore) SetHalted(_ context.Context, _ string) error        { return nil }
func (NoopHaltStore) IsHalted(_ context.Context, _ string) (bool, error) { return false, nil }
