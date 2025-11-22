package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// Acquire a lock with TTL
func AcquireLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (bool, error) {
	ok, err := rdb.SetNX(ctx, key, 1, ttl).Result()
	return ok, err
}

// Release the lock
func ReleaseLock(ctx context.Context, rdb *redis.Client, key string) error {
	_, err := rdb.Del(ctx, key).Result()
	return err
}
