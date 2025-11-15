package redis

import (
	"time"

	"github.com/go-redis/redis/v8"
)

// Acquire a lock with TTL
func AcquireLock(rdb *redis.Client, key string, ttl time.Duration) (bool, error) {
	ok, err := rdb.SetNX(ctx, key, 1, ttl).Result()
	return ok, err
}

// Release the lock
func ReleaseLock(rdb *redis.Client, key string) error {
	_, err := rdb.Del(ctx, key).Result()
	return err
}
