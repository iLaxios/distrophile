package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const NodeHeartbeatKeyPrefix = "node:hb:"

func SetNodeHeartbeat(ctx context.Context, rdb *redis.Client, nodeID string, ttl time.Duration) error {
	key := fmt.Sprintf("%s%s", NodeHeartbeatKeyPrefix, nodeID)
	// value can be timestamp
	return rdb.Set(ctx, key, time.Now().Unix(), ttl).Err()
}

func IsNodeAlive(ctx context.Context, rdb *redis.Client, nodeID string) (bool, error) {
	key := fmt.Sprintf("%s%s", NodeHeartbeatKeyPrefix, nodeID)
	res, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func ListAliveNodes(ctx context.Context, rdb *redis.Client, pattern string) ([]string, error) {
	// optional: use SCAN; for small-scale testing you can keep known nodes elsewhere
	var out []string
	iter := rdb.Scan(ctx, 0, NodeHeartbeatKeyPrefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		// strip prefix
		out = append(out, k[len(NodeHeartbeatKeyPrefix):])
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
