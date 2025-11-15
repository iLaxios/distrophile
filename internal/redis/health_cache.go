package redis

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/iLaxios/distrophile/proto"
)


// SetNodeHealth sets node health in Redis with TTL
func SetNodeHealth(rdb *redis.Client, nodeID string, state proto.NodeState, ttl time.Duration) error {
	key := fmt.Sprintf("node:%s:state", nodeID)
	return rdb.Set(ctx, key, state.String(), ttl).Err()
}

// GetNodeHealth fetches node health from Redis
func GetNodeHealth(rdb *redis.Client, nodeID string) (proto.NodeState, error) {
	key := fmt.Sprintf("node:%s:state", nodeID)
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return proto.NodeState_UNKNOWN, nil
		}
		return proto.NodeState_UNKNOWN, err
	}

	switch val {
	case "HEALTHY":
		return proto.NodeState_HEALTHY, nil
	case "UNHEALTHY":
		return proto.NodeState_UNHEALTHY, nil
	case "OFFLINE":
		return proto.NodeState_OFFLINE, nil
	default:
		return proto.NodeState_UNKNOWN, nil
	}
}
