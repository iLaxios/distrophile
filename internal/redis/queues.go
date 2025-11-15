package redis

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)


// Generic push to a queue
func PushJob(rdb *redis.Client, queueName string, job interface{}) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return rdb.LPush(ctx, queueName, data).Err()
}

// Pop job from a queue (blocking optional)
func PopJob(rdb *redis.Client, queueName string, timeout time.Duration, dest interface{}) (bool, error) {
	val, err := rdb.BRPop(ctx, timeout, queueName).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	if len(val) < 2 {
		return false, fmt.Errorf("invalid BRPop result: %v", val)
	}

	return true, json.Unmarshal([]byte(val[1]), dest)
}
