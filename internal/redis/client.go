package redis

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/common/logger"
	"go.uber.org/zap"
)

var Rdb *redis.Client
var ctx = context.Background()

func Connect(cfg *config.Config) (*redis.Client, error) {
	if Rdb != nil {
		return Rdb, nil
	}

	Rdb = redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   0,
	})

	// Test connectivity
	if err := Rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	logger.Log().Info("Redis connected", zap.String("addr", cfg.RedisAddr))
	return Rdb, nil
}

// Optional helper to get the context
func Ctx() context.Context {
	return ctx
}
