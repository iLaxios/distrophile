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

func Connect(cfg *config.Config) *redis.Client {
	if Rdb != nil {
		return Rdb
	}

	Rdb = redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   0,
	})

	// Test connectivity
	if err := Rdb.Ping(ctx).Err(); err != nil {
		logger.Log().Error("Redis connection failed", zap.Error(err))
		return nil
	}

	logger.Log().Info("Redis connected", zap.String("addr", cfg.RedisAddr))
	return Rdb
}

// Optional helper to get the context
func Ctx() context.Context {
	return ctx
}
