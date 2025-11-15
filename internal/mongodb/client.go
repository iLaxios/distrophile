package mongodb

import (
	"context"
	"time"

	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/common/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

)

var client *mongo.Client


func Connect(cfg *config.Config) (*mongo.Client, error) {
    if client != nil {
        return client, nil
    }

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    c, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
    if err != nil {
        return nil, err
    }

    if err := c.Ping(ctx, nil); err != nil {
        return nil, err
    }

    client = c
    logger.Log().Info("MongoDB connected", zap.String("uri", cfg.MongoURI))
    return client, nil
}
