package main

import (
	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/common/logger"
	"go.uber.org/zap"
)

func main() {

	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	logger.Log().Info("Configuration: Replication Factor is ", cfg.ReplicationFactor)
	logger.Log().Info("loaded config", zap.Any("config", cfg))
}
