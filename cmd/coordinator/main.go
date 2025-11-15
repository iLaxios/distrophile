package main

import (
	"fmt"
	"net"

	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/common/logger"
	"github.com/iLaxios/distrophile/internal/coordinator"
	"github.com/iLaxios/distrophile/internal/mongodb"
	"github.com/iLaxios/distrophile/internal/redis"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		panic(fmt.Errorf("failed to load config: %w", err))
	}

	// Init logger
	log := logger.Log()
	log.Info("Loaded configuration", zap.Any("config", cfg))

	// Connect MongoDB
	mongoClient, err := mongodb.Connect(cfg)
	if err != nil {
		log.Fatal("MongoDB connection failed", zap.Error(err))
	}

	// Connect Redis
	rdb, err := redis.Connect(cfg)
	if err != nil {
		log.Fatal("Redis connection failed", zap.Error(err))
	}

	// Start gRPC server
	addr := fmt.Sprintf(":%s", cfg.CoordinatorPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Failed to listen", zap.Error(err))
	}
	log.Info("Coordinator gRPC listening", "address", addr)

	grpcServer := grpc.NewServer()

	// Initialize Coordinator service using constructor
	coord := coordinator.NewCoordinator(cfg, log, mongoClient, rdb)

	// Register gRPC services
	proto.RegisterCoordinatorServiceServer(grpcServer, coord)
	proto.RegisterCoordinatorAdminServer(grpcServer, coord)

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("Failed to serve gRPC server", zap.Error(err))
	}
}
