package main

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"

	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/common/logger"
	"github.com/iLaxios/distrophile/internal/storagenode"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func generateNodeID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("node-%x", b)
}

func main() {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		panic(fmt.Errorf("failed to load config: %w", err))
	}

	// Init logger
	log := logger.Log()
	log.Info("Loaded configuration", zap.Any("config", cfg))

	// Generate or get node ID
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = generateNodeID()
		log.Info("Generated node ID", "node_id", nodeID)
	} else {
		log.Info("Using node ID from environment", "node_id", nodeID)
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatal("Failed to create data directory", zap.Error(err))
	}
	log.Info("Data directory ready", "data_dir", cfg.DataDir)

	// Start gRPC server
	addr := fmt.Sprintf(":%s", cfg.StorageNodePort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Failed to listen", zap.Error(err))
	}
	log.Info("Storage Node gRPC listening", "address", addr, "node_id", nodeID)

	grpcServer := grpc.NewServer()

	// Initialize Storage Node service
	storageNode := storagenode.NewStorageNode(cfg, log, nodeID)

	// Register gRPC service
	proto.RegisterStorageNodeServiceServer(grpcServer, storageNode)

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("Failed to serve gRPC server", zap.Error(err))
	}
}
