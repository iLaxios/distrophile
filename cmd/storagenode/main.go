package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/common/logger"
	"github.com/iLaxios/distrophile/internal/storagenode"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func generateNodeID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("node-%x", b)
}

func getFreeSpace(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	// Available blocks * block size
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

func registerWithCoordinator(cfg *config.Config, log *zap.SugaredLogger, nodeID, nodeAddr string) error {
	if cfg.CoordinatorAddr == "" {
		log.Warn("No coordinator address configured, skipping registration")
		return nil
	}

	// Get free space
	freeBytes, err := getFreeSpace(cfg.DataDir)
	if err != nil {
		log.Warn("Failed to get free space, using 0", zap.Error(err))
		freeBytes = 0
	}

	// Connect to coordinator
	conn, err := grpc.NewClient(cfg.CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer conn.Close()

	client := proto.NewCoordinatorServiceClient(conn)

	// Retry registration with exponential backoff
	maxRetries := 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		resp, err := client.RegisterNode(ctx, &proto.RegisterNodeRequest{
			Info: &proto.NodeInfo{
				NodeId:        nodeID,
				Addr:          nodeAddr,
				State:         proto.NodeState_HEALTHY,
				FreeBytes:     freeBytes,
				LastHeartbeat: timestamppb.Now(),
			},
		})
		cancel()

		if err == nil && resp.Accepted {
			log.Infof("Successfully registered with coordinator: node_id=%s", resp.NodeId)
			return nil
		}

		if err != nil {
			log.Warnf("Registration attempt %d/%d failed: %v", attempt, maxRetries, err)
		} else {
			log.Warnf("Registration attempt %d/%d rejected: %s", attempt, maxRetries, resp.Error)
		}

		if attempt < maxRetries {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			log.Infof("Retrying in %v...", backoff)
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed to register after %d attempts", maxRetries)
}

func startHeartbeat(cfg *config.Config, log *zap.SugaredLogger, nodeID string) {
	if cfg.CoordinatorAddr == "" {
		return
	}

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			freeBytes, err := getFreeSpace(cfg.DataDir)
			if err != nil {
				log.Warn("Failed to get free space for heartbeat", zap.Error(err))
				freeBytes = 0
			}

			conn, err := grpc.NewClient(cfg.CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn("Failed to connect to coordinator for heartbeat", zap.Error(err))
				continue
			}

			client := proto.NewCoordinatorServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			_, err = client.Heartbeat(ctx, &proto.HeartbeatRequest{
				NodeId:    nodeID,
				FreeBytes: freeBytes,
				Now:       timestamppb.Now(),
			})

			cancel()
			conn.Close()

			if err != nil {
				log.Warn("Heartbeat failed", zap.Error(err))
			} else {
				log.Debugf("Heartbeat sent: node_id=%s free_bytes=%d", nodeID, freeBytes)
			}
		}
	}()
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

	// Get node address (use hostname in Docker)
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("Failed to get hostname", zap.Error(err))
	}
	nodeAddr := fmt.Sprintf("%s:%s", hostname, cfg.StorageNodePort)
	log.Info("Node address", "addr", nodeAddr)

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

	// Register with coordinator
	if err := registerWithCoordinator(cfg, log, nodeID, nodeAddr); err != nil {
		log.Fatal("Failed to register with coordinator", zap.Error(err))
	}

	// Start heartbeat goroutine
	startHeartbeat(cfg, log, nodeID)

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("Failed to serve gRPC server", zap.Error(err))
	}
}
