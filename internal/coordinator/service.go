package coordinator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"

	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/mongodb"
	rd "github.com/iLaxios/distrophile/internal/redis"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Coordinator implements proto.CoordinatorServiceServer & proto.CoordinatorAdminServer
type Coordinator struct {
	Cfg          *config.Config
	Log          *zap.SugaredLogger
	Mongo        *mongo.Client
	Redis        *redis.Client
	MetadataRepo *mongodb.MetadataRepo
	NodeRepo     *mongodb.NodeRepo

	proto.UnimplementedCoordinatorServiceServer
	proto.UnimplementedCoordinatorAdminServer
}

// Constructor for Coordinator
func NewCoordinator(cfg *config.Config, log *zap.SugaredLogger, mongo *mongo.Client, rdb *redis.Client) *Coordinator {

	FileMataDataRepo := mongodb.NewMetadataRepo(mongo, cfg.MongoDB)
	NodeRepo := mongodb.NewNodeRepo(mongo, cfg.MongoDB)

	return &Coordinator{
		Cfg:          cfg,
		Log:          log,
		Mongo:        mongo,
		Redis:        rdb,
		MetadataRepo: FileMataDataRepo,
		NodeRepo:     NodeRepo,
	}
}

// ----------------------------
// CLI <-> Coordinator RPCs
// ----------------------------

func (c *Coordinator) Upload(stream proto.CoordinatorService_UploadServer) error {
	ctx := stream.Context()

	// Receive first message (metadata)
	req, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("client closed stream before sending metadata")
		}
		c.Log.Error("Failed to receive upload metadata", zap.Error(err))
		return err
	}

	// Extract metadata from first message
	meta := req.GetMeta()
	if meta == nil {
		return fmt.Errorf("first message must contain metadata")
	}

	// Generate file_id if not provided
	fileID := meta.FileId
	if fileID == "" {
		fileID = generateUUID()
		c.Log.Info("Generated file_id", "file_id", fileID)
	}

	filename := meta.Filename
	totalSize := meta.SizeBytes
	chunkSize := meta.ChunkSize
	if chunkSize == 0 {
		chunkSize = 2 * 1024 * 1024 // Default 2MB chunks
	}

	// Acquire file-level lock to prevent concurrent uploads of the same file_id
	lockKey := fmt.Sprintf("upload:lock:%s", fileID)
	lockTTL := 30 * time.Minute // Allow up to 30 minutes for upload

	if c.Redis != nil {
		acquired, err := rd.AcquireLock(ctx, c.Redis, lockKey, lockTTL)
		if err != nil {
			c.Log.Error("Failed to acquire upload lock",
				zap.String("file_id", fileID),
				zap.Error(err))
			return c.sendError(stream, fmt.Sprintf("failed to acquire upload lock: %v", err))
		}
		if !acquired {
			c.Log.Warn("Upload already in progress", "file_id", fileID)
			return c.sendError(stream, "upload already in progress for this file_id")
		}

		// Ensure lock is released when function exits
		defer func() {
			if err := rd.ReleaseLock(ctx, c.Redis, lockKey); err != nil {
				c.Log.Warn("Failed to release upload lock",
					zap.String("file_id", fileID),
					zap.Error(err))
			}
		}()
	}

	c.Log.Infof("Starting upload: file_id=%s filename=%s size=%d chunk_size=%d",
		fileID, filename, totalSize, chunkSize)

	// Track chunks as they're uploaded
	chunks := make([]*proto.ChunkMeta, 0)
	var receivedBytes int64

	// Process chunks
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break // Client finished sending chunks
			}
			c.Log.Error("Error receiving chunk", zap.Error(err))
			return c.sendError(stream, fmt.Sprintf("failed to receive chunk: %v", err))
		}

		chunk := req.GetChunk()
		if chunk == nil {
			return c.sendError(stream, "expected chunk payload")
		}

		// Generate chunk_id if not provided
		chunkID := chunk.ChunkId
		if chunkID == "" {
			chunkID = generateUUID()
		}

		// Calculate checksum if not provided
		checksum := chunk.Checksum
		if checksum == "" && len(chunk.Data) > 0 {
			hash := sha256.Sum256(chunk.Data)
			checksum = hex.EncodeToString(hash[:])
		}

		// Pick a node to store this chunk
		node, err := c.pickNode(ctx)
		if err != nil {
			c.Log.Error("Failed to pick node", zap.Error(err))
			return c.sendError(stream, fmt.Sprintf("no available nodes: %v", err))
		}

		// Connect to storage node and send chunk
		chunkResult, err := c.storeChunkOnNode(ctx, node, fileID, chunkID, chunk, checksum)
		if err != nil {
			c.Log.Error("Failed to store chunk on node",
				zap.String("chunk_id", chunkID),
				zap.String("node_id", node.NodeId),
				zap.Error(err))

			// Send error result for this chunk
			if err := stream.Send(&proto.UploadResponse{
				Payload: &proto.UploadResponse_ChunkResult{
					ChunkResult: &proto.ChunkUploadResult{
						ChunkId: chunkID,
						NodeId:  node.NodeId,
						Success: false,
						Error:   err.Error(),
					},
				},
			}); err != nil {
				return err
			}
			continue
		}

		// Record chunk metadata
		chunks = append(chunks, &proto.ChunkMeta{
			ChunkId:   chunkID,
			Index:     chunk.Index,
			SizeBytes: chunk.SizeBytes,
			Checksum:  checksum,
			NodeIds:   []string{node.NodeId},
		})

		receivedBytes += chunk.SizeBytes

		// Send chunk result back to client
		if err := stream.Send(&proto.UploadResponse{
			Payload: &proto.UploadResponse_ChunkResult{
				ChunkResult: chunkResult,
			},
		}); err != nil {
			return err
		}
	}

	// Verify we received all expected bytes
	if receivedBytes != totalSize {
		c.Log.Warnf("Size mismatch: expected=%d received=%d", totalSize, receivedBytes)
	}

	// Create file metadata
	now := timestamppb.Now()
	fileMetadata := &proto.FileMetadata{
		FileId:    fileID,
		Filename:  filename,
		SizeBytes: totalSize,
		Chunks:    chunks,
		CreatedAt: now,
		UpdatedAt: now,
	}

	// Save to MongoDB
	if err := c.MetadataRepo.SaveFile(fileMetadata); err != nil {
		c.Log.Error("Failed to save file metadata", zap.Error(err))
		return c.sendError(stream, fmt.Sprintf("failed to save metadata: %v", err))
	}

	c.Log.Infof("Upload completed: file_id=%s chunks=%d", fileID, len(chunks))

	// Send final summary
	return stream.Send(&proto.UploadResponse{
		Payload: &proto.UploadResponse_Summary{
			Summary: &proto.FileUploadSummary{
				FileId:   fileID,
				Metadata: fileMetadata,
			},
		},
	})
}

// storeChunkOnNode connects to a storage node and stores a chunk
func (c *Coordinator) storeChunkOnNode(ctx context.Context, node *proto.NodeInfo, fileID, chunkID string, chunk *proto.ChunkPayload, checksum string) (*proto.ChunkUploadResult, error) {
	// Create gRPC connection to storage node
	conn, err := grpc.NewClient(
		node.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s: %w", node.Addr, err)
	}
	defer conn.Close()

	// Create storage node client
	client := proto.NewStorageNodeServiceClient(conn)

	// Create bidirectional stream
	storeStream, err := client.StoreChunk(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create StoreChunk stream: %w", err)
	}

	// Prepare chunk with generated ID and checksum
	chunkPayload := &proto.ChunkPayload{
		ChunkId:   chunkID,
		Index:     chunk.Index,
		Data:      chunk.Data,
		SizeBytes: chunk.SizeBytes,
		Checksum:  checksum,
	}

	// Send chunk to node
	storeReq := &proto.StoreChunkRequest{
		Chunk:    chunkPayload,
		FileId:   fileID,
		Uploader: "coordinator", // Could be client ID in future
	}

	if err := storeStream.Send(storeReq); err != nil {
		return nil, fmt.Errorf("failed to send chunk to node: %w", err)
	}

	// Receive acknowledgment
	ack, err := storeStream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive ack from node: %w", err)
	}

	// Close the stream
	if err := storeStream.CloseSend(); err != nil {
		c.Log.Warn("Failed to close StoreChunk stream", zap.Error(err))
	}

	if !ack.Ok {
		return nil, fmt.Errorf("node rejected chunk: %s", ack.Error)
	}

	return &proto.ChunkUploadResult{
		ChunkId:  chunkID,
		NodeId:   node.NodeId,
		Success:  true,
		Checksum: ack.Checksum,
	}, nil
}

// sendError sends an error response to the client
func (c *Coordinator) sendError(stream proto.CoordinatorService_UploadServer, message string) error {
	return stream.Send(&proto.UploadResponse{
		Payload: &proto.UploadResponse_Error{
			Error: &proto.ErrorErr{
				Message: message,
				Code:    1,
			},
		},
	})
}

// generateUUID generates a simple UUID v4-like string
func generateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (c *Coordinator) Download(req *proto.DownloadRequest, stream proto.CoordinatorService_DownloadServer) error {
	ctx := stream.Context()
	fileID := req.FileId

	if fileID == "" {
		return stream.Send(&proto.DownloadResponse{
			Payload: &proto.DownloadResponse_Error{
				Error: &proto.ErrorErr{
					Message: "file_id is required",
					Code:    1,
				},
			},
		})
	}

	c.Log.Infof("Download requested: file_id=%s", fileID)

	// Load file metadata from MongoDB
	fileMetadata, err := c.MetadataRepo.GetFile(fileID)
	if err != nil {
		c.Log.Error("Failed to get file metadata",
			zap.String("file_id", fileID),
			zap.Error(err))
		return stream.Send(&proto.DownloadResponse{
			Payload: &proto.DownloadResponse_Error{
				Error: &proto.ErrorErr{
					Message: fmt.Sprintf("file not found: %s", fileID),
					Code:    404,
				},
			},
		})
	}

	// Optionally send metadata first
	if err := stream.Send(&proto.DownloadResponse{
		Payload: &proto.DownloadResponse_Metadata{
			Metadata: fileMetadata,
		},
	}); err != nil {
		return err
	}

	// Get all nodes for lookup
	nodes, err := c.NodeRepo.ListNodes(ctx)
	if err != nil {
		c.Log.Error("Failed to list nodes", zap.Error(err))
		return stream.Send(&proto.DownloadResponse{
			Payload: &proto.DownloadResponse_Error{
				Error: &proto.ErrorErr{
					Message: "failed to list nodes",
					Code:    500,
				},
			},
		})
	}

	// Create node lookup map
	nodeMap := make(map[string]*proto.NodeInfo)
	for _, node := range nodes {
		nodeMap[node.NodeId] = node
	}

	// Download chunks in order
	for _, chunkMeta := range fileMetadata.Chunks {
		// Find a node that has this chunk
		var selectedNode *proto.NodeInfo
		for _, nodeID := range chunkMeta.NodeIds {
			if node, exists := nodeMap[nodeID]; exists && node.State == proto.NodeState_HEALTHY {
				selectedNode = node
				break
			}
		}

		if selectedNode == nil {
			c.Log.Error("No healthy node found for chunk",
				zap.String("chunk_id", chunkMeta.ChunkId),
				zap.Strings("node_ids", chunkMeta.NodeIds))

			if err := stream.Send(&proto.DownloadResponse{
				Payload: &proto.DownloadResponse_Error{
					Error: &proto.ErrorErr{
						Message: fmt.Sprintf("no healthy node available for chunk %s", chunkMeta.ChunkId),
						Code:    503,
					},
				},
			}); err != nil {
				return err
			}
			continue
		}

		// Fetch chunk from storage node
		chunk, err := c.getChunkFromNode(ctx, selectedNode, chunkMeta.ChunkId)
		if err != nil {
			c.Log.Error("Failed to get chunk from node",
				zap.String("chunk_id", chunkMeta.ChunkId),
				zap.String("node_id", selectedNode.NodeId),
				zap.Error(err))

			if err := stream.Send(&proto.DownloadResponse{
				Payload: &proto.DownloadResponse_Error{
					Error: &proto.ErrorErr{
						Message: fmt.Sprintf("failed to get chunk %s: %v", chunkMeta.ChunkId, err),
						Code:    500,
					},
				},
			}); err != nil {
				return err
			}
			continue
		}

		// Verify checksum if available
		if chunkMeta.Checksum != "" && chunk.Checksum != "" {
			if chunkMeta.Checksum != chunk.Checksum {
				c.Log.Warn("Checksum mismatch for chunk",
					zap.String("chunk_id", chunkMeta.ChunkId),
					zap.String("expected", chunkMeta.Checksum),
					zap.String("got", chunk.Checksum))
				// Continue anyway, but log the warning
			}
		}

		// Set chunk index from metadata
		chunk.Index = chunkMeta.Index

		// Stream chunk to client
		if err := stream.Send(&proto.DownloadResponse{
			Payload: &proto.DownloadResponse_Chunk{
				Chunk: chunk,
			},
		}); err != nil {
			return err
		}

		c.Log.Debugf("Streamed chunk: chunk_id=%s index=%d size=%d",
			chunkMeta.ChunkId, chunkMeta.Index, chunk.SizeBytes)
	}

	c.Log.Infof("Download completed: file_id=%s chunks=%d", fileID, len(fileMetadata.Chunks))
	return nil
}

// getChunkFromNode fetches a chunk from a storage node
func (c *Coordinator) getChunkFromNode(ctx context.Context, node *proto.NodeInfo, chunkID string) (*proto.ChunkPayload, error) {
	// Create gRPC connection to storage node
	conn, err := grpc.NewClient(
		node.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s: %w", node.Addr, err)
	}
	defer conn.Close()

	// Create storage node client
	client := proto.NewStorageNodeServiceClient(conn)

	// Request chunk
	resp, err := client.GetChunk(ctx, &proto.GetChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return nil, fmt.Errorf("GetChunk RPC failed: %w", err)
	}

	// Check for error in response
	if errResp := resp.GetError(); errResp != nil {
		return nil, fmt.Errorf("node returned error: %s (code: %d)", errResp.Message, errResp.Code)
	}

	// Get chunk payload
	chunk := resp.GetChunk()
	if chunk == nil {
		return nil, fmt.Errorf("node returned empty chunk")
	}

	return chunk, nil
}

func (c *Coordinator) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {

	files, total, err := c.MetadataRepo.ListFiles(int64(req.Limit), int64(req.Offset))

	if err != nil {
		c.Log.Error("Failed to list files", "error", err)
		return nil, err
	}

	return &proto.ListFilesResponse{
		Files: files,
		Total: int32(total),
	}, nil
}

func (c *Coordinator) ListNodes(ctx context.Context, req *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {

	nodes, err := c.NodeRepo.ListNodes(ctx)
	if err != nil {
		c.Log.Error("Failed to list nodes", "error", err)
	}

	return &proto.ListNodesResponse{
		Nodes: nodes,
	}, nil
}

func (c *Coordinator) GetFileInfo(ctx context.Context, req *proto.GetFileInfoRequest) (*proto.FileMetadata, error) {
	fileID := req.FileId

	if fileID == "" {
		return nil, fmt.Errorf("file_id is required")
	}

	c.Log.Info("GetFileInfo called", "file_id", fileID)

	// Load file metadata from MongoDB
	fileMetadata, err := c.MetadataRepo.GetFile(fileID)
	if err != nil {
		c.Log.Error("Failed to get file metadata",
			zap.String("file_id", fileID),
			zap.Error(err))
		return nil, fmt.Errorf("file not found: %s", fileID)
	}

	return fileMetadata, nil
}

// ----------------------------
// Node Registration
// ----------------------------

// RegisterNode registers (or upserts) node metadata into Mongo and sets heartbeat key in Redis
func (c *Coordinator) RegisterNode(ctx context.Context, req *proto.RegisterNodeRequest) (*proto.RegisterNodeResponse, error) {
	info := req.Info
	c.Log.Infof("RegisterNode called node=%s addr=%s", info.NodeId, info.Addr)

	// set last_heartbeat if not present
	if info.LastHeartbeat == nil {
		info.LastHeartbeat = timestamppb.Now()
	}

	// persist to mongo
	if err := c.NodeRepo.UpsertNode(ctx, info); err != nil {
		c.Log.Error("NodeRepo.UpsertNode failed", zap.Error(err))
		return &proto.RegisterNodeResponse{Accepted: false, Error: err.Error()}, nil
	}

	// set redis heartbeat TTL
	if c.Redis != nil {
		// Use configured timeout, default to 120 seconds
		timeoutSec := c.Cfg.HeartbeatTimeoutSec
		if timeoutSec <= 0 {
			timeoutSec = 120
		}
		ttl := time.Duration(timeoutSec) * time.Second

		if err := rd.SetNodeHeartbeat(ctx, c.Redis, info.NodeId, ttl); err != nil {
			c.Log.Warn("redis.SetNodeHeartbeat failed", zap.Error(err))
		}
	}

	return &proto.RegisterNodeResponse{
		NodeId:   info.NodeId,
		Accepted: true,
	}, nil
}

func (c *Coordinator) pickNode(ctx context.Context) (*proto.NodeInfo, error) {
	nodes, err := c.NodeRepo.ListNodes(ctx)
	if err != nil {
		c.Log.Error("pickNode: failed ListNodes", zap.Error(err))
		return nil, fmt.Errorf("failed to pick node")
	}
	if len(nodes) == 0 {
		c.Log.Error("pickNode: no nodes registered")
		return nil, fmt.Errorf("no nodes available")
	}

	var best *proto.NodeInfo
	for _, n := range nodes {
		if n.State != proto.NodeState_HEALTHY {
			continue
		}
		if best == nil || n.FreeBytes > best.FreeBytes {
			best = n
		}
	}

	if best == nil {
		c.Log.Error("pickNode: no healthy nodes")
		return nil, fmt.Errorf("no healthy nodes available")
	}

	return best, nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	nodeID := req.NodeId
	c.Log.Infof("Heartbeat from node=%s free_bytes=%d", nodeID, req.FreeBytes)

	now := time.Now()
	// Update Mongo
	if err := c.NodeRepo.UpdateHeartbeat(ctx, nodeID, req.FreeBytes, now); err != nil {
		c.Log.Error("NodeRepo.UpdateHeartbeat failed", zap.Error(err))
		// continue; still set Redis heartbeat
	}

	// Update Redis TTL
	if c.Redis != nil {
		// Use configured timeout, default to 120 seconds
		timeoutSec := c.Cfg.HeartbeatTimeoutSec
		if timeoutSec <= 0 {
			timeoutSec = 120
		}
		ttl := time.Duration(timeoutSec) * time.Second

		if err := rd.SetNodeHeartbeat(ctx, c.Redis, nodeID, ttl); err != nil {
			c.Log.Warn("redis.SetNodeHeartbeat failed", zap.Error(err))
		}
	}

	return &proto.HeartbeatResponse{Ok: true}, nil
}

// ----------------------------
// CoordinatorAdmin RPCs
// ----------------------------

func (c *Coordinator) EnqueueReplication(ctx context.Context, job *proto.ReplicationJob) (*proto.ReplicationJobAck, error) {
	c.Log.Info("EnqueueReplication called (stub)", "job_id", job.JobId)
	return &proto.ReplicationJobAck{
		JobId:    job.JobId,
		Accepted: true,
	}, nil
}

func (c *Coordinator) ListReplicationJobs(ctx context.Context, req *proto.ListReplicationJobsRequest) (*proto.ListReplicationJobsResponse, error) {
	c.Log.Info("ListReplicationJobs called (stub)")
	return &proto.ListReplicationJobsResponse{
		Jobs:  []*proto.ReplicationJob{},
		Total: 0,
	}, nil
}
