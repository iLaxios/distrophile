package storagenode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StorageNode implements proto.StorageNodeServiceServer
type StorageNode struct {
	Cfg    *config.Config
	Log    *zap.SugaredLogger
	NodeID string // This node's ID

	proto.UnimplementedStorageNodeServiceServer
}

// NewStorageNode creates a new storage node service
func NewStorageNode(cfg *config.Config, log *zap.SugaredLogger, nodeID string) *StorageNode {
	return &StorageNode{
		Cfg:    cfg,
		Log:    log,
		NodeID: nodeID,
	}
}

// StoreChunk handles bidirectional streaming: receives chunks and acks back
func (s *StorageNode) StoreChunk(stream proto.StorageNodeService_StoreChunkServer) error {
	ctx := stream.Context()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Coordinator finished sending chunks
			return nil
		}
		if err != nil {
			s.Log.Error("Error receiving StoreChunk request", zap.Error(err))
			return err
		}

		chunk := req.Chunk
		if chunk == nil {
			return status.Errorf(codes.InvalidArgument, "chunk payload is required")
		}

		chunkID := chunk.ChunkId
		if chunkID == "" {
			return status.Errorf(codes.InvalidArgument, "chunk_id is required")
		}

		// Calculate checksum if not provided
		checksum := chunk.Checksum
		if checksum == "" && len(chunk.Data) > 0 {
			hash := sha256.Sum256(chunk.Data)
			checksum = hex.EncodeToString(hash[:])
		}

		// Verify checksum if provided
		if checksum != "" && len(chunk.Data) > 0 {
			hash := sha256.Sum256(chunk.Data)
			calculatedChecksum := hex.EncodeToString(hash[:])
			if checksum != calculatedChecksum {
				s.Log.Warn("Checksum mismatch",
					zap.String("chunk_id", chunkID),
					zap.String("expected", checksum),
					zap.String("calculated", calculatedChecksum))

				// Send error ack
				if err := stream.Send(&proto.StoreChunkAck{
					ChunkId: chunkID,
					NodeId:  s.NodeID,
					Ok:      false,
					Error:   fmt.Sprintf("checksum mismatch: expected %s, got %s", checksum, calculatedChecksum),
				}); err != nil {
					return err
				}
				continue
			}
		}

		// Store chunk to disk
		if err := s.saveChunkToDisk(ctx, chunkID, chunk.Data); err != nil {
			s.Log.Error("Failed to save chunk to disk",
				zap.String("chunk_id", chunkID),
				zap.Error(err))

			// Send error ack
			if err := stream.Send(&proto.StoreChunkAck{
				ChunkId: chunkID,
				NodeId:  s.NodeID,
				Ok:      false,
				Error:   fmt.Sprintf("failed to save chunk: %v", err),
			}); err != nil {
				return err
			}
			continue
		}

		s.Log.Infof("Stored chunk: chunk_id=%s size=%d file_id=%s",
			chunkID, chunk.SizeBytes, req.FileId)

		// Send success ack
		if err := stream.Send(&proto.StoreChunkAck{
			ChunkId:  chunkID,
			NodeId:   s.NodeID,
			Ok:       true,
			Checksum: checksum,
		}); err != nil {
			return err
		}
	}
}

// saveChunkToDisk saves chunk data to disk at {data_dir}/chunks/{chunk_id}
func (s *StorageNode) saveChunkToDisk(_ context.Context, chunkID string, data []byte) error {
	// Create chunks directory if it doesn't exist
	chunksDir := filepath.Join(s.Cfg.DataDir, "chunks")
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return fmt.Errorf("failed to create chunks directory: %w", err)
	}

	// Write chunk to file
	chunkPath := filepath.Join(chunksDir, chunkID)
	if err := os.WriteFile(chunkPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write chunk file: %w", err)
	}

	return nil
}

// GetChunk retrieves a chunk from disk
func (s *StorageNode) GetChunk(ctx context.Context, req *proto.GetChunkRequest) (*proto.GetChunkResponse, error) {
	if req.ChunkId == "" {
		return &proto.GetChunkResponse{
			Payload: &proto.GetChunkResponse_Error{
				Error: &proto.ErrorErr{
					Message: "chunk_id is required",
					Code:    1,
				},
			},
		}, nil
	}

	chunkPath := filepath.Join(s.Cfg.DataDir, "chunks", req.ChunkId)
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &proto.GetChunkResponse{
				Payload: &proto.GetChunkResponse_Error{
					Error: &proto.ErrorErr{
						Message: fmt.Sprintf("chunk not found: %s", req.ChunkId),
						Code:    404,
					},
				},
			}, nil
		}
		return &proto.GetChunkResponse{
			Payload: &proto.GetChunkResponse_Error{
				Error: &proto.ErrorErr{
					Message: fmt.Sprintf("failed to read chunk: %v", err),
					Code:    500,
				},
			},
		}, nil
	}

	// Calculate checksum
	hash := sha256.Sum256(data)
	checksum := hex.EncodeToString(hash[:])

	s.Log.Infof("Retrieved chunk: chunk_id=%s size=%d", req.ChunkId, len(data))

	return &proto.GetChunkResponse{
		Payload: &proto.GetChunkResponse_Chunk{
			Chunk: &proto.ChunkPayload{
				ChunkId:   req.ChunkId,
				Data:      data,
				SizeBytes: int64(len(data)),
				Checksum:  checksum,
			},
		},
	}, nil
}

// ReplicateChunk replicates a chunk from another node
func (s *StorageNode) ReplicateChunk(ctx context.Context, req *proto.ReplicateChunkRequest) (*proto.ReplicateChunkResponse, error) {
	// TODO: Implement replication logic
	// This would involve:
	// 1. Connect to source node
	// 2. Fetch chunk via GetChunk
	// 3. Store locally via saveChunkToDisk
	// 4. Return success/failure

	s.Log.Info("ReplicateChunk called (not implemented)", "chunk_id", req.ChunkId)
	return &proto.ReplicateChunkResponse{
		ChunkId: req.ChunkId,
		Ok:      false,
		Error:   "replication not yet implemented",
	}, nil
}
