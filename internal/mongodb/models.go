package mongodb

import (
	"time"

	pb "github.com/iLaxios/distrophile/proto"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FileMetadataModel struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	FileID    string             `bson:"file_id"`
	Filename  string             `bson:"filename"`
	SizeBytes int64              `bson:"size_bytes"`
	Chunks    []ChunkMetaModel   `bson:"chunks"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
}

type ChunkMetaModel struct {
	ChunkID   string   `bson:"chunk_id"`
	Index     int32    `bson:"index"`
	SizeBytes int64    `bson:"size_bytes"`
	Checksum  string   `bson:"checksum"`
	NodeIDs   []string `bson:"node_ids"`
}

// Helper: convert proto -> model
func FileMetadataToModel(pbMeta *pb.FileMetadata) FileMetadataModel {
	chunks := make([]ChunkMetaModel, len(pbMeta.Chunks))
	for i, c := range pbMeta.Chunks {
		chunks[i] = ChunkMetaModel{
			ChunkID:   c.ChunkId,
			Index:     c.Index,
			SizeBytes: c.SizeBytes,
			Checksum:  c.Checksum,
			NodeIDs:   c.NodeIds,
		}
	}

	return FileMetadataModel{
		FileID:    pbMeta.FileId,
		Filename:  pbMeta.Filename,
		SizeBytes: pbMeta.SizeBytes,
		Chunks:    chunks,
		CreatedAt: pbMeta.CreatedAt.AsTime(),
		UpdatedAt: pbMeta.UpdatedAt.AsTime(),
	}
}

// Helper: convert model -> proto
func ModelToFileMetadata(model FileMetadataModel) *pb.FileMetadata {
	chunks := make([]*pb.ChunkMeta, len(model.Chunks))
	for i, c := range model.Chunks {
		chunks[i] = &pb.ChunkMeta{
			ChunkId:   c.ChunkID,
			Index:     c.Index,
			SizeBytes: c.SizeBytes,
			Checksum:  c.Checksum,
			NodeIds:   c.NodeIDs,
		}
	}

	return &pb.FileMetadata{
		FileId:    model.FileID,
		Filename:  model.Filename,
		SizeBytes: model.SizeBytes,
		Chunks:    chunks,
		CreatedAt: timestamppb.New(model.CreatedAt),
		UpdatedAt: timestamppb.New(model.UpdatedAt),
	}
}

// node model
type NodeModel struct {
	ID            primitive.ObjectID `bson:"_id,omitempty"`
	NodeID        string             `bson:"node_id"`
	Addr          string             `bson:"addr"`
	State         string             `bson:"state"`
	FreeBytes     int64              `bson:"free_bytes"`
	LastHeartbeat time.Time          `bson:"last_heartbeat"`
	CreatedAt     time.Time          `bson:"created_at"`
	UpdatedAt     time.Time          `bson:"updated_at"`
}
