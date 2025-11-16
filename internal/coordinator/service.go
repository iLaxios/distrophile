package coordinator

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"

	"github.com/iLaxios/distrophile/internal/common/config"
	"github.com/iLaxios/distrophile/internal/mongodb"
	rd "github.com/iLaxios/distrophile/internal/redis"
	"go.mongodb.org/mongo-driver/mongo"
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
	c.Log.Info("Upload called (stub)")
	return nil
}

func (c *Coordinator) Download(req *proto.DownloadRequest, stream proto.CoordinatorService_DownloadServer) error {
	c.Log.Info("Download called (stub)", "file_id", req.FileId)
	return nil
}

func (c *Coordinator) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	c.Log.Info("ListFiles called (stub)")

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
	c.Log.Info("ListNodes called (stub)")
	return &proto.ListNodesResponse{
		Nodes: []*proto.NodeInfo{},
	}, nil
}

func (c *Coordinator) GetFileInfo(ctx context.Context, req *proto.GetFileInfoRequest) (*proto.FileMetadata, error) {
	c.Log.Info("GetFileInfo called (stub)", "file_id", req.FileId)
	return nil, nil
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
		if err := rd.SetNodeHeartbeat(ctx, c.Redis, info.NodeId, 30*time.Second); err != nil {
			c.Log.Warn("redis.SetNodeHeartbeat failed", zap.Error(err))
		}
	}

	return &proto.RegisterNodeResponse{
		NodeId:   info.NodeId,
		Accepted: true,
	}, nil
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
		if err := rd.SetNodeHeartbeat(ctx, c.Redis, nodeID, 30*time.Second); err != nil {
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
