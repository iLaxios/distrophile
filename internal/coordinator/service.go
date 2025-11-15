package coordinator

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"

	"github.com/iLaxios/distrophile/internal/common/config"
	"go.mongodb.org/mongo-driver/mongo"
)

// Coordinator implements proto.CoordinatorServiceServer & proto.CoordinatorAdminServer
type Coordinator struct {
	Cfg   *config.Config
	Log   *zap.SugaredLogger
	Mongo *mongo.Client
	Redis *redis.Client

	proto.UnimplementedCoordinatorServiceServer
	proto.UnimplementedCoordinatorAdminServer
}

// Constructor for Coordinator
func NewCoordinator(cfg *config.Config, log *zap.SugaredLogger, mongo *mongo.Client, rdb *redis.Client) *Coordinator {
	return &Coordinator{
		Cfg:   cfg,
		Log:   log,
		Mongo: mongo,
		Redis: rdb,
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
	return &proto.ListFilesResponse{
		Files: []*proto.FileMetadata{},
		Total: 0,
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
