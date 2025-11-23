package mongodb

import (
	"context"
	"time"

	pb "github.com/iLaxios/distrophile/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NodeRepo struct {
	collection *mongo.Collection
}

func NewNodeRepo(client *mongo.Client, dbName string) *NodeRepo {
	return &NodeRepo{
		collection: client.Database(dbName).Collection("nodes"),
	}
}

func (r *NodeRepo) UpsertNode(ctx context.Context, info *pb.NodeInfo) error {
	now := time.Now()
	filter := bson.M{"node_id": info.NodeId}
	update := bson.M{
		"$set": bson.M{
			"addr":           info.Addr,
			"state":          info.State.String(), // store enum name
			"free_bytes":     info.FreeBytes,
			"last_heartbeat": info.LastHeartbeat.AsTime(),
			"updated_at":     now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}
	_, err := r.collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

func (r *NodeRepo) UpdateHeartbeat(ctx context.Context, nodeID string, freeBytes int64, last time.Time) error {
	filter := bson.M{"node_id": nodeID}
	update := bson.M{
		"$set": bson.M{
			"free_bytes":     freeBytes,
			"last_heartbeat": last,
			"state":          "HEALTHY",
			"updated_at":     time.Now(),
		},
	}
	_, err := r.collection.UpdateOne(ctx, filter, update)
	return err
}

func (r *NodeRepo) ListNodes(ctx context.Context) ([]*pb.NodeInfo, error) {
	cur, err := r.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var out []*pb.NodeInfo
	for cur.Next(ctx) {
		var m NodeModel
		if err := cur.Decode(&m); err != nil {
			continue
		}
		out = append(out, &pb.NodeInfo{
			NodeId:        m.NodeID,
			Addr:          m.Addr,
			State:         pb.NodeState(pb.NodeState_value[m.State]), // convert back if possible
			FreeBytes:     m.FreeBytes,
			LastHeartbeat: timestamppb.New(m.LastHeartbeat),
		})
	}
	return out, nil
}

func (r *NodeRepo) UpdateNodeState(ctx context.Context, nodeID string, state pb.NodeState) error {
	filter := bson.M{"node_id": nodeID}
	update := bson.M{
		"$set": bson.M{
			"state":      state.String(),
			"updated_at": time.Now(),
		},
	}
	_, err := r.collection.UpdateOne(ctx, filter, update)
	return err
}
