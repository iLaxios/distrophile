package mongodb

import (
	"context"
	"time"

	pb "github.com/iLaxios/distrophile/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MetadataRepo struct {
	db         *mongo.Database
	collection *mongo.Collection
}

func NewMetadataRepo(client *mongo.Client, dbName string) *MetadataRepo {
	db := client.Database(dbName)
	return &MetadataRepo{
		db:         db,
		collection: db.Collection("files"),
	}
}

func (r *MetadataRepo) SaveFile(meta *pb.FileMetadata) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	model := FileMetadataToModel(meta)
	model.UpdatedAt = time.Now()
	if model.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	}

	_, err := r.collection.ReplaceOne(ctx, bson.M{"file_id": model.FileID}, model, options.Replace().SetUpsert(true))
	return err
}

func (r *MetadataRepo) GetFile(fileID string) (*pb.FileMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var model FileMetadataModel
	err := r.collection.FindOne(ctx, bson.M{"file_id": fileID}).Decode(&model)
	if err != nil {
		return nil, err
	}

	return ModelToFileMetadata(model), nil
}

func (r *MetadataRepo) ListFiles(limit, offset int64) ([]*pb.FileMetadata, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Find().SetLimit(limit).SetSkip(offset)
	cursor, err := r.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	files := []*pb.FileMetadata{}
	for cursor.Next(ctx) {
		var model FileMetadataModel
		if err := cursor.Decode(&model); err != nil {
			continue
		}
		files = append(files, ModelToFileMetadata(model))
	}

	count, _ := r.collection.CountDocuments(ctx, bson.M{})
	return files, count, nil
}
