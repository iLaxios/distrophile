package main

import (
	"context"
	"fmt"
	"time"

	"github.com/iLaxios/distrophile/proto"
	"github.com/spf13/cobra"
)

func infoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info <file_id>",
		Short: "Get detailed information about a file",
		Long:  "Retrieve metadata and chunk information for a specific file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return getFileInfo(args[0])
		},
	}
	return cmd
}

func getFileInfo(fileID string) error {

	conn, client, err := connect(coordinatorAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	file, err := client.GetFileInfo(ctx, &proto.GetFileInfoRequest{
		FileId: fileID,
	})
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	fmt.Printf("File Information:\n\n")
	fmt.Printf("  File ID: %s\n", file.FileId)
	fmt.Printf("  Filename: %s\n", file.Filename)
	fmt.Printf("  Size: %d bytes\n", file.SizeBytes)
	fmt.Printf("  Chunks: %d\n", len(file.Chunks))
	if file.CreatedAt != nil {
		fmt.Printf("  Created: %s\n", file.CreatedAt.AsTime().Format(time.RFC3339))
	}
	if file.UpdatedAt != nil {
		fmt.Printf("  Updated: %s\n", file.UpdatedAt.AsTime().Format(time.RFC3339))
	}

	if len(file.Chunks) > 0 {
		fmt.Printf("\n  Chunks:\n")
		for i, chunk := range file.Chunks {
			fmt.Printf("    [%d] %s - %d bytes (nodes: %v)\n",
				i, chunk.ChunkId, chunk.SizeBytes, chunk.NodeIds)
		}
	}

	return nil
}
