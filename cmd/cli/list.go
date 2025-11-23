package main

import (
	"context"
	"fmt"
	"time"

	"github.com/iLaxios/distrophile/proto"
	"github.com/spf13/cobra"
)

func listCmd() *cobra.Command {
	var limit int32
	var offset int32

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all files in the storage system",
		Long:  "List all files with pagination support",
		RunE: func(cmd *cobra.Command, args []string) error {
			return listFiles(limit, offset)
		},
	}

	cmd.Flags().Int32VarP(&limit, "limit", "l", 100, "Maximum number of files to list")
	cmd.Flags().Int32VarP(&offset, "offset", "o", 0, "Offset for pagination")
	return cmd
}

func listFiles(limit, offset int32) error {

	conn, client, err := connect(coordinatorAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListFiles(ctx, &proto.ListFilesRequest{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	fmt.Printf("Files (%d total):\n\n", resp.Total)
	if len(resp.Files) == 0 {
		fmt.Println("  No files found")
		return nil
	}

	for _, file := range resp.Files {
		fmt.Printf("  %s\n", file.FileId)
		fmt.Printf("    Filename: %s\n", file.Filename)
		fmt.Printf("    Size: %d bytes\n", file.SizeBytes)
		fmt.Printf("    Chunks: %d\n", len(file.Chunks))
		if file.CreatedAt != nil {
			fmt.Printf("    Created: %s\n", file.CreatedAt.AsTime().Format(time.RFC3339))
		}
		fmt.Println()
	}

	return nil
}
