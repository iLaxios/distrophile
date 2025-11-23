package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/iLaxios/distrophile/proto"
	"github.com/spf13/cobra"
)

func downloadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download <file_id> [output_file]",
		Short: "Download a file from the storage system",
		Long:  "Download a file by its file_id. If output_file is not specified, the original filename will be used.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			fileID := args[0]
			outputPath := fileID + ".downloaded"
			if len(args) >= 2 {
				outputPath = args[1]
			}
			return downloadFile(fileID, outputPath)
		},
	}
	return cmd
}

func downloadFile(fileID, outputPath string) error {

	conn, client, err := connect(coordinatorAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stream, err := client.Download(ctx, &proto.DownloadRequest{
		FileId: fileID,
	})
	if err != nil {
		return fmt.Errorf("failed to create download stream: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	var fileMetadata *proto.FileMetadata
	var totalBytes int64

	fmt.Printf("Downloading file_id=%s...\n", fileID)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive: %w", err)
		}

		switch payload := resp.Payload.(type) {
		case *proto.DownloadResponse_Metadata:
			fileMetadata = payload.Metadata
			fmt.Printf("File: %s (%d bytes, %d chunks)\n",
				fileMetadata.Filename, fileMetadata.SizeBytes, len(fileMetadata.Chunks))
		case *proto.DownloadResponse_Chunk:
			chunk := payload.Chunk
			if _, err := file.Write(chunk.Data); err != nil {
				return fmt.Errorf("failed to write chunk: %w", err)
			}
			totalBytes += chunk.SizeBytes
			fmt.Printf("✓ Received chunk %d (%d bytes)\n", chunk.Index, chunk.SizeBytes)
		case *proto.DownloadResponse_Error:
			return fmt.Errorf("download error: %s (code: %d)", payload.Error.Message, payload.Error.Code)
		}
	}

	if fileMetadata != nil {
		// Rename to original filename if we have metadata
		if fileMetadata.Filename != "" {
			newPath := filepath.Join(filepath.Dir(outputPath), fileMetadata.Filename)
			if err := os.Rename(outputPath, newPath); err == nil {
				outputPath = newPath
			}
		}
	}

	fmt.Printf("\n✅ Download complete!\n")
	fmt.Printf("  Saved to: %s\n", outputPath)
	fmt.Printf("  Size: %d bytes\n", totalBytes)
	return nil
}
