package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/iLaxios/distrophile/proto"
	"github.com/spf13/cobra"
)

func uploadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload <file>",
		Short: "Upload a file to the storage system",
		Long:  "Upload a file to the distributed storage system. The file will be chunked and stored across storage nodes.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return uploadFile(args[0])
		},
	}
	return cmd
}

func uploadFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	conn, client, err := connect(coordinatorAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stream, err := client.Upload(ctx)
	if err != nil {
		return fmt.Errorf("failed to create upload stream: %w", err)
	}

	// Send metadata
	filename := filepath.Base(filePath)
	chunkSize := int32(2 * 1024 * 1024) // 2MB
	err = stream.Send(&proto.UploadRequest{
		Payload: &proto.UploadRequest_Meta{
			Meta: &proto.FileUploadMeta{
				Filename:  filename,
				SizeBytes: fileInfo.Size(),
				ChunkSize: chunkSize,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send metadata: %w", err)
	}

	fmt.Printf("Uploading %s (%d bytes)...\n", filename, fileInfo.Size())

	// Start receiving responses
	done := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				done <- nil
				return
			}
			if err != nil {
				done <- err
				return
			}

			switch payload := resp.Payload.(type) {
			case *proto.UploadResponse_ChunkResult:
				result := payload.ChunkResult
				if result.Success {
					fmt.Printf("✓ Chunk uploaded: %s\n", result.ChunkId)
				} else {
					fmt.Printf("✗ Chunk failed: %s - %s\n", result.ChunkId, result.Error)
				}
			case *proto.UploadResponse_Summary:
				summary := payload.Summary
				fmt.Printf("\n✅ Upload complete!\n")
				fmt.Printf("  File ID: %s\n", summary.FileId)
				fmt.Printf("  Filename: %s\n", summary.Metadata.Filename)
				fmt.Printf("  Size: %d bytes\n", summary.Metadata.SizeBytes)
				fmt.Printf("  Chunks: %d\n", len(summary.Metadata.Chunks))
				done <- nil
				return
			case *proto.UploadResponse_Error:
				done <- fmt.Errorf("upload error: %s (code: %d)", payload.Error.Message, payload.Error.Code)
				return
			}
		}
	}()

	// Send file in chunks
	chunkIndex := int32(0)
	buffer := make([]byte, chunkSize)

	for {
		n, err := file.Read(buffer)
		if n == 0 && err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Calculate checksum
		hash := sha256.Sum256(buffer[:n])
		checksum := hex.EncodeToString(hash[:])

		// Send chunk
		err = stream.Send(&proto.UploadRequest{
			Payload: &proto.UploadRequest_Chunk{
				Chunk: &proto.ChunkPayload{
					Index:     chunkIndex,
					Data:      buffer[:n],
					SizeBytes: int64(n),
					Checksum:  checksum,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to send chunk %d: %w", chunkIndex, err)
		}

		chunkIndex++
	}

	// Close send side
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close send: %w", err)
	}

	// Wait for completion
	return <-done
}
