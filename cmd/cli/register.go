package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/iLaxios/distrophile/proto"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	nodeID    string
	nodeAddr  string
	freeBytes int64
)

func registerNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "register-node",
		Short: "Register a storage node with the coordinator",
		Long:  "Manually register a storage node by providing node ID, address, and available storage capacity",
		RunE: func(cmd *cobra.Command, args []string) error {
			return registerNode()
		},
	}

	cmd.Flags().StringVarP(&nodeID, "node-id", "n", "", "Node ID (auto-generated if not provided)")
	cmd.Flags().StringVarP(&nodeAddr, "address", "a", "", "Node address (host:port)")
	cmd.Flags().Int64VarP(&freeBytes, "free-bytes", "f", 0, "Free storage space in bytes")

	cmd.MarkFlagRequired("address")
	cmd.MarkFlagRequired("free-bytes")

	return cmd
}

func registerNode() error {
	// Generate node ID if not provided
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%s", uuid.New().String()[:8])
		fmt.Printf("Generated node ID: %s\n", nodeID)
	}

	// Validate inputs
	if nodeAddr == "" {
		return fmt.Errorf("node address is required")
	}
	if freeBytes <= 0 {
		return fmt.Errorf("free bytes must be greater than 0")
	}

	// Connect to coordinator
	conn, client, err := connect(coordinatorAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer conn.Close()

	// Prepare node info
	nodeInfo := &proto.NodeInfo{
		NodeId:        nodeID,
		Addr:          nodeAddr,
		State:         proto.NodeState_HEALTHY,
		FreeBytes:     freeBytes,
		LastHeartbeat: timestamppb.Now(),
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Call RegisterNode RPC
	fmt.Printf("Registering node with coordinator at %s...\n", coordinatorAddr)
	resp, err := client.RegisterNode(ctx, &proto.RegisterNodeRequest{
		Info: nodeInfo,
	})
	if err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	// Display result
	if resp.Accepted {
		fmt.Printf("\n✓ Node registered successfully!\n")
		fmt.Printf("  Node ID: %s\n", resp.NodeId)
		fmt.Printf("  Address: %s\n", nodeAddr)
		fmt.Printf("  Free Space: %.2f GB\n", float64(freeBytes)/(1024*1024*1024))
		fmt.Printf("  State: HEALTHY\n")
	} else {
		fmt.Printf("\n✗ Node registration failed\n")
		if resp.Error != "" {
			fmt.Printf("  Error: %s\n", resp.Error)
		}
		return fmt.Errorf("registration not accepted")
	}

	return nil
}
