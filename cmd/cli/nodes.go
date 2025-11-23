package main

import (
	"context"
	"fmt"
	"time"

	"github.com/iLaxios/distrophile/proto"
	"github.com/spf13/cobra"
)

func nodesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "List all storage nodes",
		Long:  "List all registered storage nodes with their status and capacity",
		RunE: func(cmd *cobra.Command, args []string) error {
			return listNodes()
		},
	}
	return cmd
}

func listNodes() error {
	conn, client, err := connect(coordinatorAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListNodes(ctx, &proto.ListNodesRequest{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	fmt.Printf("Storage Nodes (%d total):\n\n", len(resp.Nodes))
	if len(resp.Nodes) == 0 {
		fmt.Println("  No nodes registered")
		return nil
	}

	for _, node := range resp.Nodes {
		stateStr := node.State.String()
		freeGB := float64(node.FreeBytes) / (1024 * 1024 * 1024)
		fmt.Printf("  %s\n", node.NodeId)
		fmt.Printf("    Address: %s\n", node.Addr)
		fmt.Printf("    State: %s\n", stateStr)
		fmt.Printf("    Free: %.2f GB\n", freeGB)
		if node.LastHeartbeat != nil {
			fmt.Printf("    Last Heartbeat: %s\n", node.LastHeartbeat.AsTime().Format(time.RFC3339))
		}
		fmt.Println()
	}

	return nil
}
