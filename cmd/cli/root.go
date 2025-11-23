package main

import (
	"fmt"
	"time"

	"github.com/iLaxios/distrophile/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewRootCmd builds the root command and attaches subcommands.
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "distrophile",
		Short: "Distrophile CLI - Distributed file storage client",
		Long:  "A CLI tool for interacting with the Distrophile distributed file storage system",
		Run: func(cmd *cobra.Command, args []string) {
			printLogo()
			cmd.Help()
		},
	}

	rootCmd.AddCommand(uploadCmd())
	rootCmd.AddCommand(downloadCmd())
	rootCmd.AddCommand(listCmd())
	rootCmd.AddCommand(infoCmd())
	rootCmd.AddCommand(nodesCmd())
	rootCmd.AddCommand(registerNodeCmd())

	return rootCmd
}

// connect creates a gRPC connection to the coordinator and returns the client.
func connect(addr string) (*grpc.ClientConn, proto.CoordinatorServiceClient, error) {
	if addr == "" {
		return nil, nil, fmt.Errorf("coordinator address is empty")
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	client := proto.NewCoordinatorServiceClient(conn)
	// small sleep to allow connection negotiation if needed
	time.Sleep(10 * time.Millisecond)
	return conn, client, nil
}
