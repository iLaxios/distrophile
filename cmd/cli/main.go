package main

import (
	"fmt"
	"os"
)

var coordinatorAddr string

func printLogo() {
	fmt.Println(`
  _____  _     _                   _     _ _      
 |  __ \(_)   | |                 | |   (_) |     
 | |  | |_ ___| |_ _ __ ___  _ __ | |__  _| | ___ 
 | |  | | / __| __| '__/ _ \| '_ \| '_ \| | |/ _ \
 | |__| | \__ \ |_| | | (_) | |_) | | | | | |  __/
 |_____/|_|___/\__|_|  \___/| .__/|_| |_|_|_|\___|
                            | |                   
                            |_|                   
	`)
}

func main() {
	rootCmd := NewRootCmd()

	rootCmd.PersistentFlags().StringVarP(&coordinatorAddr, "coordinator", "c", getDefaultCoordinatorAddr(), "Coordinator address")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getDefaultCoordinatorAddr() string {
	if addr := os.Getenv("COORDINATOR_ADDR"); addr != "" {
		return addr
	}
	return "localhost:9000"
}
