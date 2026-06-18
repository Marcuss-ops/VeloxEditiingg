package main

import (
	"fmt"
	"log"
	"os"

	"velox-server/internal/config"
)

func main() {
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("config: %v", err)
	}

	// Dispatch subcommands before starting the server.
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := runMigrate(cfg, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := runServer(cfg); err != nil {
		log.Fatalf("server exit: %v", err)
	}
}
