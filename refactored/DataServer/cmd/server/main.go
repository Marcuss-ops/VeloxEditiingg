package main

import (
	"fmt"
	"log"
	"os"

	"velox-server/internal/config"
)

const usageText = `Usage: velox-server [serve]

When invoked without arguments the binary behaves identically to 'velox-server serve'.
The legacy 'migrate' subcommand, the pdf bridge CLI, and the remote-engine-bridge
have all been removed; data is now managed exclusively via the SQLite-backed
YouTube service and the master HTTP endpoints.
`

func main() {
	cfg := config.FromEnv()

	switch {
	case len(args) == 0:
		// Default: serve.
		if err := runServer(cfg); err != nil {
			log.Fatalf("server exit: %v", err)
		}
	case args[0] == "serve":
		if err := runServer(cfg); err != nil {
			log.Fatalf("server exit: %v", err)
		}
	case args[0] == "--help" || args[0] == "-h" || args[0] == "help":
		fmt.Print(usageText)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n%s", args[0], usageText)
		os.Exit(2)
	}
}
