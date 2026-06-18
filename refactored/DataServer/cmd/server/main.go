package main

import (
	"fmt"
	"log"
	"os"

	"velox-server/internal/config"
)

const usageText = `Usage: velox-server [serve]

When invoked without arguments the binary behaves identically to 'velox-server serve'.

Subcommands:
  serve  Run the HTTP/gRPC master (default).

Examples:
  velox-server serve
`

func main() {
	cfg := config.FromEnv()
	args := os.Args[1:]

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

