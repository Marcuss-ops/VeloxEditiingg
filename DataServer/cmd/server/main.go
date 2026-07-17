package main

import (
	"fmt"
	"log"
	"os"

	"velox-server/internal/config"
)

// Build-time metadata, injected via `go build -ldflags "-X main.Version=...
// -X main.BuildTime=..."`. Declared as `var` (not `const`) because
// ldflags -X only mutates writable symbols. Defaults are safe values
// for local dev; release pipelines (DataServer/Dockerfile and
// .github/workflows/master-image.yml) override them at build time.
var (
	Version   = "dev"
	BuildTime = "unknown"
)

const usageText = `Usage: velox-server [serve]

When invoked without arguments the binary behaves identically to 'velox-server serve'.

Subcommands:
  serve  Run the HTTP/gRPC master (default).

Examples:
  velox-server serve
`

func main() {
	// Emit the build identity early so operators see it in
	// `docker logs` / `journalctl` regardless of where the binary
	// actually listens. Cheap; no side effects.
	log.Printf("velox-server %s (built %s)", Version, BuildTime)

	// Load optional .env file before reading configuration from the
	// environment. This makes local development and end-to-end tests
	// easier because a single file can carry all variables, while
	// shell-exported values still take precedence.
	if envPath := config.EnvFilePath(); envPath != "" {
		if err := config.LoadEnvFile(envPath); err != nil {
			log.Printf("[BOOTSTRAP] WARNING: failed to load env file %s: %v", envPath, err)
		}
	}

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
