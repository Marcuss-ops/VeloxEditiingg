package main

import (
	"fmt"
	"log"
	"os"

	"velox-server/internal/config"
)

const usageText = `Usage: velox-server [serve | migrate legacy-status]

When invoked without arguments the binary behaves identically to 'velox-server serve'.

Subcommands:
  serve                  Run the HTTP/gRPC master (default).
  migrate legacy-status  Read-only preflight audit for the runtime legacy
                         purge (PR7). Reports counts of legacy job states,
                         embedded flat fields, and legacy columns. With
                         --strict the command exits non-zero if there is
                         still legacy data blocking the purge.

Examples:
  velox-server serve
  velox-server migrate legacy-status
  velox-server migrate legacy-status --json
  velox-server migrate legacy-status --strict
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
	case args[0] == "migrate":
		os.Exit(runMigrate(cfg, args[1:]))
	case args[0] == "--help" || args[0] == "-h" || args[0] == "help":
		fmt.Print(usageText)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n%s", args[0], usageText)
		os.Exit(2)
	}
}

// runMigrate dispatches the `velox-server migrate <subcommand> [args...]`
// subcommand tree. Today only `legacy-status` is exposed; new subcommands
// (apply, rollback, workflows-v2 --apply, etc.) live here.
func runMigrate(cfg *config.Config, args []string) int {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "migrate: missing subcommand. Try `velox-server migrate legacy-status`.\n\n%s", usageText)
		return 2
	}
	switch args[0] {
	case "legacy-status":
		return runLegacyStatus(cfg, args[1:], os.Stdout, os.Stderr)
	case "--help", "-h", "help":
		fmt.Print(usageText)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "migrate: unknown subcommand %q\n\n%s", args[0], usageText)
		return 2
	}
}
