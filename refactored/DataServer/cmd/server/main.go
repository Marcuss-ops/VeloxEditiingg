package main

import (
	"fmt"
	"log"
	"os"

	"velox-server/internal/config"
)

const usageText = `Usage: velox-server [command] [<args>]

Commands:
  serve                       Start the Velox HTTP server (default if no command is given).
  migrate <subcommand>        One-shot admin subcommands. Run 'velox-server migrate' for the list.

When invoked without arguments the binary behaves identically to 'velox-server serve'.
`

func main() {
	args := os.Args[1:]
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
	case args[0] == "migrate":
		if err := runMigrate(cfg, args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "migrate exit: %v\n", err)
			os.Exit(2)
		}
	case args[0] == "--help" || args[0] == "-h" || args[0] == "help":
		fmt.Print(usageText)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n%s", args[0], usageText)
		os.Exit(2)
	}
}
