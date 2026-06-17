package main

import (
	"log"

	"velox-server/internal/config"
)

func main() {
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("config: %v", err)
	}
	if err := runServer(cfg); err != nil {
		log.Fatalf("server exit: %v", err)
	}
}
