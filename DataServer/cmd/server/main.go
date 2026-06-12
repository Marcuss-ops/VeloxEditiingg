package main

import (
	"log"

	"velox-server/internal/config"
)

func main() {
	cfg := config.FromEnv()
	if err := runServer(cfg); err != nil {
		log.Fatalf("server exit: %v", err)
	}
}
