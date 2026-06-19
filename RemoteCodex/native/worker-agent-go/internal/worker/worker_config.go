// Package worker provides configuration handling utilities for the worker agent.
package worker

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"velox-worker-agent/pkg/logger"
)

// WaitForShutdown blocks until a shutdown signal is received.
func WaitForShutdown(ctx context.Context, worker *Worker) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		worker.Stop()
	case sig := <-sigChan:
		logger.Info("Received signal %v, shutting down...", sig)
		worker.Stop()
	}
}