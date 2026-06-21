package main

import (
	"context"
	"log"
	"net/http"
	"time"
)

// shutdownHTTPServer gracefully shuts down the HTTP server with a
// bounded timeout and logs the outcome.  It is extracted from the
// runServer composition root so the teardown sequence is a single
// obvious call site rather than an inline 20-line block.
func shutdownHTTPServer(srv *http.Server, timeout time.Duration) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[SERVER] Graceful shutdown failed: %v", err)
	} else {
		log.Println("[SERVER] HTTP server stopped cleanly")
	}
}

// shutdownGRPCServer stops the gRPC server immediately (not graceful —
// workers hold open bidirectional streams that prevent GracefulStop
// from ever returning).  Extracted here so the teardown block in
// runServer stays minimal.
func shutdownGRPCServer(srv grpcServer) {
	if srv == nil {
		return
	}
	srv.Stop()
	log.Println("[SERVER] gRPC server stopped")
}
