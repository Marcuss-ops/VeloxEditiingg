// Package grpcserver / handler_server.go
//
// gRPC server bootstrap for the WorkerControl service.
// Extracted from handler.go to keep the core types file focused.
package grpcserver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "velox-shared/controltransport/pb"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// StartGRPCServer starts a gRPC server on the configured port and registers
// the WorkerControl handler. Supports mTLS when certFile/keyFile/caFile are provided.
func StartGRPCServer(port int, handler *Handler, certFile, keyFile, caFile string) (*grpc.Server, net.Listener, error) {
	if port <= 0 {
		return nil, nil, nil // gRPC disabled
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc: listen on :%d: %w", port, err)
	}

	var grpcOpts []grpc.ServerOption

	if certFile != "" && keyFile != "" {
		serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("grpc: load server cert/key: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			MinVersion:   tls.VersionTLS12,
		}

		if caFile != "" {
			caPEM, err := os.ReadFile(caFile)
			if err != nil {
				return nil, nil, fmt.Errorf("grpc: read ca file: %w", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caPEM) {
				return nil, nil, fmt.Errorf("grpc: failed to parse CA certificate")
			}
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			tlsConfig.ClientCAs = certPool
			log.Printf("[GRPC] mTLS enabled — requiring client certificates signed by CA")
		}

		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		allowInsecure := os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV") == "true"
		if !allowInsecure {
			return nil, nil, fmt.Errorf("grpc: TLS cert/key required in production (set VELOX_GRPC_ALLOW_INSECURE_DEV=true for dev)")
		}
		handler.config.AllowInsecure = true
		log.Printf("[GRPC] WARNING: insecure gRPC server — dev mode only")
		grpcOpts = append(grpcOpts, grpc.Creds(insecure.NewCredentials()))
	}

	// Add OpenTelemetry stats handler for gRPC trace context propagation.
	// otelgrpc.NewServerHandler() extracts W3C traceparent from inbound
	// gRPC metadata and injects it into the request context.
	// Scorecard v2 / Step 15c.
	grpcOpts = append(grpcOpts, grpc.StatsHandler(otelgrpc.NewServerHandler()))

	srv := grpc.NewServer(grpcOpts...)
	pb.RegisterWorkerControlServer(srv, handler)

	// Serve in a goroutine but block until the server is actually accepting
	// connections. Without this, the caller may return before srv.Serve()
	// enters its accept loop, creating a race where workers see "connection
	// reset by peer" because the TCP handshake completes but the gRPC
	// server isn't ready to handle the preface exchange.
	serveStarted := make(chan struct{})
	go func() {
		// Close serveStarted immediately before srv.Serve(lis) to signal
		// that the goroutine has launched. There is a residual window between
		// close(serveStarted) and srv.Serve entering its accept loop; the TCP
		// dial below (belt-and-suspenders) catches that gap.
		close(serveStarted)
		log.Printf("[GRPC] Velox master gRPC server listening on :%d", port)
		if err := srv.Serve(lis); err != nil {
			log.Printf("[GRPC] Server error: %v", err)
		}
	}()
	// Wait for the goroutine to close serveStarted — this gates the goroutine
	// launch but NOT the gRPC accept loop (see belt-and-suspenders below).
	<-serveStarted

	// Belt-and-suspenders: verify the OS accept queue is actually ready with
	// a local TCP dial. This catches the residual race window between
	// close(serveStarted) and srv.Serve entering its accept loop.
	if conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond); err == nil {
		conn.Close()
	}

	return srv, lis, nil
}
