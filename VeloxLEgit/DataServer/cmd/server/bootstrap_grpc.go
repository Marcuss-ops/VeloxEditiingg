package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/grpcserver"

	"google.golang.org/grpc"
)

type grpcServer interface {
	GracefulStop()
	Stop()
}

type grpcServerWrapper struct {
	Server   *grpc.Server
	Listener net.Listener
}

func (w *grpcServerWrapper) GracefulStop() { w.Server.GracefulStop() }
func (w *grpcServerWrapper) Stop()         { w.Server.Stop() }

// enforceGRPCRequireTLS implements RW-PROD-001 §3 A5: when the operator sets
//
//	VELOX_GRPC_REQUIRE_TLS=true
//
// the master MUST refuse to bootstrap if gRPC TLS material is missing.
// Refactored to return error (M2 from RW-PROD-001 final review) so
// unit tests can assert the failure mode without process-exit wrappers.
// The caller (runServer) wraps the error in log.Fatal at the production
// composition root.
//
// Order of checks (matches cfg_server.go load order):
//  1. cfg.Server.GRPCTLSCertFile (cert against the master)
//  2. cfg.Server.GRPCTLSKeyFile  (master's private key)
//  3. cfg.Server.GRPCTLSCAFile   (CA used to verify worker client certs)
//
// The env is read with TrimSpace so accidental whitespace from copy-paste
// (a recurring operator mistake) does not silently downgrade behaviour.
// Production fail-closed: the same VELOX_GRPC_REQUIRE_TLS=true value works
// in dev (where it would also block, but local dev typically has certs
// via the gen-worker-certs.sh helper).
func enforceGRPCRequireTLS(cfg *config.Config) error {
	if strings.TrimSpace(os.Getenv("VELOX_GRPC_REQUIRE_TLS")) != "true" {
		return nil // feature off; existing fail-fast logic in StartGRPCServer applies
	}
	var missing []string
	if strings.TrimSpace(cfg.Server.GRPCTLSCertFile) == "" {
		missing = append(missing, "VELOX_GRPC_TLS_CERT_FILE")
	}
	if strings.TrimSpace(cfg.Server.GRPCTLSKeyFile) == "" {
		missing = append(missing, "VELOX_GRPC_TLS_KEY_FILE")
	}
	if strings.TrimSpace(cfg.Server.GRPCTLSCAFile) == "" {
		missing = append(missing, "VELOX_GRPC_TLS_CA_FILE")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf(
		"RW-PROD-001 A5: VELOX_GRPC_REQUIRE_TLS=true but missing TLS material: %s. "+
			"Refusing to start the master with plaintext gRPC because the operator opted into "+
			"hard-Reject (see docs/rw-prod/RW-PROD-001.md §3 A5). Either supply the cert/key/CA "+
			"triple or unset VELOX_GRPC_REQUIRE_TLS to fall back to the startGRPCServer runtime guard",
		strings.Join(missing, ", "))
}

func buildGRPCHandlerConfig(cfg *config.Config, insecureDev bool) *grpcserver.HandlerConfig {
	return &grpcserver.HandlerConfig{
		PushMode:       cfg.Server.GRPCPushMode,
		AllowInsecure:  insecureDev,
		AllowedWorkers: cfg.Workers.AllowedWorkers,
	}
}
