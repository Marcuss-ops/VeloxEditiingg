package main

// Transport: HTTP listener start/stop + gRPC listener start/stop
// (with optional TLS, worker allowlist, capability-registry gate).
// The transportBundle owns both listeners and the single error
// channel runServer reads from in its main select. Graceful
// shutdown stops gRPC first (so workers stop accepting pushes),
// then HTTP (so in-flight requests finish), with bounded timeouts.
//
// Blocco 4 step #2: extracted from bootstrap.go.

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/grpcserver"
)

// transportBundle owns the running listeners and the single error
// channel runServer reads from in its main select.
type transportBundle struct {
	httpServer  *http.Server
	router      *gin.Engine
	grpcServer  grpcServer
	grpcStarted bool
	errChan     chan error
}

// shutdown stops the gRPC listener first (so workers stop accepting
// new pushes) then the HTTP listener (so in-flight requests finish).
// Returns once both listeners have exited or their stop timeouts fired.
func (t *transportBundle) shutdown() {
	if t == nil {
		return
	}
	if t.grpcServer != nil {
		shutdownGRPCServer(t.grpcServer)
	}
	shutdownHTTPServer(t.httpServer, 30*time.Second)
}

// startTransports brings up the HTTP and gRPC listeners from the
// fully-built appComponents. Returns a transportBundle the caller
// uses in runUntilShutdown for the main-select error channel and
// the graceful teardown path.
//
// grpcStarted = true ONLY when StartGRPCServer returns nil AND the
// server is actually listening. The transport probe (registered in
// bootstrap_readiness.go) captures this flag so a misconfigured
// gRPC plane (cert error, port in use, auth failure) shows up in
// /ready as a missing-transport failure rather than silently
// serving the HTTP API with a dead gRPC transport.
func startTransports(cfg *config.Config, c *appComponents) (*transportBundle, error) {
	bundle := transportBundle{
		errChan: make(chan error, 1),
	}

	// ── HTTP server ─────────────────────────────────────────────────
	bundle.router = newRouter(cfg, c.routerBundle(), c.modules.Registry)
	logRegisteredRoutesAtBoot(bundle.router)
	bundle.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           bundle.router,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("[SERVER] Velox master listening on %s", bundle.httpServer.Addr)

	go func() {
		var err error
		if cfg.Server.TLSCertFile != "" && cfg.Server.TLSKeyFile != "" {
			log.Printf("[SERVER] TLS enabled (cert: %s, key: %s)", cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
			err = bundle.httpServer.ListenAndServeTLS(cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
		} else {
			err = bundle.httpServer.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Printf("[SERVER] Listen error: %v", err)
		}
		bundle.errChan <- err
	}()

	// ── gRPC server ─────────────────────────────────────────────────
	if cfg.Server.GRPCPort > 0 {
		// PR-REMOVE-LIFECYCLE: j.Repository is the canonical jobs
		// surface; the old `j.Lifecycle.Jobs()` indirection is gone.
		jobsRepo := c.jobs.Repository
		if jobsRepo != nil && c.workers.CommandManager != nil {
			insecureDev := cfg.Runtime.GRPCAllowInsecureDev
			// PR-5 P0 fail-fast: refuse to start the master with
			// insecure gRPC outside the dev release channel.
			// Production / staging MUST use the TLS cert+key+CA triple.
			if insecureDev && cfg.Runtime.ReleaseChannel != "dev" {
				return nil, fmt.Errorf("[FAIL] PR-5 P0 guard: VELOX_GRPC_ALLOW_INSECURE_DEV=true on release channel =%q. Production / staging MUST use the TLS cert+key+CA triple. Set VELOX_RELEASE_CHANNEL=dev to confirm dev intent, or supply VELOX_GRPC_TLS_{CERT,KEY,CA}_FILE and unset VELOX_GRPC_ALLOW_INSECURE_DEV",
					cfg.Runtime.ReleaseChannel)
			}
			// RW-PROD-001 A5: hard-Reject plaintext gRPC.
			if err := enforceGRPCRequireTLS(cfg); err != nil {
				return nil, fmt.Errorf("gRPC require-TLS guard: %w", err)
			}
			if err := grpcserver.ValidateWorkerAllowlist(cfg.Workers.AllowedWorkers, insecureDev); err != nil {
				return nil, err
			}
			grpcHandler := grpcserver.NewHandler(
				c.workers.Registry, c.workers.CommandManager, jobsRepo,
				c.tasks.TaskRepository, c.tasks.AttemptRepository,
				c.assets.ArtifactSvc, c.persistence.SQLite,
				buildGRPCHandlerConfig(cfg, insecureDev),
			)
			// feat/task-report-ingestion: install the canonical
			// TaskReportIngestionService so handleTaskResult delegates
			// to the audit-mandated sequence (atomic close + artifact
			// register + Job roll-up).
			if c.tasks.IngestionSvc != nil {
				grpcHandler.SetIngestionSvc(c.tasks.IngestionSvc)
				log.Printf("[BOOTSTRAP] installed TaskReportIngestionService on gRPC handler (feat/task-report-ingestion)")
			}
			// Scorecard v1: wire the placement rejection counter and
			// worker resource sink onto the gRPC handler so placement
			// rejections and heartbeat resource counters land on the
			// Prometheus /metrics endpoint.
			if c.metricsCollector != nil {
				grpcHandler.SetResourceSink(c.metricsCollector)
				grpcHandler.SetPlacementRejectionSink(c.metricsCollector)
				log.Printf("[BOOTSTRAP] wired metrics collector sinks on gRPC handler (placement + worker resources)")
			}
			// Blocco 1 final-wire: wire the canonical capability
			// registry so the on-the-wire "artifact.commit.v1"
			// dispatch path can fail-closed via codes.PermissionDenied
			// before handleArtifactUploaded delegates to artifactSvc.
			grpcHandler.SetCapabilityRegistry(c.capabilityRegistry)
			log.Printf("[BOOTSTRAP] wired capability registry (artifact.commit.v1 gate) on gRPC handler")
			gs, lis, gerr := grpcserver.StartGRPCServer(
				cfg.Server.GRPCPort, grpcHandler,
				cfg.Server.GRPCTLSCertFile, cfg.Server.GRPCTLSKeyFile, cfg.Server.GRPCTLSCAFile,
			)
			if gerr != nil {
				// Blocco 1 (P0 #2, #3, #4): when GRPCPort > 0, a gRPC
				// startup failure is a misconfiguration the operator
				// MUST see loudly. Log-and-continue would mask the
				// failure for the lifetime of the pod; k8s/systemd need
				// the non-nil error so the pod can be restarted.
				return nil, fmt.Errorf("[SERVER] gRPC server failed to start on port %d: %w", cfg.Server.GRPCPort, gerr)
			}
			if gs != nil {
				bundle.grpcServer = &grpcServerWrapper{Server: gs, Listener: lis}
				bundle.grpcStarted = true
			}
		}
	}
	return &bundle, nil
}
