// Package grpcserver — worker authorizer for gRPC allowlist enforcement.
//
// The WorkerAuthorizer gate-checks every inbound gRPC worker stream BEFORE
// credential validation and session creation. The allowlist is parsed once
// from VELOX_ALLOWED_WORKERS (comma-separated) at handler construction time
// and never re-read at runtime — a config change requires a master restart.
package grpcserver

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
)

// WorkerAuthorizer decides whether a worker is allowed to connect.
type WorkerAuthorizer interface {
	IsAllowed(workerID string) bool
}

// allowlistAuthorizer implements WorkerAuthorizer with a static parsed set.
type allowlistAuthorizer struct {
	workers    map[string]bool // parsed set of allowed worker IDs
	emptyList  bool            // true when VELOX_ALLOWED_WORKERS was empty/whitespace
	insecure   bool            // true when VELOX_GRPC_ALLOW_INSECURE_DEV=true
	loggedWarn atomic.Bool     // prevent duplicate warning logs (goroutine-safe)
}

// NewAllowlistAuthorizer parses the VELOX_ALLOWED_WORKERS string and returns
// an authorizer. insecureDev should be true only when the master is also
// running with VELOX_GRPC_ALLOW_INSECURE_DEV=true (double-consent).
//
// Rules:
//   - allowlist non-empty → worker MUST be in the set
//   - allowlist empty + insecure dev → allowed (with one-time warning)
//   - allowlist empty + production → IsAllowed returns false (bootstrap
//     must have already fail-fast rejected this configuration)
func NewAllowlistAuthorizer(allowedWorkersCSV string, insecureDev bool) WorkerAuthorizer {
	a := &allowlistAuthorizer{
		workers:  make(map[string]bool),
		insecure: insecureDev,
	}

	trimmed := strings.TrimSpace(allowedWorkersCSV)
	if trimmed == "" || trimmed == "*" {
		a.emptyList = true
		return a
	}

	for _, id := range strings.Split(trimmed, ",") {
		id = strings.TrimSpace(id)
		if id != "" {
			a.workers[id] = true
		}
	}
	if len(a.workers) == 0 {
		a.emptyList = true
	}

	return a
}

// IsAllowed returns true if workerID may connect.
func (a *allowlistAuthorizer) IsAllowed(workerID string) bool {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return false
	}

	// Non-empty allowlist: exact match required.
	if !a.emptyList {
		return a.workers[workerID]
	}

	// Empty allowlist in dev: warn once, then allow.
	if a.insecure {
		if !a.loggedWarn.Swap(true) {
			log.Printf("[GRPC][AUTHZ] VELOX_ALLOWED_WORKERS is empty — allowing all workers because VELOX_GRPC_ALLOW_INSECURE_DEV=true (NEVER do this in production)")
		}
		return true
	}

	// Empty allowlist in production: deny. Bootstrap should have caught this
	// before the gRPC server started, but this is defense in depth.
	log.Printf("[GRPC][AUTHZ] VELOX_ALLOWED_WORKERS is empty in production mode — denying worker %q", workerID)
	return false
}

// ValidateWorkerAllowlist is called at bootstrap time to fail-fast when the
// allowlist is empty in production mode. Returns nil if the configuration is
// acceptable, or an error describing the problem.
func ValidateWorkerAllowlist(allowedWorkersCSV string, insecureDev bool) error {
	trimmed := strings.TrimSpace(allowedWorkersCSV)
	if trimmed != "" && trimmed != "*" {
		return nil
	}

	if insecureDev {
		return nil // dev mode: empty allowlist is a warning, not a fatal error
	}

	return fmt.Errorf(
		"VELOX_ALLOWED_WORKERS is empty (or set to \"*\") in production mode. " +
			"Set VELOX_GRPC_ALLOW_INSECURE_DEV=true for local development, " +
			"or configure at least one worker ID (comma-separated). " +
			"An empty allowlist in production would silently admit any worker.")
}
