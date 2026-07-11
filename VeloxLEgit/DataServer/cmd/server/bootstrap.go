package main

// bootstrap.go — slim composition root (Blocco 4 step #2).
//
// History: this file used to host ~939 lines that mixed composition
// (build* and wirePostBuild), transport (HTTP+gRPC start/stop),
// readiness (capability registry + /ready checks), the supervisor
// registry, the runServer orchestration, and the test-only
// buildTestDeps helper. After the Blocco 4 split:
//   - bootstrap_composition.go owns domain composition
//     (buildAppComponents, wirePostBuild, buildSupervisor) +
//     the appComponents struct.
//   - bootstrap_transport.go owns HTTP+gRPC listener lifecycle
//     (transportBundle, startTransports).
//   - bootstrap_readiness.go owns capability registry + /ready
//     checks (registerReadinessChecks).
//   - bootstrap_test_helpers_test.go owns the test-only
//     testServerDeps + buildTestDeps.
//
// This file is now: env helpers + slim orchestration only. Target
// length ≤200 lines. New top-level concerns grow in their own file;
// do NOT re-bloat this one.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"velox-server/internal/config"
)

// requireLiveWorkersEnabled is the canonical gate for the A8 opt-in.
// Encapsulated as a package-level helper so the readiness check call
// site stays readable AND so a future operator-mode (e.g. `velox fleet
// live-only`) can flip the same flag from a non-env source without
// rewriting the closure above.
func requireLiveWorkersEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("VELOX_REQUIRE_LIVE_WORKERS")), "true")
}

// criticalRetryConfigFromEnv reads VELOX_CRITICAL_MAX_RETRIES (int;
// 0 = infinite for ClassCritical — legacy behaviour) and
// VELOX_CRITICAL_FAIL_AFTER (int; log-WARN threshold unrelated to
// the bounded-retry choice — set independently so operators tuning
// MAX_RETRIES do not lose operational visibility on loops that
// remain infinite). Both default to a sensible legacy value
// (MAX_RETRIES=0, FAIL_AFTER=10) so deployments without the env vars
// keep the pre-Blocco-1 behaviour.
//
// Verdetto P0 #4 (Blocco 1): a positive MAX_RETRIES converts the
// supervisor's ClassCritical failure mode from "infinite backoff +
// log-WARN" to "bounded retries + cancel supCtx + return error to
// runServer". Operators opting into the bounded modal then propagate
// to k8s via the existing fail-loud path.
func criticalRetryConfigFromEnv() (maxRetries int, failAfter int) {
	const defaultMaxRetries = 0
	const defaultFailAfter = 10
	maxRetries = defaultMaxRetries
	failAfter = defaultFailAfter

	if v := strings.TrimSpace(os.Getenv("VELOX_CRITICAL_MAX_RETRIES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxRetries = n
		} else if err != nil {
			log.Printf("[SUPERVISOR] VELOX_CRITICAL_MAX_RETRIES=%q is not a valid int; using default %d", v, defaultMaxRetries)
		} else if n < 0 {
			log.Printf("[SUPERVISOR] VELOX_CRITICAL_MAX_RETRIES=%d is negative; clamping to 0 (infinite)", n)
			maxRetries = 0
		}
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_CRITICAL_FAIL_AFTER")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			failAfter = n
		} else if err != nil {
			log.Printf("[SUPERVISOR] VELOX_CRITICAL_FAIL_AFTER=%q is not a valid int; using default %d", v, defaultFailAfter)
		}
	}
	return maxRetries, failAfter
}

// runServer is the slim composition root. It delegates to:
//   - buildAppComponents      (cmd/server/bootstrap_composition.go)
//   - startTransports         (cmd/server/bootstrap_transport.go)
//   - registerReadinessChecks (cmd/server/bootstrap_readiness.go)
//   - runUntilShutdown        (here; signal handling + teardown)
//
// The orchestration here stays linear and readable: each helper
// owns its subsystem and returns the typed surface the next
// helper needs. New top-level concerns grow in their own file;
// DO NOT re-bloat this function.
func runServer(cfg *config.Config) error {
	if err := runDataLayerAudit(cfg); err != nil {
		return err
	}

	components, err := buildAppComponents(cfg)
	if err != nil {
		return err
	}
	defer components.close()

	transport, err := startTransports(cfg, components)
	if err != nil {
		return err
	}

	registerReadinessChecks(components, transport)

	log.Printf("[BOOTSTRAP] Bootstrap complete — %d modules, %d background runners",
		components.modules.Registry.Len(), components.supervisor.Len())
	if components.health != nil {
		components.health.MarkReady()
	}

	return runUntilShutdown(components, transport)
}

// runUntilShutdown supervises HTTP, gRPC and the background supervisor
// until SIGINT/SIGTERM OR an unrecoverable error fires. The graceful
// teardown goes through transport.shutdown() which closes both
// listeners with bounded timeouts.
//
// The four-arm select encodes the "fail-loud, never silent" contract
// from Blocco 1 verdetti P0 #4 + P0 #5:
//   * HTTP listener error → return (k8s/systemd restart)
//   * supervisor critical error → wrap + return (k8s/systemd restart)
//   * supervisor done with no signal → "unexpected exit" error
//     (catches the false-success path where a critical runner died
//     without ctx-cancellation)
//   * SIGINT/SIGTERM → graceful shutdown
func runUntilShutdown(c *appComponents, t *transportBundle) error {
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	supervisorDone := make(chan struct{})
	// Blocco 1 (P0 #4): supervisorErrCh carries the supervisor's
	// returned error (typically a ClassCritical exhaustion) into
	// runUntilShutdown's main select, so a dead supervisor fails
	// loudly rather than masking the failure behind a passing
	// HTTP listener.
	supervisorErrCh := make(chan error, 1)
	go func() {
		defer close(supervisorDone)
		if supErr := c.supervisor.Run(bgCtx); supErr != nil {
			log.Printf("[SERVER] supervisor returned critical error: %v", supErr)
			// Buffered chan, capacity 1; non-blocking send so a
			// double-failure scenario does not deadlock the
			// supervisor goroutine on shutdown.
			select {
			case supervisorErrCh <- supErr:
			default:
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-t.errChan:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
	case supErr := <-supervisorErrCh:
		// Blocco 1 (P0 #4): supervisor returned a fatal error
		// (most commonly a ClassCritical runner exhausted retry
		// budget). Surface it as the return value so the wrapping
		// caller (main) can log + exit non-zero, matching the
		// k8s/systemd fail-loud contract.
		return fmt.Errorf("[SERVER] supervisor reported fatal error: %w", supErr)
	case <-supervisorDone:
		// Verdetto P0 #5 (Blocco 2): the supervisor goroutine
		// exited WITHOUT sending an error to supervisorErrCh
		// and without a SIGINT/SIGTERM being observed. The
		// only legitimate way this fires is supervisor.Run
		// returning nil (graceful shutdown via parent ctx
		// cancel) which propagates to bgCancel, which then
		// causes every supervised runner to exit cleanly. In
		// that case the quit signal is typically received
		// FIRST; if we reach this branch without a quit, the
		// supervisor has terminated with a nil return under
		// a live bgCtx — a false-success path that means a
		// critical runner has silently died. Surface it as
		// an error so k8s/systemd restarts the pod.
		return errors.New("background supervisor exited unexpectedly")
	case <-quit:
		log.Println("[SERVER] Shutdown signal received, shutting down gracefully...")
	}

	// ── Graceful teardown ───────────────────────────────────────────
	bgCancel()
	log.Println("[SERVER] Background goroutines cancelling — waiting for them to exit...")

	select {
	case <-supervisorDone:
		log.Println("[SERVER] Background goroutines stopped cleanly")
	case <-time.After(15 * time.Second):
		log.Printf("[SERVER] background shutdown timed out after 15s — proceeding with teardown anyway")
	}

	t.shutdown()
	log.Println("[SERVER] Server stopped")
	return nil
}

