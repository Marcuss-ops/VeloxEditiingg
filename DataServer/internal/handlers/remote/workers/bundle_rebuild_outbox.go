// Package workers — outbox-side handler for async bundle rebuilds.
//
// The synchronous POST /install_worker/force_regenerate_zip?wait=1 path
// runs the build inline and returns 200 OK with the new bundle hash.
// The async wait=0 path used to do `go func() { _, _ = run() }()` and
// ACK 202 immediately — fire-and-forget. If the master pod died
// between ACK and execution there was no durable record of the
// rebuild request; the operator saw an "accepted" log entry and a
// stale bundle on disk.
//
// The transactional outbox version durably enqueues a
// WORKER_BUNDLE_REBUILD_REQUESTED event BEFORE the 202 ACK. The
// dispatcher races the work to the registered handler. If the master
// pod dies after the ACK and before dispatch, the outbox row is
// re-claimable on next boot (status=PENDING, locked_until in the past
// OR not locked at all). The handler is naturally idempotent —
// velox-bundler overwrites the existing zips in place — so retries
// are safe.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"velox-server/internal/outbox"
)

// BundleRebuildRequestedEventType is the canonical outbox.EventType
// emitted by the async POST /install_worker/force_regenerate_zip
// handler. Must match BundleRebuildHandler.EventType().
//
// NOT included in outbox.KnownEventTypes: this handler is wired at
// the composition root (cmd/server/bootstrap_workers.go) rather than
// in outbox.ProductionRegistry(). Reason: workers package is the
// owner of the subprocess contract — keeping the registration
// adjacent to the producer avoids dragging the velox-bundler binary
// resolution + repoRoot logic into the outbox package, which would
// import-cycle against the workers package.
//
// The completeness invariant asserted by
// internal/outbox/completeness_test.go deliberately scopes to
// globally-registered handlers (today: JOB_FAILED); subsystem
// handlers like this one are validated by the workers-package
// tests (see bundle_rebuild_outbox_test.go).
const BundleRebuildRequestedEventType = "WORKER_BUNDLE_REBUILD_REQUESTED"

// BundleRebuildHandler is the dispatched Handler for the async
// bundle rebuild event type. It runs the velox-bundler subprocess
// with --source + --output, identical to the synchronous path in
// bundle_rebuild.go, but here failures map to typed HandlerError
// values so the dispatcher's transient-vs-permanent decision
// applies (transient → retry up to MaxAttempts; permanent → FAILED).
//
// BINARY RESOLUTION:
// BundleBinaryPath is field-injectable so tests can substitute a
// stub binary; production defaults to getBundlerPath (the
// repo-root-relative `DataServer/bin/velox-bundler`).
//
// IDEMPOTENCY:
// velox-bundler is naturally idempotent — it overwrites the existing
// zips in h.bundleDir. Dispatch retries (outbox transient retries up
// to MaxAttempts) are safe.
type BundleRebuildHandler struct {
	// BundleBinaryPath resolves the absolute path to the
	// velox-bundler binary given a repoRoot. Defaults to the
	// production layout (repoRoot/DataServer/bin/velox-bundler);
	// tests inject a stub here.
	BundleBinaryPath func(repoRoot string) string
}

// NewBundleRebuildHandler returns a BundleRebuildHandler with the
// production default for BundleBinaryPath. Tests override the field
// after construction.
func NewBundleRebuildHandler() *BundleRebuildHandler {
	return &BundleRebuildHandler{
		BundleBinaryPath: getBundlerPath,
	}
}

// EventType satisfies outbox.Handler.
func (h *BundleRebuildHandler) EventType() string {
	return BundleRebuildRequestedEventType
}

// Handle runs the subprocess. The expected payload is encoded by
// the producer as a tiny JSON object created in bundle_rebuild.go —
// see the encodeBundleRebuildPayload helper below.
//
// Error mapping (canonical, see outbox.HandlerError):
//   - payload decode failure          → Permanent (malformed payload)
//   - velox-bundler binary missing    → Permanent (won't heal without
//     operator intervention; the
//     binary path was validated at
//     enqueue-time, so a missing
//     binary at dispatch-time is
//     an operator-fixable hazard)
//   - subprocess exit non-zero + output → Transient (could be a
//     transient fs lock or a
//     memory-pressure condition;
//     retry bounds behaviour to
//     MaxAttempts before FAILED)
//
// Returning nil marks the event PROCESSED; the dispatcher never
// re-runs it.
func (h *BundleRebuildHandler) Handle(ctx context.Context, e outbox.Event) error {
	var p bundleRebuildPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return outbox.Permanent(fmt.Errorf("bundle rebuild payload decode: %w", err))
	}
	if p.RepoRoot == "" || p.BundleDir == "" {
		return outbox.Permanent(fmt.Errorf("bundle rebuild payload missing repo_root/bundle_dir"))
	}
	binaryPath := h.resolveBinary(p.RepoRoot)
	if _, err := os.Stat(binaryPath); err != nil {
		return outbox.Permanent(fmt.Errorf("velox-bundler binary missing at dispatch time: %w", err))
	}

	// Subprocess is naturally idempotent (overwrites zips).
	cmd := exec.CommandContext(ctx, binaryPath,
		"--source", p.RepoRoot, "--output", p.BundleDir)
	cmd.Dir = filepath.Join(p.RepoRoot, "DataServer")

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[OUTBOX] bundle rebuild failed event_id=%s binary=%s err=%v | %s",
			e.EventID, binaryPath, err, strings.TrimSpace(string(out)))
		return outbox.Transient(fmt.Errorf("velox-bundler exit: %w", err))
	}
	log.Printf("[OUTBOX] bundle rebuild completed event_id=%s out-path=%s combo=%s",
		e.EventID, p.BundleDir, strings.TrimSpace(string(out)))
	return nil
}

// resolveBinary is the gatekeeper for the field-overridable
// BundleBinaryPath. Falls back to getBundlerPath if no override was
// provided.
func (h *BundleRebuildHandler) resolveBinary(repoRoot string) string {
	if h.BundleBinaryPath != nil {
		return h.BundleBinaryPath(repoRoot)
	}
	return getBundlerPath(repoRoot)
}

// bundleRebuildPayload is the JSON shape the producer writes into
// outbox.Payload. Public so the harness in bundle_rebuild.go can
// json.Marshal it.
type bundleRebuildPayload struct {
	RepoRoot  string `json:"repo_root"`
	BundleDir string `json:"bundle_dir"`
	// BinaryPath is the operator-facing log surface only — the
	// handler re-resolves via h.BundleBinaryPath(repoRoot). Stored
	// here so post-mortem operators see the original path the
	// client saw at HTTP-call time.
	BinaryPath string `json:"binary_path,omitempty"`
}

// encodeBundleRebuildPayload is the canonical producer-side encoder.
// Exposed so bundle_rebuild.go can call it rather than re-marshalling
// inline. The marshalled bytes are the outbox.Payload.
func encodeBundleRebuildPayload(repoRoot, bundleDir, binary string) ([]byte, error) {
	if repoRoot == "" || bundleDir == "" {
		return nil, errors.New("encodeBundleRebuildPayload: empty repoRoot/bundleDir")
	}
	p := bundleRebuildPayload{
		RepoRoot:   repoRoot,
		BundleDir:  bundleDir,
		BinaryPath: binary,
	}
	return json.Marshal(p)
}

// RegisterBundleRebuildOutboxHandler is the canonical wiring site
// for the worker-side outbox handler.
//
// At process boot, the package init() function below registers a
// factory via outbox.RegisterHandlerFactory so the canonical
// *outbox.Registry (returned by outbox.ProductionRegistry()) picks
// up the handler automatically — no composition-root coordination
// required. Future subsystem packages can copy this pattern.
//
// For unit tests that build their own *outbox.Registry (via
// outbox.NewRegistry()), call RegisterBundleRebuildOutboxHandler
// directly to wire the handler against the test-local registry; the
// factory registration has no effect on registries built without
// the production cache.
func RegisterBundleRebuildOutboxHandler(reg *outbox.Registry) {
	if reg == nil {
		panic("workers.RegisterBundleRebuildOutboxHandler: nil registry")
	}
	reg.MustRegister(NewBundleRebuildHandler())
}

// init wires the subsystem factory so the canonical
// outbox.ProductionRegistry() includes this package's handler at
// first-call time. Idempotent against duplicate factory registration
// — the registry's duplicate-panic protects the caller; factory
// registration itself appends to a list (no dedup needed because each
// factory closure creates one MustRegister call).
func init() {
	outbox.RegisterHandlerFactory(RegisterBundleRebuildOutboxHandler)
}
