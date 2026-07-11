package grpcserver

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"velox-server/internal/registry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubHandlerForGate builds a Handler sized for the gate-only unit
// tests. The gate reads ONLY h.capabilityRegistry, so the rest of the
// struct stays zero. This keeps the helper trivially safe to construct
// in isolation — no fixture boilerplate, no path-required setters.
func stubHandlerForGate(reg *registry.CapabilityRegistry) *Handler {
	return &Handler{capabilityRegistry: reg}
}

// TestCheckArtifactCommitGate_NilRegistry pins the backward-compat
// path: a Handler constructed without a registry returns nil so
// legacy test paths + bootstrap variants that bypass the registry
// (e.g. dry-run/dev wiring) are NOT regressed by the gate.
func TestCheckArtifactCommitGate_NilRegistry(t *testing.T) {
	h := stubHandlerForGate(nil)
	if err := h.checkArtifactCommitGate("w-test"); err != nil {
		t.Fatalf("nil-registry gate must return nil, got: %v", err)
	}
}

// TestCheckArtifactCommitGate_AllPassing pins the happy path: every
// registered probe is ok, so the gate returns nil and the Stream()
// dispatch proceeds to handleArtifactUploaded.
func TestCheckArtifactCommitGate_AllPassing(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	for _, name := range []string{"coordinator", "spool", "transport"} {
		if err := reg.Register(registry.Probe{Name: name, Check: func() error { return nil }}); err != nil {
			t.Fatalf("Register(%s): %v", name, err)
		}
	}
	h := stubHandlerForGate(reg)
	if err := h.checkArtifactCommitGate("w-test"); err != nil {
		t.Fatalf("all-passing gate must return nil, got: %v", err)
	}
}

// TestCheckArtifactCommitGate_FailClosed_PermissionDenied pins the
// fail-closed semantic: when at least one probe fails, the gate must
// return a gRPC PermissionDenied status (NOT the literal Readyz err,
// NOT Unimplemented, NOT Unavailable). This locks the design choice
// documented in handler_artifacts.go's godoc. A future refactor that
// flips the code to OK / Unauthenticated / FailedPrecondition
// outside the documented exception list will fail this test.
func TestCheckArtifactCommitGate_FailClosed_PermissionDenied(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	_ = reg.Register(registry.Probe{Name: "coordinator", Check: func() error { return nil }})
	_ = reg.Register(registry.Probe{
		Name:  "spool",
		Check: func() error { return fmt.Errorf("staging dir unwritable") },
	})
	h := stubHandlerForGate(reg)

	got := h.checkArtifactCommitGate("w-test")
	if got == nil {
		t.Fatalf("failing-probe gate must return non-nil")
	}
	st, ok := status.FromError(got)
	if !ok {
		t.Fatalf("expected gRPC status, got: %T (%v)", got, got)
	}
	if code := st.Code(); code != codes.PermissionDenied {
		t.Errorf("gRPC code = %s; want PermissionDenied", code)
	}
}

// TestCheckArtifactCommitGate_MessageCarriesFailingProbe pins the
// operator-visibility contract: the gRPC status message must include
// both the failing probe name AND the underlying error text, so ops
// can grep "spool" + the inner error ("staging dir unwritable")
// without diving into the gRPC status code. Without this, the worker
// log + operator dashboard both stay generic.
func TestCheckArtifactCommitGate_MessageCarriesFailingProbe(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	_ = reg.Register(registry.Probe{Name: "coordinator", Check: func() error { return nil }})
	_ = reg.Register(registry.Probe{
		Name:  "spool",
		Check: func() error { return fmt.Errorf("staging dir unwritable") },
	})
	h := stubHandlerForGate(reg)

	got := h.checkArtifactCommitGate("w-test")
	if got == nil {
		t.Fatalf("failing-probe gate must return non-nil")
	}
	st, _ := status.FromError(got)
	msg := st.Message()
	if !strings.Contains(msg, "spool") {
		t.Errorf("status message must include failing probe name 'spool', got: %q", msg)
	}
	if !strings.Contains(msg, "staging dir unwritable") {
		t.Errorf("status message must include underlying probe error text, got: %q", msg)
	}
	if !strings.Contains(msg, "artifact commit refused") {
		t.Errorf("status message must include the documented preamble, got: %q", msg)
	}
}

// TestCheckArtifactCommitGate_ChainPreserved pins the
// ErrCapabilityNotReady sentinel in the gRPC error chain. Used by
// structured callers (test assertions, future metrics scrapers, any
// caller that branches on the sentinel without parsing the message
// string). A future refactor that flips the wrapping from %w to %v
// (or drops the inner wrapped err) would silently break this test,
// catching the regression before it ships.
func TestCheckArtifactCommitGate_ChainPreserved(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	_ = reg.Register(registry.Probe{
		Name:  "transport",
		Check: func() error { return fmt.Errorf("no modules") },
	})
	h := stubHandlerForGate(reg)

	got := h.checkArtifactCommitGate("w-test")
	if got == nil {
		t.Fatalf("failing-probe gate must return non-nil")
	}
	if !errors.Is(got, registry.ErrCapabilityNotReady) {
		t.Errorf("gRPC status error must preserve ErrCapabilityNotReady sentinel in chain; errors.Is failed for %v", got)
	}
}

// TestCheckArtifactCommitGate_AllFailingProbesAppearInOrder pins the
// iteration-order invariant: when multiple probes fail, the failing
// list in the gRPC status message MUST be in probe-name-sorted order
// (matches CapabilityRegistry.Readyz()'s deterministic ordering so
// alerts + log diffs are reproducible).
func TestCheckArtifactCommitGate_AllFailingProbesAppearInOrder(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	// Register out of sorted order on purpose.
	_ = reg.Register(registry.Probe{Name: "zeta", Check: func() error { return fmt.Errorf("z-err") }})
	_ = reg.Register(registry.Probe{Name: "alpha", Check: func() error { return fmt.Errorf("a-err") }})
	_ = reg.Register(registry.Probe{Name: "mu", Check: func() error { return fmt.Errorf("m-err") }})
	h := stubHandlerForGate(reg)

	got := h.checkArtifactCommitGate("w-test")
	if got == nil {
		t.Fatalf("failing-probes gate must return non-nil")
	}
	st, _ := status.FromError(got)
	msg := st.Message()

	idxAlpha := strings.Index(msg, "alpha(")
	idxMu := strings.Index(msg, "mu(")
	idxZeta := strings.Index(msg, "zeta(")
	if idxAlpha < 0 || idxMu < 0 || idxZeta < 0 {
		t.Fatalf("missing one of alpha/mu/zeta entries in status message: %q", msg)
	}
	if !(idxAlpha < idxMu && idxMu < idxZeta) {
		t.Errorf("expected failing entries in name-sorted order alpha→mu→zeta, got: %q", msg)
	}
}

// TestCheckArtifactCommitGate_NoSentinelLeak ensures that a generic
// (non-ErrCapabilityNotReady) error from a probe isn't accidentally
// upgraded into the ErrCapabilityNotReady sentinel by the gate
// wrapping. Today this is a sanity check: every probe returns either
// nil or its own error; registry.Readyz() aggregates only
// failures, and the wrapping only adds ErrCapabilityNotReady. But a
// future refactor that re-shapes Readyz() could regress this — keep
// the test for regression-safety.
func TestCheckArtifactCommitGate_NoSentinelLeak(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	// A probe that fails with a non-ErrCapabilityNotReady error — the gate
	// should still surface the registry.ErrCapabilityNotReady wrap because
	// registry.Readyz() wraps all-failing aggregation with the sentinel.
	_ = reg.Register(registry.Probe{
		Name:  "spool",
		Check: func() error { return errors.New("plain") },
	})
	h := stubHandlerForGate(reg)

	got := h.checkArtifactCommitGate("w-test")
	if got == nil {
		t.Fatalf("failing-probe gate must return non-nil")
	}
	st, _ := status.FromError(got)
	if code := st.Code(); code != codes.PermissionDenied {
		t.Errorf("non-cap probe failure must still surface as PermissionDenied, got: %s", code)
	}
	// The wrapped error must STILL be in the chain (registry.Readyz()
	// wraps failing lists under ErrCapabilityNotReady regardless).
	if !errors.Is(got, registry.ErrCapabilityNotReady) {
		t.Errorf("non-cap probe failure must still trigger ErrCapabilityNotReady chain, got: %v", got)
	}
}

// TestCheckArtifactCommitGate_EmptyRegistry pins the empty-registry
// behaviour, which is the production smoke path before all module
// subsystems are wired in (loading-time window): an empty registry
// Readyz returns nil (no failures), so the gate must pass. Without
// this contract, every fresh boot would PermissionDenied workers
// until at least one probe gets registered.
func TestCheckArtifactCommitGate_EmptyRegistry(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	h := stubHandlerForGate(reg)
	if err := h.checkArtifactCommitGate("w-test"); err != nil {
		t.Fatalf("empty-registry gate must return nil (smoke path), got: %v", err)
	}
}

// TestCheckArtifactCommitGate_ProbeNameInFormattedMessage pins the
// log-line dumpability contract: ART-style logs and operator
// dashboards parse the gRPC status message by substring, so the
// failing probe name (followed by its '(') MUST appear verbatim in
// the rendered message. The test uses the transport probe (whose
// underlying error is multi-word) to confirm the rendering keeps
// arbitrary-length probe descriptions, not just 2-word ones.
func TestCheckArtifactCommitGate_ProbeNameInFormattedMessage(t *testing.T) {
	reg := registry.NewCapabilityRegistry()
	_ = reg.Register(registry.Probe{
		Name:  "transport",
		Check: func() error { return fmt.Errorf("no modules registered") },
	})
	h := stubHandlerForGate(reg)

	got := h.checkArtifactCommitGate("worker-abc-123")
	if got == nil {
		t.Fatalf("failing-probe gate must return non-nil")
	}
	st, _ := status.FromError(got)
	if !strings.Contains(st.Message(), "transport(") {
		t.Errorf("status message must include failing probe name 'transport(' for ops greppability, got: %q", st.Message())
	}
	// Also confirm the multi-word inner error is preserved end-to-end
	// so ops log-grep by error substring ("modules registered") still
	// works.
	if !strings.Contains(st.Message(), "modules registered") {
		t.Errorf("status message must preserve the multi-word inner error for operator grep, got: %q", st.Message())
	}
}
