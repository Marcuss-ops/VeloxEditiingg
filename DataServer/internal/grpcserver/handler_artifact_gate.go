// Package grpcserver / handler_artifact_gate.go
//
// The on-the-wire "artifact.commit.v1" readiness gate: checkArtifactCommitGate
// plus the fail-closed artCommitGateError wrapper. Extracted from
// handler_artifacts.go (split per responsabilità) so the gate's
// gRPC-status contract lives apart from the upload handler.
package grpcserver

import (
	"fmt"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkArtifactCommitGate enforces the on-the-wire "artifact.commit.v1"
// readiness contract. It is invoked by Handler.Stream() inside the
// *pb.WorkerToMasterEnvelope_ArtifactUploaded switch case BEFORE
// handleArtifactUploaded is delegated to, so a master whose
// coordinator / spool / transport subsystem is unhealthy can never
// accept a commit.
//
// Fail-closed semantic (documented):
//
//   - codes.PermissionDenied when the registry reports at least one
//     capability not ready. The method exists and the auth layer
//     passed, but the master itself lacks the prerequisites to
//     safely run the commit. PermissionDenied is the canonical
//     "prerequisites not satisfied" gRPC code and matches the
//     pattern already established by the VELOX_ALLOWED_WORKERS
//     gate in handler.go (codes.PermissionDenied from the
//     WorkerAuthorizer path).
//   - codes.Unimplemented is REJECTED for this case: the method
//     exists, and returning Unimplemented would tell the worker to
//     drop the capability entry from its dispatch table — wrong
//     behaviour when the failure is transient/misconfiguration.
//   - codes.Unavailable is REJECTED: the spool+transport are
//     readable as "transient" which would let the worker auto-retry
//     in tight loop against an unhealthy master.
//   - codes.FailedPrecondition is REJECTED: it overlaps with the
//     protocol-version handshake's existing use of the same code,
//     so an operator reading gRPC logs could not distinguish
//     "wrong proto" from "spool broken".
//
// Backward-compat: if h.capabilityRegistry is nil (legacy test paths,
// partial-wiring variants), the gate returns nil and commit proceeds.
// This matches the nil-safe pattern of SetResourceSink /
// SetPlacementRejectionSink and the existing "NIL-safe" comments on
// those setters.
//
// Returning this custom error terminates the Stream() loop with the
// gRPC code mapped through GRPCStatus(). The worker treats
// PermissionDenied as non-retryable (its own policy) so the master
// stays off the worker's hot-path until an operator runs a /ready
// check + flips the gate back to healthy via a master restart.
//
// Why a custom wrapper type instead of status.Errorf:
//
//	google.golang.org/grpc/status.Errorf delegates to fmt.Sprintf,
//	NOT fmt.Errorf, so the %w directive is rendered as literal text
//	("artifact commit refused: %!w(MISSING)") instead of wrapping.
//	To preserve the registry.ErrCapabilityNotReady sentinel in the
//	errors.Is chain AND surface the right gRPC code, we need BOTH a
//	proper Unwrap() AND a GRPCStatus() interface implementation.
//	status.Errorf alone gives us neither. artCommitGateError below
//	is the minimal correct shape.
func (h *Handler) checkArtifactCommitGate(workerID string) error {
	if h.capabilityRegistry == nil {
		return nil // Backward-compat — see godoc.
	}
	if err := h.capabilityRegistry.Readyz(); err != nil {
		log.Printf("[GRPC] artifact.commit.v1 from worker %s refused: %v", workerID, err)
		return &artCommitGateError{
			inner: err,
		}
	}
	return nil
}

// artCommitGateError carries the artifact-commit fail-closed error in
// a form that satisfies three callers at once:
//
//  1. The gRPC server framework: implements GRPCStatus() so the
//     framework serializes it as codes.PermissionDenied with the
//     wrapped message — NOT codes.Unknown (which is what a bare
//     fmt.Errorf wrap would yield).
//  2. Structured test assertions: Unwrap() exposes the inner
//     registry.Readyz() error, so errors.Is(returned,
//     registry.ErrCapabilityNotReady) returns true.
//  3. Operator dashboards: Error() renders "<preamble>: <inner>"
//     so logs and gRPC status messages both carry the failing probe
//     name + detail for ops greppability.
//
// The struct remains unexported on purpose — Handler.checkArtifactCommitGate
// is the single constructor; nothing else in the codebase should be
// returning this type.
type artCommitGateError struct {
	inner error
}

func (e *artCommitGateError) Error() string {
	return fmt.Sprintf("artifact commit refused: capability not ready: %v", e.inner)
}

func (e *artCommitGateError) Unwrap() error {
	return e.inner
}

// GRPCStatus implements the gRPC status-erreur contract so the
// framework serializes codes.PermissionDenied on the wire without an
// extra hop through status.Error.
func (e *artCommitGateError) GRPCStatus() *status.Status {
	return status.New(codes.PermissionDenied, e.Error())
}

// Sanity check at compile time: artCommitGateError must satisfy the
// grpc-Status-erreur contract that the gRPC framework inspects at
// runtime. If a future refactor drops GRPCStatus() (or renames it),
// the framework silently downgrades to codes.Unknown — and this
// compile-time guard prevents that regression from landing.
var _ interface {
	Error() string
	GRPCStatus() *status.Status
} = (*artCommitGateError)(nil)
