// Package queue / artifact_gate.go — PR 3 artifact success gate.
//
// SUCCEEDED is no longer reachable through the LifecycleService surfaced to
// HTTP / gRPC / workflow handlers. The ONLY legal path to SUCCEEDED goes
// through this ArtifactSuccessGate, which holds the secret JobRepository
// reference wired in by the bootstrap. The outbox dispatcher invokes this
// gate after the artifact service verifies a render artifact.
//
// Why a dedicated component?
//
//   - Spec §5: "Il completamento SUCCEEDED non deve essere pubblico per gli
//     handler. Deve essere accessibile solamente al finalizer artifact
//     attraverso una porta specifica."
//   - Encourages correct sequencing: artifact verification → SUCCEEDED →
//     delivery targets, never the reverse.
//   - Brings the secret-port reference under a typed constructor whose
//     only call site is the bootstrap composition root.
//
// Concurrency: MarkSucceeded is delegated straight to JobRepository.PR3MarkSucceeded
// which serialises on the SQLite write lock. No extra locking needed here.
package queue

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/store"
)

// ErrSucceededForbidden is returned by anything that was NOT the gate when
// it tries to reach the SUCCEEDED transition. The constant exists so the
// bootstrap can grep for it during composition auditing.
var ErrSucceededForbidden = errors.New("queue: SUCCEEDED transition is reserved for the artifact success gate")

// ArtifactSuccessGate is the only component authorised to flip a job to
// SUCCEEDED. Constructed exactly once in bootstrap with the secret
// store.JobRepository reference, then never handed to handlers.
type ArtifactSuccessGate struct {
	repo store.JobRepository
	// capAuditFunc is optional; bootstrap may wire a *log.Printf for
	// observability (every transition emitted by the gate is a real world
	// signal worth recording). Leaving nil is fine — the gate still works.
	capAuditFunc func(jobID, artifactID string, revision int)
}

// NewArtifactSuccessGate constructs the gate. The repo reference is the
// "private port" the bootstrap composition wires to the artifact finalizer
// out-of-band. Calling this constructor outside bootstrap is a programming
// error — it is exposed only so the test suite can substitute fakes.
func NewArtifactSuccessGate(repo store.JobRepository) *ArtifactSuccessGate {
	if repo == nil {
		// Hard fail: a nil repo means the gate cannot authorise any
		// SUCCEEDED transitions, so constructing it would be a foot-gun.
		panic("queue: ArtifactSuccessGate requires a non-nil JobRepository")
	}
	return &ArtifactSuccessGate{repo: repo}
}

// SetAuditHook wires a logging callback for every emitted SUCCEEDED
// transition. Used by bootstrap for visibility.
func (g *ArtifactSuccessGate) SetAuditHook(fn func(jobID, artifactID string, revision int)) {
	g.capAuditFunc = fn
}

// MarkSucceeded promotes jobID to SUCCEEDED only if the artifact gate
// accepts it. This is the canonical entry point; it MUST be the only code
// path that calls repo.PR3MarkSucceeded.
//
// The event_type suffix "succeeded_via_artifact_gate" is logged in the
// job_event row so audits can grep for any SUCCEEDED that did NOT come
// from the gate (defence-in-depth).
func (g *ArtifactSuccessGate) MarkSucceeded(ctx context.Context, jobID, artifactID, workerID string, attemptID, revision int) error {
	if jobID == "" {
		return fmt.Errorf("ArtifactSuccessGate.MarkSucceeded: empty jobID")
	}
	if artifactID == "" {
		return fmt.Errorf("ArtifactSuccessGate.MarkSucceeded: empty artifactID (SUCCEEDED must be tied to a verified artifact)")
	}
	if g.repo == nil {
		return ErrSucceededForbidden
	}

	err := g.repo.PR3MarkSucceeded(ctx, store.MarkSucceededCommand{
		JobID:            jobID,
		ArtifactID:       artifactID,
		AttempterID:      attemptID,
		WorkerID:         workerID,
		ExpectedRevision: revision,
	})
	if err != nil {
		return err
	}
	if g.capAuditFunc != nil {
		g.capAuditFunc(jobID, artifactID, revision)
	}
	return nil
}

// CanMarkSucceeded reports whether the gate currently has authority to
// promote jobs (used by health checks / startup assertions).
func (g *ArtifactSuccessGate) CanMarkSucceeded() bool {
	return g != nil && g.repo != nil
}
