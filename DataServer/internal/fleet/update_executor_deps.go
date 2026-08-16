// Package fleet — Step 9/15 UpdateExecutor dependency surface.
//
// This file owns the narrow consumer-defined interfaces the
// UpdateExecutor depends on. Each interface is intentionally
// tiny — Go convention: consumer-side interfaces are smaller
// than producer-side. Production wires live implementations
// (real SSH client, real Docker CLI, real cosign verifier, real
// Drive verifier, real registry, real deployment_records repo);
// tests wire in-process stubs that exercise the 12+ failure +
// happy-path scenarios without standing up real infra.
//
// Each interface's doc comment lists the FAILURE MODES the
// interface contract permits. The executor treats non-nil
// errors from these interfaces as "forward failed; run
// rollback" unless the executor has reached an explicit
// irreversible terminal (e.g. registry empty / unregistered
// worker) where forward fails WITHOUT rollback (no point
// rolling back a forward that never started).
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/workers"
)

// UpdatePayload is the Operation.Payload schema for the
// `update` kind. Decoded by the executor at the start of
// Execute. Marshalling it back is unnecessary (the audit ledger
// retains the raw op.Payload bytes for replay).
//
// both previous_digest and target_digest MUST be sha256-pinned
// (validated via deploy.ValidateImageRef in the executor's
// input-validation phase). The schema accepts either:
//
//   - Both set: caller supplies the snapshot explicitly.
//     Snapshot-from-DB is skipped (useful for manual operator
//     rollbacks against a known prior digest).
//   - Only target_digest set: snapshot is read from
//     deployment_records via GetLatestDeploymentForWorker —
//     canonical production path.
//
// Empty target_digest is rejected as ErrEmptyImageRef. Empty
// previous_digest triggers the snapshot-from-DB path; if THAT
// also surfaces ErrDeploymentNotFound, production adopts the authenticated
// runtime digest into a terminal baseline row (no rollout pipeline).
type UpdatePayload struct {
	TargetDigest   string `json:"target_digest"`
	PreviousDigest string `json:"previous_digest,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

// BackendSSHClient is the typed surface for "execute this
// command on the worker". Production wires a real SSH-backed
// implementation (command over worker.env SSH key); tests
// wire an in-process recorder that returns canned output.
//
// Failures (non-nil err) trigger rollback. Output is used for
// logging only — the executor does not act on stdout/stderr
// content beyond what the caller already parsed.
type BackendSSHClient interface {
	Run(ctx context.Context, workerID string, command string) (output string, err error)
}

// BackendDockerClient owns the worker's container-lifecycle
// surface. Each method corresponds to one step in the forward
// or rollback pipeline:
//
//	ActivateImage — validate, pull, atomically update worker.env and restart
//	  the canonical velox-worker.service on the worker.
//	ContainerRunning — sanity check after restart; returns
//	  (true, nil) when the container reports running.
//
// All three MAY return ErrContainerUnhealthy (sentinel exposed
// from this package) to signal "the container is broken but
// the docker daemon is responsive". The executor maps this
// into the rollback path.
type BackendDockerClient interface {
	ActivateImage(ctx context.Context, workerID, imageRef string) (output string, err error)
	ContainerRunning(ctx context.Context, workerID string) (bool, error)
}

// BackendSmokeRunner executes the Level D smoke test on the
// freshly-updated worker. Returns the artifact_id produced by
// the smoke so the executor can hand it to the Drive verifier.
//
// The Level D smoke is a full end-to-end render + verify (per
// the rollout's "Level D" surface in the user spec). Future
// Steps replace this noop-shaped stub with a real shell-out
// to `submit-canary-remote.sh` + a JSON artefact landing in
// Drive. For Step 9, the canonical test wiring is a stub.
type BackendSmokeRunner interface {
	RunLevelD(ctx context.Context, workerID string) (artifactID string, err error)
}

// BackendDriveVerifier checks that a Drive-resident smoke
// artifact is reachable from the master. The verifier pulls
// file metadata + a sample (HEAD) and confirms the size
// matches the expectedBytes argument; non-zero expectedBytes
// catches "upload silently failed but Drive returned success"
// regressions.
//
// Returns ErrDriveDeliveryMissing if the file is not
// reachable; ErrDriveDeliverySize if the size does not match.
// Both trigger rollback.
type BackendDriveVerifier interface {
	VerifyDelivery(ctx context.Context, driveFileID string, expectedBytes int64) error
}

// BackendRegistryGater is the typed surface for in-process
// registry lookups. The worker must be registered BEFORE the
// update begins; the executor rejects otherwise.
//
//   - IsActiveJobsZero polls Worker.Metrics["active_tasks"]
//     (the canonical active-job indicator in registry_health.go).
//   - IsDrained reports whether the registry read model reflects
//     drain=true. The executor requires BOTH conditions before
//     DEPLOYING: a SetDrainMode call that returns nil without the
//     registry entering DRAINING must never advance the rollout.
//
// WaitForIdle is exposed so tests can drive the timeout-bounded
// polling without standing up a clock; production calls it with
// a 5min budget.
type BackendRegistryGater interface {
	GetWorker(ctx context.Context, workerID string) (*workers.Worker, error)
	IsActiveJobsZero(ctx context.Context, workerID string) bool
	IsDrained(ctx context.Context, workerID string) bool
	SetDrainMode(ctx context.Context, workerID string, drain bool) error
}

// BackendRuntimeSnapshotReader returns the runtime identity captured from
// the worker's authenticated control session. It is used for safe ledger
// bootstrap and the no-op fast path; it must not be implemented from Docker,
// worker configuration, or operator input.
type BackendRuntimeSnapshotReader interface {
	GetAuthenticatedRuntimeSnapshot(ctx context.Context, workerID string) (*store.WorkerRuntimeSnapshot, error)
}

// BackendRuntimePreflight verifies the immutable, canonical worker runtime
// before an update owns the worker's drain transition. The check is
// deliberately read-only: it may inspect the worker's systemd/Compose/
// container/health contract, but it must not repair or mutate the host.
// Recovery is a separate operation and must be audited independently.
type BackendRuntimePreflight interface {
	Check(ctx context.Context, workerID string) error
}

// RealRegistryUpdateGater adapts the production worker registry to the
// UpdateExecutor registry surface. The executor owns the drain transition;
// this adapter keeps that mutation on the same registry used by placement.
type RealRegistryUpdateGater struct {
	Reg   *workers.Registry
	Store *store.SQLiteStore
}

func (g *RealRegistryUpdateGater) GetWorker(ctx context.Context, workerID string) (*workers.Worker, error) {
	if g == nil || g.Reg == nil {
		return nil, errors.New("worker registry not wired")
	}
	return g.Reg.GetWorker(ctx, workerID), nil
}

func (g *RealRegistryUpdateGater) IsActiveJobsZero(ctx context.Context, workerID string) bool {
	info, err := g.GetWorker(ctx, workerID)
	if err != nil || info == nil {
		return false
	}
	value, ok := info.Metrics["active_tasks"]
	if !ok || value == nil {
		return false
	}
	switch n := value.(type) {
	case int:
		return n == 0
	case int32:
		return n == 0
	case int64:
		return n == 0
	case float32:
		return n == 0
	case float64:
		return n == 0
	case json.Number:
		v, parseErr := n.Int64()
		return parseErr == nil && v == 0
	default:
		return false
	}
}

// IsDrained reports whether the registry read model reflects the
// worker as actually draining. It reads the same cached Worker the
// placement matcher sees — never the return value of SetDrainMode.
// This closes the bug class where a drain backend reports success
// without the worker entering DRAINING.
func (g *RealRegistryUpdateGater) IsDrained(ctx context.Context, workerID string) bool {
	info, err := g.GetWorker(ctx, workerID)
	if err != nil || info == nil {
		return false
	}
	return info.Drain
}

func (g *RealRegistryUpdateGater) SetDrainMode(ctx context.Context, workerID string, drain bool) error {
	if g == nil || g.Reg == nil {
		return errors.New("worker registry not wired")
	}
	return g.Reg.SetWorkerDrain(ctx, workerID, drain)
}

func (g *RealRegistryUpdateGater) GetAuthenticatedRuntimeSnapshot(ctx context.Context, workerID string) (*store.WorkerRuntimeSnapshot, error) {
	if g == nil || g.Store == nil {
		return nil, errors.New("worker runtime snapshot store not wired")
	}
	return g.Store.GetAuthenticatedWorkerRuntimeSnapshot(ctx, workerID)
}

// BackendDeploymentRepo is the typed surface for the
// deployment_records table. The executor's contract:
//
//   - GetLatestDeploymentForWorker MUST return
//     store.ErrDeploymentNotFound when no row exists — NOT
//     a wrapped error. The executor uses the authenticated runtime
//     reader for a terminal bootstrap baseline in production.
//   - InsertDeploymentRecord is called with Status=PENDING —
//     a 2-tuple call (insert + later UpdateDeploymentStatus)
//     rather than a 3-tuple (insert + status change + flag
//     change), so partial failure mid-cascade can be reasoned
//     about row-by-row.
//   - MarkVerifiedSucceeded is the ONLY forward-success writer and the
//     ONLY path that advances last_successful_digest: the store re-verifies
//     observedDigest against the row's target digest inside the transition
//     transaction (VERIFYING_DIGEST enforcement), so an unverified success
//     can never become the last-known-good digest.
//   - MarkDeploymentRolledBack atomically sets
//     status=ROLLED_BACK AND is_rollback=true in one UPDATE
//     so the dashboard's transition row is never observed in
//     a torn (status=RolledBack, flag=0) state.
//   - MarkFailed / MarkDeploymentRolledBack carry errCode — the stable
//     DeploymentErrorCode (DIGEST_MISMATCH, DRAIN_TIMEOUT, …) — separately
//     from the human-readable errMsg. Both are persisted to the journal row
//     and projected into the read model (migration 153).
type BackendDeploymentRepo interface {
	GetLatestDeploymentForWorker(ctx context.Context, workerID string) (*store.DeploymentRecord, error)
	InsertDeploymentRecord(ctx context.Context, r store.DeploymentRecord) error
	MarkVerifiedSucceeded(ctx context.Context, deploymentID, observedDigest string, finishedAt time.Time) error
	MarkFailed(ctx context.Context, deploymentID string, finishedAt time.Time, errCode, errMsg string) error
	MarkDeploymentRolledBack(ctx context.Context, deploymentID string, finishedAt time.Time, rollbackOK bool, errCode string) error
}

// BackendDeploymentPhaseRecorder is the optional seam for persisting the
// in-flight rollout phase (DRAINING → DEPLOYING → RESTARTING →
// WAITING_READY → VERIFYING_DIGEST) into worker_deployment_state.last_phase
// (migration 152) so operators can observe WHERE a rollout is — or where it
// stopped. Recording is best-effort observability: a failed phase write
// never fails the rollout.
type BackendDeploymentPhaseRecorder interface {
	RecordDeploymentPhase(ctx context.Context, workerID, phase string) error
}

// BackendDeploymentBaselineRepo is the narrow extension used only for a
// verified ledger bootstrap. Keeping it separate preserves the ordinary
// rollout invariant that InsertDeploymentRecord always starts PENDING.
type BackendDeploymentBaselineRepo interface {
	InsertBaselineDeploymentRecord(ctx context.Context, r store.DeploymentRecord) error
}

// BackendImageRefValidator wraps the canonical validation in
// internal/deploy/validator.go. Exposed as an interface for
// symmetry with the other Backends; production wires the
// trivial DefaultImageRefValidator.
type BackendImageRefValidator interface {
	Validate(ref string) error
}

// NowFunc produces the timestamp used for started_at /
// finished_at rows. Exposed as a func so tests can pin the
// clock; production passes time.Now.
type NowFunc func() time.Time
