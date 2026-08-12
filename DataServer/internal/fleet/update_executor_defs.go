package fleet

import (
	"context"
	"errors"
	"strings"
	"time"
)

// Sentinel errors returned by Execute. Each maps to a
// grep-friendly error_message prefix in the audit ledger so
// operator dashboards can route on them.
var (
	// ErrEmptyRegistry is retained for partially composed legacy callers that
	// have no authenticated runtime reader. Production uses the reader to
	// bootstrap a verified baseline instead. Forward never starts.
	ErrEmptyRegistry = errors.New("empty_registry")

	// ErrBootstrapUnverifiable means a missing ledger could not be safely
	// adopted because the authenticated runtime digest was absent or the
	// bootstrap dependency was not wired.
	ErrBootstrapUnverifiable = errors.New("bootstrap_unverifiable")

	// ErrBootstrapDigestMismatch means the operator target does not match the
	// digest reported by the currently authenticated worker session.
	ErrBootstrapDigestMismatch = errors.New("bootstrap_digest_mismatch")

	// ErrBootstrapWorkerDisconnected and ErrBootstrapWorkerUnhealthy are
	// fail-closed bootstrap guards. Adoption never changes worker state.
	ErrBootstrapWorkerDisconnected = errors.New("bootstrap_worker_disconnected")
	ErrBootstrapWorkerUnhealthy    = errors.New("bootstrap_worker_unhealthy")

	// ErrUnregisteredWorker is returned when the worker_id is
	// not present in the in-process registry. Forward never
	// started; NO rollback is attempted.
	ErrUnregisteredWorker = errors.New("unregistered_worker")

	// ErrRollbackSucceeded is the marker for "forward failed
	// but the rollback cascade recovered the worker". Wrapped
	// around the forward error so the audit dashboard's error
	// field reads "<forward> (rollback_ok to <prev>)".
	ErrRollbackSucceeded = errors.New("rollback_ok")

	// ErrRollbackFailed is the marker for "forward failed AND
	// the rollback cascade also failed". The audit dashboard
	// surfaces this as a hard error; operator intervention
	// required.
	ErrRollbackFailed = errors.New("rollback_failed")

	// ErrContainerUnhealthy is returned by the deps backend
	// when the container is broken. The executor treats this
	// as a forward-failure trigger and rolls back.
	ErrContainerUnhealthy = errors.New("container_unhealthy")

	// ErrDriveDeliveryMissing is returned by BackendDriveVerifier
	// when the smoke artifact is not reachable on Drive.
	ErrDriveDeliveryMissing = errors.New("drive_delivery_missing")

	// ErrDriveDeliverySize is returned by BackendDriveVerifier
	// when the smoke artifact's Drive-resident size does not
	// match the expected byte count.
	ErrDriveDeliverySize = errors.New("drive_delivery_size_mismatch")

	// ErrSmokeFailed is returned by BackendSmokeRunner on
	// Level D smoke test failure.
	ErrSmokeFailed = errors.New("smoke_failed")

	// ErrDigestMismatch is returned by the VERIFYING_DIGEST phase when the
	// authenticated running digest does not match the operation target. The
	// caller marks the forward row FAILED with error_code `digest_mismatch`
	// (message: "digest_mismatch: expected=<target> observed=<running>") and
	// runs the rollback cascade to restore the previous digest. running_digest
	// itself is never written by the control plane — it stays the observed
	// heartbeat value.
	ErrDigestMismatch = errors.New("digest_mismatch")
)

// DeploymentErrorCode is the stable, closed vocabulary for a failed fleet
// operation. Codes are persisted in deployment_records.error_code (journal,
// migration 153) and projected into worker_deployment_state.
// last_operation_error_code (read model) so operators, metrics and admin
// filters can route on the code while the human-readable message stays free
// form. A new operation clears the current code; the history keeps every
// older code in its journal row.
const (
	DeploymentErrorCodeDrainTimeout        = "DRAIN_TIMEOUT"
	DeploymentErrorCodeDeployCommandFailed = "DEPLOY_COMMAND_FAILED"
	DeploymentErrorCodeRestartFailed       = "RESTART_FAILED"
	DeploymentErrorCodeReadyTimeout        = "READY_TIMEOUT"
	DeploymentErrorCodeDigestMismatch      = "DIGEST_MISMATCH"
	DeploymentErrorCodeSSHFailed           = "SSH_FAILED"

	// Step-specific extensions beyond the core vocabulary: smoke and Drive
	// are downstream phases the operator must distinguish from the six core
	// codes, and a failed rollback cascade has its own routing identity.
	DeploymentErrorCodeSmokeFailed    = "SMOKE_FAILED"
	DeploymentErrorCodeDriveFailed    = "DRIVE_DELIVERY_FAILED"
	DeploymentErrorCodeRollbackFailed = "ROLLBACK_FAILED"
)

// classifyDeploymentError maps a forward/rollback pipeline error to its
// stable DeploymentErrorCode. Sentinel errors win first (errors.Is — the
// mapping survives re-wrapping), then the executor's own step wrap-prefixes
// (cosign: / activate image: / container_running: / health_ready: /
// waiting_ready: / drain wait:), and finally an SSH transport signature
// (the production SSH client errors surface "ssh" in their text). Unknown
// errors default to DEPLOY_COMMAND_FAILED rather than an empty code, so the
// journal always carries a routable failure class.
//
// Reachability note: DRAIN_TIMEOUT is part of the closed vocabulary but a
// drain-gate failure happens BEFORE the PENDING deployment row is inserted
// (drain-first design: a failed drain inserts no row), so it is persisted as
// a fleet_operations error_message only, never as a deployment error_code.
// The branch is kept so the code survives if the pipeline is ever reordered
// to insert the row before draining.
func classifyDeploymentError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, ErrDigestMismatch):
		return DeploymentErrorCodeDigestMismatch
	case errors.Is(err, ErrSmokeFailed):
		return DeploymentErrorCodeSmokeFailed
	case errors.Is(err, ErrDriveDeliveryMissing), errors.Is(err, ErrDriveDeliverySize):
		return DeploymentErrorCodeDriveFailed
	case errors.Is(err, ErrContainerUnhealthy):
		return DeploymentErrorCodeRestartFailed
	case strings.Contains(msg, "ssh"):
		return DeploymentErrorCodeSSHFailed
	case strings.Contains(msg, "waiting_ready:"):
		return DeploymentErrorCodeReadyTimeout
	case strings.Contains(msg, "drain wait:"):
		return DeploymentErrorCodeDrainTimeout
	case strings.Contains(msg, "health_ready:"), strings.Contains(msg, "container_running:"):
		return DeploymentErrorCodeRestartFailed
	// Rollback-cascade errors are checked BEFORE the generic activate-image
	// prefix: a rollback step failure is a rollback failure even when it
	// happens while re-activating the previous digest.
	case strings.Contains(msg, "rollback"):
		return DeploymentErrorCodeRollbackFailed
	case strings.Contains(msg, "cosign:"), strings.Contains(msg, "activate image:"):
		return DeploymentErrorCodeDeployCommandFailed
	default:
		return DeploymentErrorCodeDeployCommandFailed
	}
}

// Per-step timeouts. Each step uses context.WithTimeout to
// bound its own budget — a runaway step fails fast rather
// than pinning the FleetController's opTimeout for the
// entire update.
//
// Total forward budget: 25-30 min. Total rollback budget:
// 15-20 min. Confirms the user's spec detail
// ("DRAINING → /health/ready → health → Master connection
// → Level D smoke → Drive delivery verification").
const (
	timeoutSnapshot       = 10 * time.Second
	timeoutDrainVerify    = 5 * time.Second
	timeoutActiveJobsIdle = 5 * time.Minute
	timeoutWaitReady      = 5 * time.Minute
	timeoutCosign         = 30 * time.Second
	timeoutDockerPull     = 10 * time.Minute
	timeoutComposeRestart = 2 * time.Minute
	timeoutContainerCheck = 30 * time.Second
	timeoutHealthReady    = 60 * time.Second
	timeoutMasterCheck    = 30 * time.Second
	timeoutSmokeRun       = 5 * time.Minute
	timeoutDriveVerify    = 60 * time.Second
)

// Rollout phase vocabulary persisted into worker_deployment_state.last_phase
// (migration 152) via the BackendDeploymentPhaseRecorder seam. The sequence
// is the UpdateExecutor's internal pipeline; terminal outcomes stay
// expressed by last_operation_status (SUCCEEDED/FAILED).
const (
	RolloutPhaseDraining        = "DRAINING"
	RolloutPhaseDeploying       = "DEPLOYING"
	RolloutPhaseRestarting      = "RESTARTING"
	RolloutPhaseWaitingReady    = "WAITING_READY"
	RolloutPhaseVerifyingDigest = "VERIFYING_DIGEST"
)

// BackendCosignVerifierIfc is the local alias for
// velox-server/internal/deploy/cosign.CosignVerifier so the
// executor struct definition here doesn't import the cosign
// package twice (once in the deps file, once again here).
//
// Go allows declaring the type alias in either location; placed
// here because the executor is the consumer.
type BackendCosignVerifierIfc interface {
	Verify(ctx context.Context, ref string) error
}
