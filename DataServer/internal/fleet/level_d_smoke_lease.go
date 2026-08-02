// Package fleet — RegistryDrainLease (BackendLeaseStore adapter).
//
// Split out of level_d_smoke_deps.go. See the parent file for the
// full Level D smoke dependency-surface contract.
package fleet

import (
	"context"
	"fmt"
	"strings"

	workersreg "velox-server/internal/workers"
)

// RegistryDrainLease adapts the in-process workersreg.Registry
// to the BackendLeaseStore surface: AcquireSmokeLease flips
// WorkerInfo.Drain=true (excluding the worker from real-job
// placement for the smoke duration); ReleaseSmokeLease restores
// Drain=false. Symmetric with Step 6/15's mutations handler
// which calls SetWorkerDrain directly.
//
// Audit-only invariant: when WorkerInfo.Drain is set transiently
// by smoke, the Worker's Health derivation (Step 3/15) reflects
// DRAINING on the next poll. The deferred ReleaseSmokeLease
// in LevelDSmokeExecutor's Phase 3 cleanup ensures the worker
// recovers to HEALTHY even if a panic / cleanup-skipped state
// interrupts the pipeline.
//
// The runID convention is "smoke-<workerID>-<nanos>" — see
// LevelDSmokeExecutor's `runID := fmt.Sprintf(...)` call —
// so ReleaseSmokeLease splits the runID to recover workerID.
// Future steps may swap this for a parallel
// smoke_lease_owner column in workers.raw_json, but for atomic
// Step 12+ we accept the URL-encoding constraint.
type RegistryDrainLease struct {
	Reg *workersreg.Registry
}

// NewRegistryDrainLease returns the lease store wrapping the
// given registry. Production calls this in bootstrap with
// m.Workers.Registry() as the registry. Returns nil if reg is
// nil so the bootstrap's nil-tolerance flow-through survives.
func NewRegistryDrainLease(reg *workersreg.Registry) BackendLeaseStore {
	if reg == nil {
		return nil
	}
	return &RegistryDrainLease{Reg: reg}
}

// AcquireSmokeLease calls reg.SetWorkerDrain(workerID, true) so
// costmodel.Score excludes the worker. Returns
// ErrSmokeLeaseUnavailable on any underlying error so the
// audit-row grep is stable.
func (r *RegistryDrainLease) AcquireSmokeLease(_ context.Context, runID, workerID string) error {
	if r == nil || r.Reg == nil {
		return ErrSmokeLeaseUnavailable
	}
	if err := r.Reg.SetWorkerDrain(context.Background(), workerID, true); err != nil {
		return fmt.Errorf("%w: worker drain or registry error: %v", ErrSmokeLeaseUnavailable, err)
	}
	return nil
}

// ReleaseSmokeLease calls reg.SetWorkerDrain(workerID, false)
// idempotently. Parses the workerID from the runID: the executor
// formats runID as "smoke-<workerID>-<nanos>". Since workerID
// may itself contain dashes (e.g. "velox-worker-523925eb"), we
// strip the "smoke-" prefix then split on the LAST dash to
// separate workerID from nanos.
func (r *RegistryDrainLease) ReleaseSmokeLease(_ context.Context, runID string) error {
	if r == nil || r.Reg == nil {
		return nil
	}
	const prefix = "smoke-"
	if !strings.HasPrefix(runID, prefix) {
		return nil
	}
	withoutPrefix := runID[len(prefix):]
	lastDash := strings.LastIndex(withoutPrefix, "-")
	if lastDash <= 0 {
		return nil
	}
	workerID := withoutPrefix[:lastDash]
	if err := r.Reg.SetWorkerDrain(context.Background(), workerID, false); err != nil {
		return fmt.Errorf("smoke: release drain: %w", err)
	}
	return nil
}
