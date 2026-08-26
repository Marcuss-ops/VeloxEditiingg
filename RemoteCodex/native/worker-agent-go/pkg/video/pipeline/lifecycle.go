// Package pipeline — canonical operational lifecycle phase catalog.
//
// lifecycle.go defines the operator-visible lifecycle phases that cover
// a task from asset prefetching through render to commit. These are
// distinct from the renderer-internal telemetry phases (decode/composite/
// encode) which are emitted by the C++ engine's ProgressSnapshot callback
// during the RENDERING phase.
//
// The canonical lifecycle:
//
//	PREFETCHING → VERIFYING_ASSETS → WAITING_RUNTIME_ASSETS →
//	MATERIALIZING → BUILDING_PLAN → RENDERING → FINALIZING →
//	OUTPUT_READY → PUBLISHING → COMMIT_WAIT → DONE
//
// Every task lifecycle stage must use UpdateTaskProgress to project a
// single canonical phase name onto the active task. The heartbeat sends
// this as the operational_phase field alongside the renderer's
// progress_phase, giving operators end-to-end visibility without any new
// database columns or persistence changes.
package pipeline

import "time"

// Canonical operational lifecycle phases. Lower-case, snake_case,
// stable naming — do not rename without updating dashboard filters
// and fleetctl rendering.
//
// PhaseOrder documents the intended chronological sequence; phases
// may be skipped (e.g., V1 payloads skip BUILDING_PLAN) but should
// never appear out of order.
const (
	// PhasePrefetching is the initial phase: the worker is fetching
	// or resolving assets from cache/download before any rendering
	// work begins.
	PhasePrefetching = "prefetching"

	// PhaseVerifyingAssets is set when the worker is verifying
	// asset integrity (SHA256 checks, size validation) after
	// download/resolution completes.
	PhaseVerifyingAssets = "verifying_assets"

	// PhaseWaitingRuntimeAssets is set when the worker is waiting
	// for runtime asset dependencies (e.g., future/prefetched assets
	// that are not yet available locally).
	PhaseWaitingRuntimeAssets = "waiting_runtime_assets"

	// PhaseMaterializing is set when the worker is materializing
	// the working set: resolving V2 compiled plan asset references
	// into local filesystem paths.
	PhaseMaterializing = "materializing"

	// PhaseBuildingPlan is set when the worker is building or
	// validating the render plan (segment splitting, timeline
	// construction) before dispatching to the C++ engine.
	PhaseBuildingPlan = "building_plan"

	// PhaseRendering covers the entire C++ engine execution. The
	// renderer's ProgressSnapshot callback provides sub-phase
	// visibility (decode/composite/encode) via progress_phase,
	// while this operational phase remains "rendering".
	PhaseRendering = "rendering"

	// PhaseFinalizing is set when the C++ engine has completed
	// rendering and the worker is finalizing output artifacts
	// (hashing, probing, verification).
	PhaseFinalizing = "finalizing"

	// PhaseOutputReady is set when output artifacts are finalized
	// and ready for publication but upload has not yet started.
	PhaseOutputReady = "output_ready"

	// PhasePublishing is set when the worker is uploading output
	// artifacts to the master (declare → upload → completion).
	PhasePublishing = "publishing"

	// PhaseCommitWait is set when the worker is waiting for the
	// master to acknowledge the committed task result.
	PhaseCommitWait = "commit_wait"

	// PhaseDone is set when the task has completed successfully
	// (or failed) and the result has been submitted.
	PhaseDone = "done"
)

// AllPhases returns the ordered list of all canonical lifecycle phases.
// Useful for validation, dashboard rendering, and testing.
func AllPhases() []string {
	return []string{
		PhasePrefetching,
		PhaseVerifyingAssets,
		PhaseWaitingRuntimeAssets,
		PhaseMaterializing,
		PhaseBuildingPlan,
		PhaseRendering,
		PhaseFinalizing,
		PhaseOutputReady,
		PhasePublishing,
		PhaseCommitWait,
		PhaseDone,
	}
}

// IsValidPhase returns true if the given string is a recognized
// canonical lifecycle phase.
func IsValidPhase(phase string) bool {
	for _, p := range AllPhases() {
		if p == phase {
			return true
		}
	}
	return false
}

// PhaseProgress is the callback signature for UpdateTaskProgress.
// The caller provides a function that receives the new phase and
// timestamp and applies it to the active task's state.
type PhaseProgress func(phase string, now time.Time)

// UpdateTaskProgress is the single entry point for all task lifecycle
// phase transitions. It validates the phase, records the timestamp,
// and invokes the callback so the caller can update its internal state
// and wake the heartbeat.
//
// This function is safe to call from any goroutine; the caller's
// callback is responsible for any required synchronization.
func UpdateTaskProgress(taskID, phase string, update PhaseProgress) {
	if taskID == "" || phase == "" || update == nil {
		return
	}
	if !IsValidPhase(phase) {
		return
	}
	update(phase, time.Now().UTC())
}
