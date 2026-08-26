// Package worker — canonical operational lifecycle phase catalog.
//
// operational_phases.go re-exports the phase constants from the shared
// pipeline lifecycle catalog and provides the Worker-specific
// UpdateOperationalPhase adapter that projects the canonical phase
// onto the active task and wakes the heartbeat.
//
// All task lifecycle stages MUST use UpdateOperationalPhase — never
// set Progress.Phase directly from lifecycle code.
package worker

import (
	"context"
	"strings"
	"time"

	"velox-worker-agent/pkg/video/pipeline"
)

type taskIDContextKey struct{}

// ContextWithTaskID carries the active task identity through asset
// resolution, allowing cache-miss progress to update the correct task.
func ContextWithTaskID(ctx context.Context, taskID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, taskIDContextKey{}, strings.TrimSpace(taskID))
}

// TaskIDFromContext returns the task identity attached by ContextWithTaskID.
func TaskIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	taskID, _ := ctx.Value(taskIDContextKey{}).(string)
	return strings.TrimSpace(taskID)
}

// Re-export canonical phase constants from the shared pipeline catalog.
// Consumer code should prefer pipeline.PhaseXxx directly; these
// aliases exist for backward compatibility within the worker package.
const (
	PhasePrefetching          = pipeline.PhasePrefetching
	PhaseVerifyingAssets      = pipeline.PhaseVerifyingAssets
	PhaseWaitingRuntimeAssets = pipeline.PhaseWaitingRuntimeAssets
	PhaseMaterializing        = pipeline.PhaseMaterializing
	PhaseBuildingPlan         = pipeline.PhaseBuildingPlan
	PhaseRendering            = pipeline.PhaseRendering
	PhaseFinalizing           = pipeline.PhaseFinalizing
	PhaseOutputReady          = pipeline.PhaseOutputReady
	PhasePublishing           = pipeline.PhasePublishing
	PhaseCommitWait           = pipeline.PhaseCommitWait
	PhaseDone                 = pipeline.PhaseDone
)

// UpdateOperationalPhase sets the canonical operational lifecycle
// phase on the active task and wakes the heartbeat so the master
// observes the transition immediately. This delegates to the shared
// pipeline.UpdateTaskProgress for validation and timestamping.
//
// The method is safe to call from any goroutine; it acquires
// activeTasksMu internally.
func (w *Worker) UpdateOperationalPhase(taskID, phase string) {
	if w == nil || taskID == "" {
		return
	}
	pipeline.UpdateTaskProgress(taskID, phase, func(p string, now time.Time) {
		w.activeTasksMu.Lock()
		if active := w.activeTasks[taskID]; active != nil {
			active.OperationalPhase = p
			active.Progress.LastProgressAt = now
		}
		w.activeTasksMu.Unlock()
		w.wakeHeartbeat()
	})
}

// updateUploadProgress records per-artifact upload progress in the
// active task's CumulativeMetrics so the heartbeat carries upload
// visibility (bytes uploaded, total, percent, speed, ETA, artifact
// index). uploadStarted is the wall-clock time the overall upload
// phase began (from publishArtifactsV1). This is called after each
// artifact upload completes in uploadDeclaredArtifacts.
func (w *Worker) updateUploadProgress(taskID string, uploadedBytes, totalBytes int64, artifactIndex, artifactTotal int, uploadStarted time.Time) {
	if w == nil || taskID == "" {
		return
	}
	now := time.Now().UTC()
	w.activeTasksMu.Lock()
	if active := w.activeTasks[taskID]; active != nil {
		if active.Progress.CumulativeMetrics == nil {
			active.Progress.CumulativeMetrics = make(map[string]float64)
		}
		active.Progress.CumulativeMetrics["upload_bytes"] = float64(uploadedBytes)
		active.Progress.CumulativeMetrics["upload_total_bytes"] = float64(totalBytes)
		if totalBytes > 0 {
			active.Progress.CumulativeMetrics["upload_percent"] = float64(uploadedBytes) / float64(totalBytes) * 100
		}
		// Compute upload speed (bytes/sec) and ETA from wall-clock elapsed time.
		elapsed := time.Since(uploadStarted).Seconds()
		if elapsed > 0 && uploadedBytes > 0 {
			bytesPerSec := float64(uploadedBytes) / elapsed
			active.Progress.CumulativeMetrics["upload_bytes_per_second"] = bytesPerSec
			if totalBytes > uploadedBytes && bytesPerSec > 0 {
				remainingBytes := float64(totalBytes - uploadedBytes)
				active.Progress.CumulativeMetrics["upload_eta_seconds"] = remainingBytes / bytesPerSec
			} else {
				active.Progress.CumulativeMetrics["upload_eta_seconds"] = 0
			}
		}
		active.Progress.CumulativeMetrics["upload_artifact_index"] = float64(artifactIndex)
		active.Progress.CumulativeMetrics["upload_artifact_total"] = float64(artifactTotal)
		active.Progress.LastProgressAt = now
	}
	w.activeTasksMu.Unlock()
	w.wakeHeartbeat()
}
