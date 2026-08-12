package performance

// snapshot_assembler.go — the AttemptSnapshot → PerformanceReceiptV1
// projection.
//
// The telemetry pipeline (internal/telemetry) collects one canonical
// AttemptSnapshot at attempt Stop and the ReceiptSink hands it to this
// function. It is the receipt projection of the canonical facts: every
// section is mapped from snapshot facts ONLY, absent facts stay zero
// (fail-closed, never inferred), and the Derived section is produced by
// the single Deriver — never computed here.

import (
	sharedtelemetry "velox-shared/telemetry"
	attempttelemetry "velox-worker-agent/internal/telemetry"
)

// AssembleFromSnapshot builds the canonical PerformanceReceiptV1 from the
// RAW per-attempt fact bundle collected by the collector registry.
//
// Unlike the RunMetrics-based assembler (which the executor feeds with
// engine sidecar detail), this projection consumes the canonical snapshot
// the telemetry pipeline owns: resource facts (cgroup/proc), process and
// media facts projected from the canonical journal, and the per-attempt
// cache delta. Sections the snapshot does not carry (workload, mux bytes,
// scheduling) stay zero until their collector lands.
func AssembleFromSnapshot(snapshot *attempttelemetry.AttemptSnapshot) *PerformanceReceiptV1 {
	receipt := NewPerformanceReceiptV1()
	if snapshot == nil {
		return receipt
	}
	receipt.Identity = PerformanceIdentity{
		JobID:     snapshot.Identity.JobID,
		AttemptID: snapshot.Identity.AttemptID,
		WorkerID:  snapshot.Identity.WorkerID,
		// PerformanceIdentity has no ExecutorID field; the executor is
		// deliberately not mapped into the receipt identity (it rides the
		// attempt metadata on the master side).
	}
	raw := snapshot.RawEnvelope()
	wall := snapshot.WallMs
	if wall <= 0 {
		wall = int64(raw.Resources.WallClockSeconds * 1000)
	}
	receipt.Timing.WallMs = wall
	receipt.Process = ProcessMetrics{
		EngineSpawnCount:         raw.Process.EngineSpawnCount,
		EngineExternalSpawnCount: raw.Process.EngineExternalSpawnCount,
		EngineFfmpegSpawnCount:   raw.Process.EngineFfmpegSpawnCount,
		EngineFfprobeSpawnCount:  raw.Process.EngineFfprobeSpawnCount,
		EngineShellSpawnCount:    raw.Process.EngineShellSpawnCount,
		EngineCurlSpawnCount:     raw.Process.EngineCurlSpawnCount,
	}
	receipt.CPU = CPUMetrics{
		WallMs:            wall,
		CPUTotalMs:        raw.Resources.CpuTimeMs,
		EngineCPUUserMs:   raw.Process.EngineCPUUserMs,
		EngineCPUSystemMs: raw.Process.EngineCPUSystemMs,
	}
	receipt.Scheduling = SchedulingMetrics{
		VoluntaryContextSwitches:   raw.Process.EngineVoluntaryContextSwitches,
		InvoluntaryContextSwitches: raw.Process.EngineInvoluntaryContextSwitches,
		MinorPageFaults:            raw.Process.EngineMinorPageFaults,
		MajorPageFaults:            raw.Process.EngineMajorPageFaults,
	}
	receipt.IO = IOMetrics{
		TotalBytesRead:    raw.Resources.DiskReadBytes,
		TotalBytesWritten: raw.Resources.DiskWriteBytes,
		FinalBytesWritten: raw.Resources.OutputBytes,
	}
	receipt.Memory = MemoryMetrics{PeakRSSBytes: raw.Resources.PeakRssBytes}
	// Media facts are projected from the canonical journal: output frames
	// and bytes observed on media-producer events. Fine-grained decode/
	// encode breakdowns are engine-sidecar-owned and not in the snapshot.
	receipt.Media = MediaMetrics{
		Frames:      raw.Media.FramesOut,
		OutputBytes: raw.Media.BytesOut,
	}
	receipt.Phases = phasesFromSnapshot(snapshot.Events)
	// Derived KPIs: one call, one definition — the single
	// DerivedMetricsCalculator. Workload (clip count) and external process
	// counts are render-plan/ProcessRunner-owned facts not yet carried by
	// the snapshot; they stay zero until their collectors land.
	derivedRaw := RawMetrics{
		WallMs:            wall,
		Phases:            receipt.Phases,
		CPUWallMS:         raw.Resources.CpuTimeMs,
		TotalBytesRead:    raw.Resources.DiskReadBytes,
		TotalBytesWritten: raw.Resources.DiskWriteBytes,
		OutputBytes:       raw.Resources.OutputBytes,
	}
	receipt.Derived = Derive(derivedRaw)
	// CPUWallRatio uses the same Deriver output as the receipt's derived
	// section; no second ratio formula is allowed in this projection.
	receipt.CPU.CPUWallRatio = receipt.Derived.CPUWallRatio
	return receipt
}

// phasesFromSnapshot maps the canonical journal rows onto the receipt
// Phases section, stamping each row with its catalog timing role
// (classifyRecordedPhase). Derive then sums ONLY the TimingExclusive rows
// into accounted_ratio — the same rule as the RunMetrics path.
func phasesFromSnapshot(events []attempttelemetry.RecordedPhase) []PhaseTiming {
	if len(events) == 0 {
		return nil
	}
	phases := make([]PhaseTiming, 0, len(events))
	for _, event := range events {
		phases = append(phases, PhaseTiming{
			Name:        recordedPhaseName(event),
			DurationMS:  event.DurationMS,
			CPUMs:       int64(event.CPUMS),
			QueueWaitMS: int64(event.QueueWaitMS),
			BytesIn:     event.BytesIn,
			BytesOut:    event.BytesOut,
			FramesIn:    event.FramesIn,
			FramesOut:   event.FramesOut,
			TimingMode:  classifyRecordedPhase(event),
		})
	}
	return phases
}

// classifyRecordedPhase resolves the canonical catalog timing role of one
// journal event. It shares ONE classifier with the RunMetrics assembler
// path (classifyTimingRole in classify.go) so the two receipt projections
// can never diverge on accounted_ratio semantics.
func classifyRecordedPhase(event attempttelemetry.RecordedPhase) sharedtelemetry.TimingMode {
	return classifyTimingRole(event.Component, event.Action, event.Scope, event.Phase)
}

// recordedPhaseName mirrors descriptiveName for the journal row type.
func recordedPhaseName(event attempttelemetry.RecordedPhase) string {
	return descriptiveName(event.EventName, event.Component, event.Action, event.Phase, event.EventType)
}
