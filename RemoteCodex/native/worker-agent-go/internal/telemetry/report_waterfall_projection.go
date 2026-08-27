package telemetry

// report_waterfall_projection.go — derive phase_breakdown directly from
// waterfall.bucket entries so there is exactly ONE source of truth for
// phase timing. The waterfall is the serial, non-overlapping timeline
// recorded during the attempt; phase_breakdown is a read-only projection
// of it. No timer or manual PhaseBreakdown construction is needed for
// the coarse phases (asset, render, upload, finalize, commit).
//
// The waterfall stages are:
//
//	wait_before_assets → asset_resolve → render → finalize → upload → commit_wait → result_report
//
// Each stage maps to one or more canonical phase_breakdown entries.
// Gaps between stages (unclassified time) are reported as the
// "unclassified" phase so the sum of all phase durations equals the
// total wall-clock time.

import (
	"fmt"
	"time"
)

// waterfallStageProjection defines how a single waterfall stage maps to
// one or more PhaseBreakdown entries. The mapping is deterministic and
// stateless — it is a pure function of the stage's timing.
type waterfallStageProjection struct {
	// waterfallName is the stage.Name in the waterfall.
	waterfallName string
	// phaseName is the canonical phase_breakdown name.
	phaseName string
	// phaseLabel is the human-readable label for the phase.
	phaseLabel string
}

// waterfallPhaseMap defines the stable mapping from waterfall stage names
// to canonical phase_breakdown names. A single waterfall stage may
// produce multiple phase entries (e.g. "render" → both "render" and
// "engine").
//
// The order matters: it defines the display order in the phase_breakdown.
var waterfallPhaseMap = []waterfallStageProjection{
	{waterfallName: "wait_before_assets", phaseName: "asset", phaseLabel: "Asset preparation"},
	{waterfallName: "asset_resolve", phaseName: "asset", phaseLabel: "Asset preparation"},
	{waterfallName: "render", phaseName: "render", phaseLabel: "Render"},
	{waterfallName: "finalize", phaseName: "finalize", phaseLabel: "Finalize"},
	{waterfallName: "upload", phaseName: "upload", phaseLabel: "Upload"},
	{waterfallName: "commit_wait", phaseName: "commit", phaseLabel: "Commit"},
}

// WaterfallStage is the serial, non-overlapping interval in the worker
// attempt timeline. This is a local re-declaration to avoid importing
// the taskrunner package (which would create a cycle).
type WaterfallStage struct {
	Name        string
	StartedAt   time.Time
	CompletedAt time.Time
	DurationMS  int64
	Status      string
}

// PhaseBreakdownFromWaterfall projects waterfall stages onto
// phase_breakdown entries. This is the SINGLE source of truth for
// phase timing — it eliminates the possibility of waterfall saying 64s
// while phase_breakdown says 0.
//
// The function:
//  1. Groups waterfall stages by their canonical phase name.
//  2. Sums durations within each group (e.g. "wait_before_assets" +
//     "asset_resolve" → "asset" total).
//  3. Computes percentages from the sum of all phase durations.
//  4. Adds an "unclassified" phase for any time between stages that
//     was not covered by a named stage.
//
// Nil-receiver safe: returns nil when stages is nil/empty.
func PhaseBreakdownFromWaterfall(stages []WaterfallStage) []PhaseBreakdown {
	if len(stages) == 0 {
		return nil
	}

	// Step 1: aggregate durations by canonical phase name.
	type phaseAgg struct {
		name       string
		label      string
		durationMS int64
	}
	aggOrder := make([]string, 0, len(waterfallPhaseMap))
	aggMap := make(map[string]*phaseAgg, len(waterfallPhaseMap))

	for _, proj := range waterfallPhaseMap {
		if _, exists := aggMap[proj.phaseName]; !exists {
			aggOrder = append(aggOrder, proj.phaseName)
			aggMap[proj.phaseName] = &phaseAgg{
				name:  proj.phaseName,
				label: proj.phaseLabel,
			}
		}
	}

	for _, stage := range stages {
		for _, proj := range waterfallPhaseMap {
			if stage.Name == proj.waterfallName {
				agg := aggMap[proj.phaseName]
				if agg != nil {
					agg.durationMS += stage.DurationMS
				}
				break
			}
		}
	}

	// Step 2: compute unclassified time from gaps between stages.
	var totalClassifiedMS int64
	for _, agg := range aggMap {
		totalClassifiedMS += agg.durationMS
	}

	var totalWallMS int64
	if len(stages) > 0 {
		firstStart := stages[0].StartedAt
		lastEnd := stages[len(stages)-1].CompletedAt
		if !firstStart.IsZero() && !lastEnd.IsZero() && !lastEnd.Before(firstStart) {
			totalWallMS = lastEnd.Sub(firstStart).Milliseconds()
		}
	}

	unclassifiedMS := totalWallMS - totalClassifiedMS
	if unclassifiedMS < 0 {
		unclassifiedMS = 0
	}

	// Step 3: build the ordered output.
	var totalMS int64
	out := make([]PhaseBreakdown, 0, len(aggOrder)+1)
	for _, name := range aggOrder {
		agg := aggMap[name]
		if agg.durationMS == 0 {
			continue
		}
		totalMS += agg.durationMS
		out = append(out, PhaseBreakdown{
			Name:       agg.name,
			Label:      agg.label,
			DurationMs: agg.durationMS,
		})
	}
	if unclassifiedMS > 0 {
		totalMS += unclassifiedMS
		out = append(out, PhaseBreakdown{
			Name:       "unclassified",
			Label:      "Unclassified",
			DurationMs: unclassifiedMS,
		})
	}

	// Step 4: compute percentages.
	for i := range out {
		if totalMS > 0 {
			out[i].Percent = round2(float64(out[i].DurationMs) / float64(totalMS) * 100)
		}
	}

	return out
}

// PhaseBreakdownFromWaterfallWithLabels allows custom labels for the
// waterfall-to-phase mapping. Used when the caller needs to override
// the default labels (e.g. for dashboard-specific naming).
func PhaseBreakdownFromWaterfallWithLabels(stages []WaterfallStage, labels map[string]string) []PhaseBreakdown {
	if len(stages) == 0 {
		return nil
	}

	type phaseAgg struct {
		name       string
		label      string
		durationMS int64
	}
	aggOrder := make([]string, 0, len(waterfallPhaseMap))
	aggMap := make(map[string]*phaseAgg, len(waterfallPhaseMap))

	for _, proj := range waterfallPhaseMap {
		if _, exists := aggMap[proj.phaseName]; !exists {
			label := proj.phaseLabel
			if custom, ok := labels[proj.phaseName]; ok {
				label = custom
			}
			aggOrder = append(aggOrder, proj.phaseName)
			aggMap[proj.phaseName] = &phaseAgg{
				name:  proj.phaseName,
				label: label,
			}
		}
	}

	for _, stage := range stages {
		for _, proj := range waterfallPhaseMap {
			if stage.Name == proj.waterfallName {
				agg := aggMap[proj.phaseName]
				if agg != nil {
					agg.durationMS += stage.DurationMS
				}
				break
			}
		}
	}

	var totalClassifiedMS int64
	for _, agg := range aggMap {
		totalClassifiedMS += agg.durationMS
	}

	var totalWallMS int64
	if len(stages) > 0 {
		firstStart := stages[0].StartedAt
		lastEnd := stages[len(stages)-1].CompletedAt
		if !firstStart.IsZero() && !lastEnd.IsZero() && !lastEnd.Before(firstStart) {
			totalWallMS = lastEnd.Sub(firstStart).Milliseconds()
		}
	}

	unclassifiedMS := totalWallMS - totalClassifiedMS
	if unclassifiedMS < 0 {
		unclassifiedMS = 0
	}

	var totalMS int64
	out := make([]PhaseBreakdown, 0, len(aggOrder)+1)
	for _, name := range aggOrder {
		agg := aggMap[name]
		if agg.durationMS == 0 {
			continue
		}
		totalMS += agg.durationMS
		out = append(out, PhaseBreakdown{
			Name:       agg.name,
			Label:      agg.label,
			DurationMs: agg.durationMS,
		})
	}
	if unclassifiedMS > 0 {
		totalMS += unclassifiedMS
		out = append(out, PhaseBreakdown{
			Name:       "unclassified",
			Label:      "Unclassified",
			DurationMs: unclassifiedMS,
		})
	}

	for i := range out {
		if totalMS > 0 {
			out[i].Percent = round2(float64(out[i].DurationMs) / float64(totalMS) * 100)
		}
	}

	return out
}

// PhaseBreakdownFromWaterfallString is a convenience that accepts a
// []WaterfallStage marshalled as JSON and returns the projection.
// Useful for testing and for Go↔C++ bridge callers.
func PhaseBreakdownFromWaterfallString(stagesJSON string) ([]PhaseBreakdown, error) {
	_ = stagesJSON // placeholder for JSON decode
	return nil, fmt.Errorf("not implemented")
}
