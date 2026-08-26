package observability

import (
	sharedtelemetry "velox-shared/telemetry"
)

type WaterfallBucket struct {
	Name       string `json:"name"`
	StartMS    int64  `json:"start_ms"`
	EndMS      int64  `json:"end_ms"`
	DurationMS int64  `json:"duration_ms"`
}

type AttemptWaterfall struct {
	AttemptID         string            `json:"attempt_id"`
	WallMS            int64             `json:"wall_ms"`
	Buckets           []WaterfallBucket `json:"buckets"`
	AccountedMS       int64             `json:"accounted_ms"`
	UnaccountedMS     int64             `json:"unaccounted_ms"`
	CoveragePct       float64           `json:"coverage_pct"`
	MissingMilestones []string          `json:"missing_milestones,omitempty"`
	InvertedBuckets   []string          `json:"inverted_buckets,omitempty"`
}

var bucketDefs = []struct {
	Name string
	From sharedtelemetry.AttemptMilestone
	To   sharedtelemetry.AttemptMilestone
}{
	{"dispatch_to_execution", sharedtelemetry.MilestoneAttemptAccepted, sharedtelemetry.MilestoneExecutionStarted},
	{"pre_asset_setup", sharedtelemetry.MilestoneExecutionStarted, sharedtelemetry.MilestoneAssetsRequested},
	{"asset_preparation", sharedtelemetry.MilestoneAssetsRequested, sharedtelemetry.MilestoneAllAssetsReady},
	{"pre_plan_wait", sharedtelemetry.MilestoneAllAssetsReady, sharedtelemetry.MilestonePlanStarted},
	{"plan_compile", sharedtelemetry.MilestonePlanStarted, sharedtelemetry.MilestonePlanCompleted},
	{"pre_render_wait", sharedtelemetry.MilestonePlanCompleted, sharedtelemetry.MilestoneRenderStarted},
	{"render", sharedtelemetry.MilestoneRenderStarted, sharedtelemetry.MilestoneRenderCompleted},
	{"finalize", sharedtelemetry.MilestoneRenderCompleted, sharedtelemetry.MilestoneOutputDurable},
	{"publish_queue_wait", sharedtelemetry.MilestoneOutputDurable, sharedtelemetry.MilestonePublishStarted},
	{"publish", sharedtelemetry.MilestonePublishStarted, sharedtelemetry.MilestonePublishCompleted},
	{"result_finalize", sharedtelemetry.MilestonePublishCompleted, sharedtelemetry.MilestoneResultSent},
	{"result_ingest", sharedtelemetry.MilestoneResultSent, sharedtelemetry.MilestoneAttemptCompleted},
}

func BuildAttemptWaterfall(attemptID string, samples []sharedtelemetry.AttemptMilestoneSample, wallMS int64) AttemptWaterfall {
	elapsed := make(map[sharedtelemetry.AttemptMilestone]int64, len(samples))
	for _, s := range samples {
		elapsed[s.Name] = s.ElapsedMS
	}
	var buckets []WaterfallBucket
	var accounted int64
	var missing []string
	var inverted []string
	for _, def := range bucketDefs {
		start, okStart := elapsed[def.From]
		end, okEnd := elapsed[def.To]
		if !okStart || !okEnd {
			if !okStart {
				missing = append(missing, string(def.From))
			}
			if !okEnd {
				missing = append(missing, string(def.To))
			}
			continue
		}
		if end < start {
			// Inverted boundary pair (end < start): the bucket cannot be built and
			// the stretch is NOT silently attributed anywhere. The pair is reported
			// explicitly and its span lands in unaccounted_ms as honest UNKNOWN.
			inverted = append(inverted, def.Name)
			continue
		}
		dur := end - start
		buckets = append(buckets, WaterfallBucket{Name: def.Name, StartMS: start, EndMS: end, DurationMS: dur})
		accounted += dur
	}
	// Deliberately NOT clamped: when the milestone timeline over-covers the wall
	// (overlapping buckets, duplicate/late milestones, or worker/master clock skew
	// on the boundary pair) unaccounted_ms goes negative and coverage_pct exceeds
	// 100 — the corruption is surfaced instead of being masked as "100% covered".
	unaccounted := wallMS - accounted
	coverage := 0.0
	if wallMS > 0 {
		coverage = float64(accounted) / float64(wallMS) * 100
	}
	if missing == nil {
		missing = []string{}
	}
	return AttemptWaterfall{AttemptID: attemptID, WallMS: wallMS, Buckets: buckets, AccountedMS: accounted, UnaccountedMS: unaccounted, CoveragePct: coverage, MissingMilestones: dedupMissing(missing), InvertedBuckets: dedupMissing(inverted)}
}

func dedupMissing(in []string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
