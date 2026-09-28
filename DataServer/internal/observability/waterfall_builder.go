package observability

import (
	"sort"
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

type WaterfallBucket struct {
	Name       string `json:"name"`
	StartMS    int64  `json:"start_ms"`
	EndMS      int64  `json:"end_ms"`
	DurationMS int64  `json:"duration_ms"`
}

type PublishWaterfall struct {
	SlotWaitMS       int64 `json:"slot_wait_ms"`
	DeclareMS        int64 `json:"declare_ms"`
	UploadMS         int64 `json:"upload_ms"`
	RemoteFinalizeMS int64 `json:"remote_finalize_ms"`
	CommitWaitMS     int64 `json:"commit_wait_ms"`
	SpoolCommitMS    int64 `json:"spool_commit_ms"`
}

type AttemptWaterfall struct {
	AttemptID         string            `json:"attempt_id"`
	WallMS            int64             `json:"wall_ms"`
	Buckets           []WaterfallBucket `json:"buckets"`
	Publish           *PublishWaterfall `json:"publish,omitempty"`
	AccountedMS       int64             `json:"accounted_ms"`
	UnaccountedMS     int64             `json:"unaccounted_ms"`
	CoveragePct       float64           `json:"coverage_pct"`
	MissingMilestones []string          `json:"missing_milestones,omitempty"`
	InvertedBuckets   []string          `json:"inverted_buckets,omitempty"`
	// AssetPreparation is the STEP D drill-down INSIDE the asset_preparation
	// bucket, exactly as measured by the worker resolver sink and carried on
	// the durable TaskResult. Nil when the worker predates the field or no
	// resolution was observed — absence stays honest, never zero-filled, and
	// sub-phase sums may overlap (parallel downloads), so they are NOT re-
	// combined into a coverage number here.
	AssetPreparation *sharedtelemetry.AssetPreparationBreakdown `json:"asset_preparation,omitempty"`
	// SubmitToAccepted is the JOB-level pre-atttempt wait: job submitted
	// (jobs.created_at, Master clock) → first attempt accepted
	// (task_attempts.started_at, Master clock). It exists because
	// bucketDefs starts at attempt.accepted — the milestone timeline is
	// worker-monotonic from acceptance onward, so the scheduling wait
	// that precedes it (the "1 minute of attesa" next to "4m45s of
	// render") is structurally invisible to the bucket list and used to
	// live only in task_attempt_metrics.queue_ms (migration 074).
	//
	// It is deliberately OUTSIDE wall_ms / accounted_ms / coverage_pct:
	// those measure the attempt lifecycle (pinned by
	// TestSummarizeTask_AccountsForEveryAttemptWall), while this bucket
	// measures the wait that happened BEFORE the attempt existed. It is
	// attached only to the execution-level projection, and only when
	// both Master-clock boundaries are known — absence stays honest.
	SubmitToAccepted *WaterfallBucket `json:"submit_to_accepted,omitempty"`
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
	// Close every normal gap with an explicit UNKNOWN bucket. This makes the
	// operator waterfall additive (named work + unclassified = wall) without
	// fabricating a phase or changing the diagnostic unaccounted_ms value.
	// Missing milestone boundaries remain visible in missing_milestones.
	if wallMS > 0 && len(buckets) > 0 {
		ordered := append([]WaterfallBucket(nil), buckets...)
		sort.SliceStable(ordered, func(i, j int) bool {
			return ordered[i].StartMS < ordered[j].StartMS
		})
		var cursor int64
		for _, bucket := range ordered {
			if bucket.StartMS > cursor {
				buckets = append(buckets, WaterfallBucket{
					Name: "unclassified", StartMS: cursor, EndMS: bucket.StartMS,
					DurationMS: bucket.StartMS - cursor,
				})
			}
			if bucket.EndMS > cursor {
				cursor = bucket.EndMS
			}
		}
		if cursor < wallMS {
			buckets = append(buckets, WaterfallBucket{
				Name: "unclassified", StartMS: cursor, EndMS: wallMS,
				DurationMS: wallMS - cursor,
			})
		}
		sort.SliceStable(buckets, func(i, j int) bool {
			return buckets[i].StartMS < buckets[j].StartMS
		})
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
	waterfall := AttemptWaterfall{AttemptID: attemptID, WallMS: wallMS, Buckets: buckets, AccountedMS: accounted, UnaccountedMS: unaccounted, CoveragePct: coverage, MissingMilestones: dedupMissing(missing), InvertedBuckets: dedupMissing(inverted)}
	publish := publishWaterfall(elapsed)
	if publish != nil {
		waterfall.Publish = publish
	}
	return waterfall
}

func publishWaterfall(elapsed map[sharedtelemetry.AttemptMilestone]int64) *PublishWaterfall {
	values := []struct {
		out      *int64
		from, to sharedtelemetry.AttemptMilestone
	}{
		{new(int64), sharedtelemetry.MilestonePublishSlotWaitStarted, sharedtelemetry.MilestonePublishSlotWaitCompleted},
		{new(int64), sharedtelemetry.MilestonePublishDeclareStarted, sharedtelemetry.MilestonePublishDeclareCompleted},
		{new(int64), sharedtelemetry.MilestonePublishUploadStarted, sharedtelemetry.MilestonePublishUploadCompleted},
		{new(int64), sharedtelemetry.MilestonePublishRemoteFinalizeStarted, sharedtelemetry.MilestonePublishRemoteFinalizeCompleted},
		{new(int64), sharedtelemetry.MilestonePublishCommitWaitStarted, sharedtelemetry.MilestonePublishCommitWaitCompleted},
		{new(int64), sharedtelemetry.MilestonePublishSpoolCommitStarted, sharedtelemetry.MilestonePublishSpoolCommitCompleted},
	}
	present := false
	for _, v := range values {
		if start, ok := elapsed[v.from]; ok {
			if end, ok := elapsed[v.to]; ok && end >= start {
				*v.out = end - start
				present = true
			}
		}
	}
	if !present {
		return nil
	}
	return &PublishWaterfall{SlotWaitMS: *values[0].out, DeclareMS: *values[1].out, UploadMS: *values[2].out, RemoteFinalizeMS: *values[3].out, CommitWaitMS: *values[4].out, SpoolCommitMS: *values[5].out}
}

// SubmitToAcceptedBucket builds the job-level scheduling-wait bucket
// that precedes the attempt timeline. Both boundaries are Master-clock
// timestamps: `submittedAt` is jobs.created_at and `firstAttemptStart`
// is the earliest attempt started_at for the job (the moment the task
// was claimed/accepted).
//
// Returns nil when a boundary is missing or the pair is inverted —
// the bucket is then omitted instead of being reported as a fake 0ms
// wait. The bucket is expressed in the same millisecond domain as the
// attempt buckets (start 0, end = wait) so a renderer can print it
// ahead of `dispatch_to_execution`; it is intentionally NOT folded
// into accounted_ms / wall_ms (see AttemptWaterfall.SubmitToAccepted).
func SubmitToAcceptedBucket(submittedAt, firstAttemptStart time.Time) *WaterfallBucket {
	if submittedAt.IsZero() || firstAttemptStart.IsZero() || !firstAttemptStart.After(submittedAt) {
		return nil
	}
	waitMS := firstAttemptStart.Sub(submittedAt).Milliseconds()
	return &WaterfallBucket{Name: "submit_to_accepted", StartMS: 0, EndMS: waitMS, DurationMS: waitMS}
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
