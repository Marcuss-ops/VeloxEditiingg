// Package alertengine / rules.go
//
// Rule evaluation functions. Each returns a canonical runtime AlertEvent
// when the condition is breached, nil when healthy.

package alertengine

import (
	"context"
	"fmt"
	"syscall"
	"time"

	runtimealerts "velox-server/internal/alerts"
	"velox-server/internal/observability"
)

// RuleDeps holds the dependencies rules need to evaluate.
type RuleDeps struct {
	Obs          *observability.Service
	DataDir      string
	ErrorRatePct float64 // threshold for error_rate rule (default 5.0)
	P95WallMs    int64   // threshold for p95 wall time (default 300_000 = 5 min)
	DiskFreeGB   float64 // threshold for disk free (default 10.0)
	FFmpegMin    float64 // minimum ffmpeg speed ratio (default 1.5)
	// JobPendingAgeSecs is the oldest-PENDING-job age that trips
	// JobPendingAgeHigh (default 300s = 5 min). It exists for the
	// incident depth gauges cannot express: a single job waiting 7
	// minutes while workers sit idle (prefetch failed, no lease ever
	// taken) is a stuck job, not a deep queue.
	JobPendingAgeSecs int64
	// PrefetchFailureCount is the number of journaled prefetch failures
	// inside PrefetchWindow that trips PrefetchFailureSpike (default 3
	// failures / 15 min).
	PrefetchFailureCount int64
	// PrefetchWindow is the lookback for the spike rule. Fixed at
	// 15 minutes (not operator-tunable) so "spike" keeps one meaning
	// across deployments.
	PrefetchWindow time.Duration
}

// DefaultRuleDeps returns RuleDeps with safe defaults.
func DefaultRuleDeps() RuleDeps {
	return RuleDeps{
		ErrorRatePct:         5.0,
		P95WallMs:            300_000,
		DiskFreeGB:           10.0,
		FFmpegMin:            1.5,
		JobPendingAgeSecs:    300,
		PrefetchFailureCount: 3,
		PrefetchWindow:       15 * time.Minute,
	}
}

// MakeRules creates the standard set of 8 alert rules.
func MakeRules(deps RuleDeps) []RuleFunc {
	return []RuleFunc{
		ruleErrorRate(deps),
		ruleP95WallMs(deps),
		ruleWorkerOffline(deps),
		ruleDiskFree(deps),
		ruleFFmpegSpeedRatio(deps),
		rulePacketCopyContract(deps),
		ruleJobPendingAgeHigh(deps),
		rulePrefetchFailureSpike(deps),
	}
}

func ruleErrorRate(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		if deps.Obs == nil {
			return nil, nil
		}
		ov, err := deps.Obs.Overview(ctx)
		if err != nil {
			return nil, fmt.Errorf("alert rule ErrorRateHigh: overview: %w", err)
		}
		if ov.ErrorRate > deps.ErrorRatePct {
			return &runtimealerts.AlertEvent{
				RuleID:   "ErrorRateHigh",
				Severity: "warning",
				Summary:  fmt.Sprintf("Error rate %.1f%% exceeds threshold %.1f%%", ov.ErrorRate, deps.ErrorRatePct),
				Description: fmt.Sprintf(
					"Jobs completed: %d, failed: %d, rate: %.1f%%. Queue depth: %d.",
					ov.JobsCompleted24h, ov.JobsFailed24h, ov.ErrorRate, ov.QueueDepth,
				),
				Labels: map[string]string{"domain": "jobs"},
			}, nil
		}
		return nil, nil
	}
}

func ruleP95WallMs(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		if deps.Obs == nil {
			return nil, nil
		}
		ov, err := deps.Obs.Overview(ctx)
		if err != nil {
			return nil, fmt.Errorf("alert rule P95WallMsHigh: overview: %w", err)
		}
		if ov.P95RenderMS > deps.P95WallMs {
			return &runtimealerts.AlertEvent{
				RuleID:      "P95WallMsHigh",
				Severity:    "warning",
				Summary:     fmt.Sprintf("P95 render time %dms exceeds threshold %dms", ov.P95RenderMS, deps.P95WallMs),
				Description: fmt.Sprintf("P95 render: %dms. Active workers: %d.", ov.P95RenderMS, ov.ActiveWorkers),
				Labels:      map[string]string{"domain": "performance"},
			}, nil
		}
		return nil, nil
	}
}

func ruleWorkerOffline(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		if deps.Obs == nil {
			return nil, nil
		}
		workers, err := deps.Obs.ListWorkers(ctx)
		if err != nil {
			return nil, fmt.Errorf("alert rule WorkersOffline: list workers: %w", err)
		}
		var offline []string
		for _, w := range workers {
			// ConnectionStatus uses the worker registry taxonomy:
			// CONNECTED, STALE, DISCONNECTED, DRAINING.
			// Only CONNECTED means the worker is alive; everything
			// else signals a lost or draining node.
			if w.Status != "CONNECTED" {
				offline = append(offline, w.WorkerID)
			}
		}
		if len(offline) > 0 {
			return &runtimealerts.AlertEvent{
				RuleID:      "WorkersOffline",
				Severity:    "critical",
				Summary:     fmt.Sprintf("%d workers offline", len(offline)),
				Description: fmt.Sprintf("Offline workers: %v", offline),
				Labels:      map[string]string{"domain": "workers", "count": fmt.Sprintf("%d", len(offline))},
			}, nil
		}
		return nil, nil
	}
}

func ruleDiskFree(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		dir := deps.DataDir
		if dir == "" {
			return nil, nil
		}
		var stat syscall.Statfs_t
		if err := syscall.Statfs(dir, &stat); err != nil {
			return nil, fmt.Errorf("alert rule DiskFreeLow: statfs %q: %w", dir, err)
		}
		freeGB := float64(stat.Bavail*uint64(stat.Bsize)) / 1_073_741_824.0
		if freeGB < deps.DiskFreeGB {
			return &runtimealerts.AlertEvent{
				RuleID:      "DiskFreeLow",
				Severity:    "critical",
				Summary:     fmt.Sprintf("Disk free %.1f GB below threshold %.1f GB on %s", freeGB, deps.DiskFreeGB, dir),
				Description: fmt.Sprintf("Available: %.1f GB. Block size: %d, available blocks: %d.", freeGB, stat.Bsize, stat.Bavail),
				Labels:      map[string]string{"domain": "infra", "path": dir},
			}, nil
		}
		return nil, nil
	}
}

func ruleFFmpegSpeedRatio(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		if deps.Obs == nil {
			return nil, nil
		}
		// ffmpeg_speed_ratio is a scalar column on task_attempt_metrics,
		// not a phase timing. Use RecentScalarMetric which reads from
		// the correct table.
		result, err := deps.Obs.RecentScalarMetric(ctx, "ffmpeg_speed_ratio")
		if err != nil {
			return nil, fmt.Errorf("alert rule FFmpegSpeedRatioLow: recent scalar metric: %w", err)
		}
		if result == nil || result.Samples == 0 {
			return nil, nil
		}
		p95 := result.P95
		if p95 > 0 && p95 < deps.FFmpegMin {
			return &runtimealerts.AlertEvent{
				RuleID:      "FFmpegSpeedRatioLow",
				Severity:    "warning",
				Summary:     fmt.Sprintf("FFmpeg speed ratio P95 %.2fx below threshold %.2fx", p95, deps.FFmpegMin),
				Description: fmt.Sprintf("P95 ffmpeg speed ratio: %.2fx over %d samples.", p95, result.Samples),
				Labels:      map[string]string{"domain": "performance"},
			}, nil
		}
		return nil, nil
	}
}

func rulePacketCopyContract(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		if deps.Obs == nil {
			return nil, nil
		}
		result, err := deps.Obs.RecentPacketCopyContract(ctx)
		if err != nil {
			return nil, fmt.Errorf("alert rule PacketCopyContractViolated: recent metrics: %w", err)
		}
		if result == nil || result.Violations == 0 {
			return nil, nil
		}

		return &runtimealerts.AlertEvent{
			RuleID:   "PacketCopyContractViolated",
			Severity: "critical",
			Summary: fmt.Sprintf("mixed-packet contract violated in %d attempt(s)",
				result.Violations),
			Description: fmt.Sprintf(
				"concat_mode=mixed_packet requires encode_passes=0 and packet_copy_ratio=100. First violating attempt %s reported encode_passes=%d and packet_copy_ratio=%.1f.",
				result.AttemptID, result.EncodePasses, result.PacketCopyRatio,
			),
			Labels: map[string]string{
				"domain":            "performance",
				"concat_mode":       "mixed_packet",
				"violations":        fmt.Sprintf("%d", result.Violations),
				"attempt_id":        result.AttemptID,
				"packet_copy_ratio": fmt.Sprintf("%.1f", result.PacketCopyRatio),
			},
		}, nil
	}
}

// ruleJobPendingAgeHigh is the stuck-job alarm the queue-depth views
// cannot raise: it reads the OLDEST PENDING job's age, not the count.
// Prometheus twin: `VeloxJobPendingAgeHigh` on
// velox_jobs_oldest_pending_age_seconds (alerts/job-queue-and-prefetch.yml).
//
// The incident this exists for ("Milton"): a job sits in PENDING for
// 7+ minutes with 4 workers free because its prefetch failed and no
// lease was ever taken. Depth stays at 1, every other rule (error
// rate, p95 render, worker offline, disk, ffmpeg speed, packet copy)
// stays green — nothing fired.
//
// The rule stays silent when the queue reader is not wired: "age
// unknown" must never read as "age healthy", but it must also never
// fabricate an alert from a number nobody measured.
func ruleJobPendingAgeHigh(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		if deps.Obs == nil {
			return nil, nil
		}
		age, configured, err := deps.Obs.OldestPendingJobAge(ctx, time.Now())
		if err != nil {
			return nil, fmt.Errorf("alert rule JobPendingAgeHigh: %w", err)
		}
		threshold := time.Duration(deps.JobPendingAgeSecs) * time.Second
		if !configured || threshold <= 0 || age < threshold {
			return nil, nil
		}
		return &runtimealerts.AlertEvent{
			RuleID:   "JobPendingAgeHigh",
			Severity: "warning",
			Summary: fmt.Sprintf("Oldest PENDING job waiting %s (threshold %s)",
				age.Round(time.Second), threshold),
			Description: fmt.Sprintf(
				"A job has been waiting in PENDING for %s with no worker lease taken. "+
					"Depth-only views read this as queue=1; check the job's prefetch events "+
					"(dispatch_status=prefetch_failed), worker capacity and placement rejections. "+
					"Prometheus twin: VeloxJobPendingAgeHigh on velox_jobs_oldest_pending_age_seconds.",
				age.Round(time.Second)),
			Labels: map[string]string{
				"domain":      "jobs",
				"age_seconds": fmt.Sprintf("%d", int64(age.Seconds())),
			},
		}, nil
	}
}

// rulePrefetchFailureSpike fires when the durable prefetch journal
// records more failures inside the window than the threshold — the
// leading indicator for the stuck jobs ruleJobPendingAgeHigh then
// catches. Prometheus twin: `VeloxPrefetchFailureSpike` on
// velox_prefetch_failures_total (alerts/job-queue-and-prefetch.yml);
// this rule reads the same fact from job_events because the alert
// engine sits outside the metrics registry.
func rulePrefetchFailureSpike(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) (*runtimealerts.AlertEvent, error) {
		if deps.Obs == nil {
			return nil, nil
		}
		window := deps.PrefetchWindow
		if window <= 0 {
			window = 15 * time.Minute
		}
		count, configured, err := deps.Obs.RecentPrefetchFailures(ctx, window)
		if err != nil {
			return nil, fmt.Errorf("alert rule PrefetchFailureSpike: %w", err)
		}
		threshold := deps.PrefetchFailureCount
		if threshold <= 0 {
			threshold = 3
		}
		if !configured || count < threshold {
			return nil, nil
		}
		return &runtimealerts.AlertEvent{
			RuleID:   "PrefetchFailureSpike",
			Severity: "warning",
			Summary: fmt.Sprintf("%d prefetch failures in the last %s (threshold %d)",
				count, window, threshold),
			Description: fmt.Sprintf(
				"Prefetch failed %d times in %s. Failed prefetch is the usual cause of jobs "+
					"stalling in PENDING (missing cached asset, expired lease) — check "+
					"velox_prefetch_failures_total{reason=...} and the worker cache. "+
					"Prometheus twin: VeloxPrefetchFailureSpike.",
				count, window),
			Labels: map[string]string{
				"domain": "jobs",
				"count":  fmt.Sprintf("%d", count),
			},
		}, nil
	}
}
