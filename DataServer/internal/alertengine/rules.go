// Package alertengine / rules.go
//
// Rule evaluation functions. Each returns a canonical runtime AlertEvent
// when the condition is breached, nil when healthy.

package alertengine

import (
	"context"
	"fmt"
	"syscall"

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
}

// DefaultRuleDeps returns RuleDeps with safe defaults.
func DefaultRuleDeps() RuleDeps {
	return RuleDeps{
		ErrorRatePct: 5.0,
		P95WallMs:    300_000,
		DiskFreeGB:   10.0,
		FFmpegMin:    1.5,
	}
}

// MakeRules creates the standard set of 5 alert rules.
func MakeRules(deps RuleDeps) []RuleFunc {
	return []RuleFunc{
		ruleErrorRate(deps),
		ruleP95WallMs(deps),
		ruleWorkerOffline(deps),
		ruleDiskFree(deps),
		ruleFFmpegSpeedRatio(deps),
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
