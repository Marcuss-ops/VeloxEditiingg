// Package alertengine / rules.go
//
// Rule evaluation functions. Each returns an *Alert when the
// condition is breached, nil when healthy.

package alertengine

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"

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
	return func(ctx context.Context) *Alert {
		if deps.Obs == nil {
			return nil
		}
		ov, err := deps.Obs.Overview(ctx)
		if err != nil {
			return nil
		}
		if ov.ErrorRate > deps.ErrorRatePct {
			return &Alert{
				Name:     "ErrorRateHigh",
				Severity: "warning",
				Summary:  fmt.Sprintf("Error rate %.1f%% exceeds threshold %.1f%%", ov.ErrorRate, deps.ErrorRatePct),
				Description: fmt.Sprintf(
					"Jobs completed: %d, failed: %d, rate: %.1f%%. Queue depth: %d.",
					ov.JobsCompleted24h, ov.JobsFailed24h, ov.ErrorRate, ov.QueueDepth,
				),
				Labels: map[string]string{"domain": "jobs"},
			}
		}
		return nil
	}
}

func ruleP95WallMs(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) *Alert {
		if deps.Obs == nil {
			return nil
		}
		ov, err := deps.Obs.Overview(ctx)
		if err != nil {
			return nil
		}
		if ov.P95RenderMS > deps.P95WallMs {
			return &Alert{
				Name:        "P95WallMsHigh",
				Severity:    "warning",
				Summary:     fmt.Sprintf("P95 render time %dms exceeds threshold %dms", ov.P95RenderMS, deps.P95WallMs),
				Description: fmt.Sprintf("P95 render: %dms. Active workers: %d.", ov.P95RenderMS, ov.ActiveWorkers),
				Labels:      map[string]string{"domain": "performance"},
			}
		}
		return nil
	}
}

func ruleWorkerOffline(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) *Alert {
		if deps.Obs == nil {
			return nil
		}
		workers, err := deps.Obs.ListWorkers(ctx)
		if err != nil {
			return nil
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
			return &Alert{
				Name:        "WorkersOffline",
				Severity:    "critical",
				Summary:     fmt.Sprintf("%d workers offline", len(offline)),
				Description: fmt.Sprintf("Offline workers: %v", offline),
				Labels:      map[string]string{"domain": "workers", "count": fmt.Sprintf("%d", len(offline))},
			}
		}
		return nil
	}
}

func ruleDiskFree(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) *Alert {
		dir := deps.DataDir
		if dir == "" {
			return nil
		}
		var stat syscall.Statfs_t
		if err := syscall.Statfs(dir, &stat); err != nil {
			return nil
		}
		freeGB := float64(stat.Bavail*uint64(stat.Bsize)) / 1_073_741_824.0
		if freeGB < deps.DiskFreeGB {
			return &Alert{
				Name:        "DiskFreeLow",
				Severity:    "critical",
				Summary:     fmt.Sprintf("Disk free %.1f GB below threshold %.1f GB on %s", freeGB, deps.DiskFreeGB, dir),
				Description: fmt.Sprintf("Available: %.1f GB. Block size: %d, available blocks: %d.", freeGB, stat.Bsize, stat.Bavail),
				Labels:      map[string]string{"domain": "infra", "path": dir},
			}
		}
		return nil
	}
}

func ruleFFmpegSpeedRatio(deps RuleDeps) RuleFunc {
	return func(ctx context.Context) *Alert {
		if deps.Obs == nil {
			return nil
		}
		// ffmpeg_speed_ratio is a scalar column on task_attempt_metrics,
		// not a phase timing. Use RecentScalarMetric which reads from
		// the correct table.
		result, err := deps.Obs.RecentScalarMetric(ctx, "ffmpeg_speed_ratio")
		if err != nil || result == nil || result.Samples == 0 {
			return nil
		}
		p95 := result.P95
		if p95 > 0 && p95 < deps.FFmpegMin {
			return &Alert{
				Name:        "FFmpegSpeedRatioLow",
				Severity:    "warning",
				Summary:     fmt.Sprintf("FFmpeg speed ratio P95 %.2fx below threshold %.2fx", p95, deps.FFmpegMin),
				Description: fmt.Sprintf("P95 ffmpeg speed ratio: %.2fx over %d samples.", p95, result.Samples),
				Labels:      map[string]string{"domain": "performance"},
			}
		}
		return nil
	}
}

// dockerDataDir returns the data directory path for disk-free checks.
func envDataDir() string {
	if d := os.Getenv("VELOX_DATA_DIR"); d != "" {
		return d
	}
	return "/velox-data"
}

// EnvFloat reads a float64 from an env var, returning the default when
// unset or unparseable.
func EnvFloat(key string, def float64) float64 {
	if s := os.Getenv(key); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return def
}
