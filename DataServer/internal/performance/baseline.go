// Package performance defines domain types for historical performance
// baselines used by the Metrics Center to compare current attempts against
// previous versions of the code, configuration and worker class.
package performance

import "time"

// Baseline stores aggregated percentile metrics for a comparable workload
// class. The combination of (workload_key, git_sha, config_hash, worker_class)
// is unique: re-computing a baseline for the same tuple replaces the
// previous row.
type Baseline struct {
	BaselineID      string    `json:"baseline_id"`
	WorkloadKey     string    `json:"workload_key"`
	GitSHA          string    `json:"git_sha"`
	ConfigHash      string    `json:"config_hash"`
	WorkerClass     string    `json:"worker_class"`
	SampleCount     int       `json:"sample_count"`
	P50WallMs       float64   `json:"p50_wall_ms"`
	P95WallMs       float64   `json:"p95_wall_ms"`
	P50RenderFactor float64   `json:"p50_render_factor"`
	P95RenderFactor float64   `json:"p95_render_factor"`
	ErrorRate       float64   `json:"error_rate"`
	CreatedAt       time.Time `json:"created_at"`
}
