// Package taskattempts defines types for execution reports and phase metrics.
package taskattempts

import "time"

// Canonical phase names for production rendering pipeline.
// Workers must use these exact strings; free-form identifiers are rejected.
var CanonicalPhases = []string{
	"queue",
	"asset_wait",
	"cache_lookup",
	"download",
	"decode",
	"compile",
	"simulate",
	"render",
	"composite",
	"encode",
	"upload",
	"finalize",
}

// IsCanonicalPhase reports whether the given phase name is a valid canonical phase.
func IsCanonicalPhase(phase string) bool {
	for _, p := range CanonicalPhases {
		if p == phase {
			return true
		}
	}
	return false
}

// PhaseTiming records the duration of a single canonical phase for one attempt.
type PhaseTiming struct {
	AttemptID  string    `json:"attempt_id"`
	Phase      string    `json:"phase"`
	DurationMS int64     `json:"duration_ms"`
	WallStart  time.Time `json:"wall_start"`
	WallEnd    time.Time `json:"wall_end"`
}

// AttemptMetrics stores typed resource counters for one attempt.
type AttemptMetrics struct {
	AttemptID           string `json:"attempt_id"`
	InputBytes          int64  `json:"input_bytes"`
	OutputBytes         int64  `json:"output_bytes"`
	BytesFromDrive      int64  `json:"bytes_from_drive"`
	BytesFromBlobstore  int64  `json:"bytes_from_blobstore"`
	BytesFromLocalCache int64  `json:"bytes_from_local_cache"`
	CPUTimeMS           int64  `json:"cpu_time_ms"`
	GPUTimeMS           int64  `json:"gpu_time_ms"`
	PeakRSSBytes        int64  `json:"peak_rss_bytes"`
	PeakVRAMBytes       int64  `json:"peak_vram_bytes"`
}

// TaskExecutionReport is the typed, versioned, final report a worker emits
// after completing (or failing) a task attempt. The master validates all
// identity fields before persistence.
type TaskExecutionReport struct {
	ContractVersion int            `json:"contract_version"`
	TaskID          string         `json:"task_id"`
	AttemptID       string         `json:"attempt_id"`
	WorkerID        string         `json:"worker_id"`
	LeaseID         string         `json:"lease_id"`
	Status          AttemptStatus  `json:"status"`
	PhaseTimings    []PhaseTiming  `json:"phase_timings"`
	Metrics         AttemptMetrics `json:"metrics"`
	ErrorCode       string         `json:"error_code,omitempty"`
	ErrorMessage    string         `json:"error_message,omitempty"`
	SubmittedAt     time.Time      `json:"submitted_at"`
}
