// Package orchestratorv1 — standalone DTO types for the
// /api/v1/orchestrator/* HTTP endpoints. These mirror the legacy
// workflow.Run / workflow.Step / workflow.StatsReport JSON shapes
// WITHOUT importing the workflow package, so the HTTP edge is fully
// decoupled from the internal/workflow domain.
//
// All types carry explicit JSON tags matching the existing wire format
// (Go default capitalization = workflow types had no json tags), so the
// JSON output is byte-for-byte identical.
//
// Deprecated: this whole adapter is scheduled for removal by 2026-12-31
// (see orchestrator_legacy_adapter.go).
package orchestratorv1

import "time"

// LegacyRunStatus mirrors workflow.RunStatus.
type LegacyRunStatus string

const (
	LegacyRunStatusPending   LegacyRunStatus = "PENDING"
	LegacyRunStatusRunning   LegacyRunStatus = "RUNNING"
	LegacyRunStatusSucceeded LegacyRunStatus = "SUCCEEDED"
	LegacyRunStatusFailed    LegacyRunStatus = "FAILED"
	LegacyRunStatusCancelled LegacyRunStatus = "CANCELLED"
)

// LegacyStepStatus mirrors workflow.StepStatus.
type LegacyStepStatus string

const (
	LegacyStepStatusBlocked   LegacyStepStatus = "BLOCKED"
	LegacyStepStatusReady     LegacyStepStatus = "READY"
	LegacyStepStatusRunning   LegacyStepStatus = "RUNNING"
	LegacyStepStatusSucceeded LegacyStepStatus = "SUCCEEDED"
	LegacyStepStatusFailed    LegacyStepStatus = "FAILED"
)

// LegacyRunResponse is the JSON shape of GET /orchestrator/jobs (list)
// and GET /orchestrator/jobs/:id (single). Mirrors workflow.Run.
type LegacyRunResponse struct {
	RunID            string          `json:"RunID"`
	WorkflowType     string          `json:"WorkflowType"`
	Status           LegacyRunStatus `json:"Status"`
	Input            map[string]any  `json:"Input"`
	Output           map[string]any  `json:"Output"`
	Revision         int64           `json:"Revision"`
	CreatedAt        time.Time       `json:"CreatedAt"`
	UpdatedAt        time.Time       `json:"UpdatedAt"`
	StartedAt        *time.Time      `json:"StartedAt"`
	CompletedAt      *time.Time      `json:"CompletedAt"`
	LastErrorCode    string          `json:"LastErrorCode"`
	LastErrorMessage string          `json:"LastErrorMessage"`
}

// LegacyStepResponse is the JSON shape of GET /orchestrator/jobs/:id
// (steps array). Mirrors workflow.Step.
type LegacyStepResponse struct {
	StepID       string           `json:"StepID"`
	RunID        string           `json:"RunID"`
	StepKey      string           `json:"StepKey"`
	JobID        *string          `json:"JobID"`
	Status       LegacyStepStatus `json:"Status"`
	Attempt      int              `json:"Attempt"`
	MaxAttempts  int              `json:"MaxAttempts"`
	Input        map[string]any   `json:"Input"`
	Output       map[string]any   `json:"Output"`
	Revision     int64            `json:"Revision"`
	CreatedAt    time.Time        `json:"CreatedAt"`
	UpdatedAt    time.Time        `json:"UpdatedAt"`
	StartedAt    *time.Time       `json:"StartedAt"`
	CompletedAt  *time.Time       `json:"CompletedAt"`
	ErrorCode    string           `json:"ErrorCode"`
	ErrorMessage string           `json:"ErrorMessage"`
}

// LegacyStatsReport is the JSON shape of GET /orchestrator/stats.
// Mirrors workflow.StatsReport.
type LegacyStatsReport struct {
	TotalRuns     int                      `json:"TotalRuns"`
	RunsByStatus  map[LegacyRunStatus]int  `json:"RunsByStatus"`
	TotalSteps    int                      `json:"TotalSteps"`
	StepsByStatus map[LegacyStepStatus]int `json:"StepsByStatus"`
}
