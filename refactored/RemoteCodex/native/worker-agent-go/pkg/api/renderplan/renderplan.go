// Package renderplan provides the RenderPlan v1 contract for job validation.
//
// This implements the Phase 1 deliverable: unified contract with fail-fast validation.
// Jobs are validated before dispatch to runtime, rejecting invalid payloads early.
package renderplan

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"
)

// RenderPlanVersion is the current contract version.
const RenderPlanVersion = "v1"

// ============================================================================
// Typed Error Codes (Phase 2)
// ============================================================================

// ErrorCode represents a typed error code for render plan validation.
type ErrorCode string

const (
	// ERR_PLAN_SCHEMA - Schema validation failed (invalid structure, types, etc.)
	ERR_PLAN_SCHEMA ErrorCode = "ERR_PLAN_SCHEMA"
	// ERR_PLAN_REQUIRED_FIELD - Required field is missing
	ERR_PLAN_REQUIRED_FIELD ErrorCode = "ERR_PLAN_REQUIRED_FIELD"
	// ERR_PLAN_INCONSISTENT - Plan is inconsistent (e.g., no clips or no voiceover)
	ERR_PLAN_INCONSISTENT ErrorCode = "ERR_PLAN_INCONSISTENT"
)

// PlanError represents a typed error with code, field, and message.
type PlanError struct {
	Code    ErrorCode `json:"code"`
	Field   string    `json:"field,omitempty"`
	Message string    `json:"message"`
}

// Error implements the error interface.
func (e *PlanError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("[%s] %s: %s", e.Code, e.Field, e.Message)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// PlanErrors is a collection of plan errors.
type PlanErrors []*PlanError

// Error implements the error interface for multiple errors.
func (errs PlanErrors) Error() string {
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Sprintf("render plan validation failed: %s", strings.Join(msgs, "; "))
}

// HasErrors returns true if there are validation errors.
func (errs PlanErrors) HasErrors() bool {
	return len(errs) > 0
}

// Required fields for validation.
var requiredFields = []string{
	"job_id",
	"job_type",
	"created_at",
}

// ValidJobTypes defines the allowed job types.
var ValidJobTypes = map[string]bool{
	"render":        true,
	"process_video": true,
	"process_audio": true,
	"health_check":  true,
}

// ValidPriorities defines the allowed priority levels.
var ValidPriorities = map[int]bool{
	0: true, // Low
	1: true, // Normal
	2: true, // High
	3: true, // Critical
}

// ValidationError represents a contract validation failure.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   string `json:"value,omitempty"`
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Value != "" {
		return fmt.Sprintf("validation error on field '%s': %s (got: %s)", e.Field, e.Message, e.Value)
	}
	return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []*ValidationError

// Error implements the error interface for multiple errors.
func (errs ValidationErrors) Error() string {
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Sprintf("render plan validation failed: %s", strings.Join(msgs, "; "))
}

// HasErrors returns true if there are validation errors.
func (errs ValidationErrors) HasErrors() bool {
	return len(errs) > 0
}

// RenderPlan represents the v1 contract for job rendering.
type RenderPlan struct {
	// Contract metadata
	Version string `json:"version"`

	// Required fields
	JobID     string `json:"job_id"`
	JobType   string `json:"job_type"`
	CreatedAt string `json:"created_at"`

	// Optional fields
	JobRunID       string                 `json:"job_run_id,omitempty"`
	JobName        string                 `json:"job_name,omitempty"`
	Priority       int                    `json:"priority,omitempty"`
	MaxRetries     int                    `json:"max_retries,omitempty"`
	TimeoutSecs    int                    `json:"timeout_secs,omitempty"`
	AssignedWorker string                 `json:"assigned_worker,omitempty"`
	WorkerGroup    string                 `json:"worker_group,omitempty"`
	ParentJob      string                 `json:"parent_job,omitempty"`
	Parameters     map[string]interface{} `json:"parameters,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	Tags           []string               `json:"tags,omitempty"`

	// Validation timestamp
	ValidatedAt string `json:"validated_at,omitempty"`
}

// Validate performs fail-fast validation on the RenderPlan.
// Returns ValidationErrors if validation fails, nil if valid.
func (rp *RenderPlan) Validate() error {
	var errs ValidationErrors

	// Check required fields
	if rp.JobID == "" {
		errs = append(errs, &ValidationError{
			Field:   "job_id",
			Message: "is required",
		})
	}

	if rp.JobType == "" {
		errs = append(errs, &ValidationError{
			Field:   "job_type",
			Message: "is required",
		})
	} else if !ValidJobTypes[rp.JobType] {
		errs = append(errs, &ValidationError{
			Field:   "job_type",
			Message: fmt.Sprintf("must be one of: %s", strings.Join(validJobTypeNames(), ", ")),
			Value:   rp.JobType,
		})
	}

	if rp.CreatedAt == "" {
		errs = append(errs, &ValidationError{
			Field:   "created_at",
			Message: "is required",
		})
	} else {
		// Validate timestamp format
		if _, err := time.Parse(time.RFC3339, rp.CreatedAt); err != nil {
			errs = append(errs, &ValidationError{
				Field:   "created_at",
				Message: fmt.Sprintf("must be valid RFC3339 timestamp: %v", err),
				Value:   rp.CreatedAt,
			})
		}
	}

	// Validate optional fields if present
	if rp.Priority != 0 && !ValidPriorities[rp.Priority] {
		errs = append(errs, &ValidationError{
			Field:   "priority",
			Message: "must be 0 (low), 1 (normal), 2 (high), or 3 (critical)",
			Value:   fmt.Sprintf("%d", rp.Priority),
		})
	}

	if rp.MaxRetries < 0 {
		errs = append(errs, &ValidationError{
			Field:   "max_retries",
			Message: "must be >= 0",
			Value:   fmt.Sprintf("%d", rp.MaxRetries),
		})
	}

	if rp.TimeoutSecs < 0 {
		errs = append(errs, &ValidationError{
			Field:   "timeout_secs",
			Message: "must be >= 0",
			Value:   fmt.Sprintf("%d", rp.TimeoutSecs),
		})
	}

	// Validate job_run_id format if present
	if rp.JobRunID != "" && !isValidID(rp.JobRunID) {
		errs = append(errs, &ValidationError{
			Field:   "job_run_id",
			Message: "must be alphanumeric with hyphens/underscores",
			Value:   rp.JobRunID,
		})
	}

	if errs.HasErrors() {
		return errs
	}

	return nil
}

// SetDefaults applies default values to optional fields.
func (rp *RenderPlan) SetDefaults() {
	if rp.Version == "" {
		rp.Version = RenderPlanVersion
	}
	if rp.Priority == 0 {
		rp.Priority = 1 // Normal priority
	}
	if rp.MaxRetries == 0 {
		rp.MaxRetries = 3
	}
	if rp.TimeoutSecs == 0 {
		rp.TimeoutSecs = 1800 // 30 minutes
	}
	if rp.Parameters == nil {
		rp.Parameters = make(map[string]interface{})
	}
	if rp.Metadata == nil {
		rp.Metadata = make(map[string]interface{})
	}
	if rp.Tags == nil {
		rp.Tags = make([]string, 0)
	}
	rp.ValidatedAt = time.Now().UTC().Format(time.RFC3339)
}

// ValidateAndSetDefaults validates the plan and applies defaults.
// Returns error if validation fails.
func (rp *RenderPlan) ValidateAndSetDefaults() error {
	if err := rp.Validate(); err != nil {
		return err
	}
	rp.SetDefaults()
	return nil
}

// ToMap converts the RenderPlan to a map for API compatibility.
func (rp *RenderPlan) ToMap() map[string]interface{} {
	m := map[string]interface{}{
		"version":    rp.Version,
		"job_id":     rp.JobID,
		"job_type":   rp.JobType,
		"created_at": rp.CreatedAt,
	}

	if rp.JobRunID != "" {
		m["job_run_id"] = rp.JobRunID
	}
	if rp.JobName != "" {
		m["job_name"] = rp.JobName
	}
	if rp.Priority != 0 {
		m["priority"] = rp.Priority
	}
	if rp.MaxRetries != 0 {
		m["max_retries"] = rp.MaxRetries
	}
	if rp.TimeoutSecs != 0 {
		m["timeout_secs"] = rp.TimeoutSecs
	}
	if rp.AssignedWorker != "" {
		m["assigned_worker"] = rp.AssignedWorker
	}
	if rp.WorkerGroup != "" {
		m["worker_group"] = rp.WorkerGroup
	}
	if rp.ParentJob != "" {
		m["parent_job"] = rp.ParentJob
	}
	if len(rp.Parameters) > 0 {
		m["parameters"] = rp.Parameters
	}
	if len(rp.Metadata) > 0 {
		m["metadata"] = rp.Metadata
	}
	if len(rp.Tags) > 0 {
		m["tags"] = rp.Tags
	}
	if rp.ValidatedAt != "" {
		m["validated_at"] = rp.ValidatedAt
	}

	return m
}

// FromMap creates a RenderPlan from a map.
func FromMap(m map[string]interface{}) *RenderPlan {
	rp := &RenderPlan{
		Parameters: make(map[string]interface{}),
		Metadata:   make(map[string]interface{}),
		Tags:       make([]string, 0),
	}

	if v, ok := m["version"].(string); ok {
		rp.Version = v
	}
	if v, ok := m["job_id"].(string); ok {
		rp.JobID = v
	}
	if v, ok := m["job_type"].(string); ok {
		rp.JobType = v
	}
	if v, ok := m["created_at"].(string); ok {
		rp.CreatedAt = v
	}
	if v, ok := m["job_run_id"].(string); ok {
		rp.JobRunID = v
	}
	if v, ok := m["job_name"].(string); ok {
		rp.JobName = v
	}
	if v, ok := m["priority"].(float64); ok {
		rp.Priority = int(v)
	}
	if v, ok := m["max_retries"].(float64); ok {
		rp.MaxRetries = int(v)
	}
	if v, ok := m["timeout_secs"].(float64); ok {
		rp.TimeoutSecs = int(v)
	}
	if v, ok := m["assigned_worker"].(string); ok {
		rp.AssignedWorker = v
	}
	if v, ok := m["worker_group"].(string); ok {
		rp.WorkerGroup = v
	}
	if v, ok := m["parent_job"].(string); ok {
		rp.ParentJob = v
	}
	if v, ok := m["parameters"].(map[string]interface{}); ok {
		rp.Parameters = v
	}
	if v, ok := m["metadata"].(map[string]interface{}); ok {
		rp.Metadata = v
	}
	if v, ok := m["tags"].([]interface{}); ok {
		for _, item := range v {
			if s, ok := item.(string); ok {
				rp.Tags = append(rp.Tags, s)
			}
		}
	}

	return rp
}

// ============================================================================
// ValidateRenderPlan - Centralized Entrypoint (Phase 2)
// ============================================================================

// ValidateRenderPlanOptions contains options for render plan validation.
type ValidateRenderPlanOptions struct {
	// AllowLegacyVersionFallback allows fallback from version field if render_plan_version is missing
	AllowLegacyVersionFallback bool
}

// ValidateRenderPlan is the centralized entrypoint for render plan validation.
// It validates the plan structure, required fields, and consistency (ANY_CLIP + VOICEOVER rule).
func ValidateRenderPlan(plan *RenderPlan, opts *ValidateRenderPlanOptions) error {
	var errs PlanErrors

	// 1. Validate render_plan_version (required)
	if plan.Version == "" {
		if opts != nil && opts.AllowLegacyVersionFallback {
			// Fallback: use legacy version field if present
			plan.Version = RenderPlanVersion
		} else {
			errs = append(errs, &PlanError{
				Code:    ERR_PLAN_REQUIRED_FIELD,
				Field:   "render_plan_version",
				Message: "render_plan_version is required",
			})
		}
	}

	// 2. Validate required fields
	if plan.JobID == "" {
		errs = append(errs, &PlanError{
			Code:    ERR_PLAN_REQUIRED_FIELD,
			Field:   "job_id",
			Message: "job_id is required",
		})
	}

	if plan.JobType == "" {
		errs = append(errs, &PlanError{
			Code:    ERR_PLAN_REQUIRED_FIELD,
			Field:   "job_type",
			Message: "job_type is required",
		})
	} else if !ValidJobTypes[plan.JobType] {
		errs = append(errs, &PlanError{
			Code:    ERR_PLAN_SCHEMA,
			Field:   "job_type",
			Message: fmt.Sprintf("must be one of: %s", strings.Join(validJobTypeNames(), ", ")),
		})
	}

	if plan.CreatedAt == "" {
		errs = append(errs, &PlanError{
			Code:    ERR_PLAN_REQUIRED_FIELD,
			Field:   "created_at",
			Message: "created_at is required",
		})
	} else {
		if _, err := time.Parse(time.RFC3339, plan.CreatedAt); err != nil {
			errs = append(errs, &PlanError{
				Code:    ERR_PLAN_SCHEMA,
				Field:   "created_at",
				Message: fmt.Sprintf("must be valid RFC3339 timestamp: %v", err),
			})
		}
	}

	// 3. Validate ANY_CLIP/SCENE + VOICEOVER rule for render jobs
	if plan.JobType == "render" || plan.JobType == "process_video" {
		if err := validateAnyClipVoiceover(plan); err != nil {
			errs = append(errs, err)
		}
	}

	if errs.HasErrors() {
		return errs
	}

	return nil
}

// validateAnyClipVoiceover validates the ANY_CLIP/SCENE + VOICEOVER rule.
// Rule: At least one clip or scene payload AND at least one voiceover must be present.
func validateAnyClipVoiceover(plan *RenderPlan) *PlanError {
	params := plan.Parameters
	if params == nil {
		return &PlanError{
			Code:    ERR_PLAN_INCONSISTENT,
			Field:   "parameters",
			Message: "parameters are required for render jobs",
		}
	}

	// Check for ANY_CLIP: at least one of start_clip_paths, middle_clip_paths, end_clip_paths, stock_clip_paths
	hasClip := false
	clipFields := []string{"start_clip_paths", "middle_clip_paths", "end_clip_paths", "stock_clip_paths"}
	for _, field := range clipFields {
		if clips, ok := params[field]; ok {
			if clipList, ok := clips.([]interface{}); ok && len(clipList) > 0 {
				hasClip = true
				break
			}
			if clipList, ok := clips.([]string); ok && len(clipList) > 0 {
				hasClip = true
				break
			}
		}
	}

	// Check for SCENE payloads: scenes_json or a pre-normalized scenes array/map.
	hasScenePlan := false
	sceneFields := []string{"scenes_json", "scenes", "scene_json", "scene_plan"}
	for _, field := range sceneFields {
		if val, ok := params[field]; ok {
			switch v := val.(type) {
			case []interface{}:
				if len(v) > 0 {
					hasScenePlan = true
				}
			case []string:
				if len(v) > 0 {
					hasScenePlan = true
				}
			case map[string]interface{}:
				if len(v) > 0 {
					hasScenePlan = true
				}
			case string:
				if strings.TrimSpace(v) != "" {
					hasScenePlan = true
				}
			}
		}
		if hasScenePlan {
			break
		}
	}

	// Check for VOICEOVER: at least one voiceover path
	hasVoiceover := false
	voiceoverFields := []string{"voiceover_paths", "audio_path", "voiceover_path"}
	for _, field := range voiceoverFields {
		if vo, ok := params[field]; ok {
			if voList, ok := vo.([]interface{}); ok && len(voList) > 0 {
				hasVoiceover = true
				break
			}
			if voList, ok := vo.([]string); ok && len(voList) > 0 {
				hasVoiceover = true
				break
			}
			if voStr, ok := vo.(string); ok && voStr != "" {
				hasVoiceover = true
				break
			}
		}
	}

	if !hasClip && !hasScenePlan {
		return &PlanError{
			Code:    ERR_PLAN_INCONSISTENT,
			Field:   "parameters",
			Message: "at least one clip path or scenes payload is required (start_clip_paths, middle_clip_paths, end_clip_paths, stock_clip_paths, or scenes_json)",
		}
	}

	if !hasVoiceover {
		return &PlanError{
			Code:    ERR_PLAN_INCONSISTENT,
			Field:   "parameters",
			Message: "at least one voiceover path is required (voiceover_paths, audio_path, or voiceover_path)",
		}
	}

	return nil
}

// GenerateIdempotencyKey generates a deterministic idempotency key from job_id, job_run_id, and operation.
func GenerateIdempotencyKey(jobID, jobRunID, operation string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s:%s:%s", jobID, jobRunID, operation)))
	return fmt.Sprintf("%x", h.Sum(nil))[:32]
}

// Helper functions

func validJobTypeNames() []string {
	names := make([]string, 0, len(ValidJobTypes))
	for k := range ValidJobTypes {
		names = append(names, k)
	}
	return names
}

func isValidID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
