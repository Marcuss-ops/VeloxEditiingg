// Package renderplan: render-plan validation logic.
//
// The error aggregation uses pkg/validation.FieldErrors so other Velox
// components can reuse the same pattern. Business rules (which job types
// need clips and voiceover) live here because they are render-specific.
package renderplan

import (
	"fmt"
	"strings"
	"time"

	"velox-worker-agent/pkg/validation"
)

var requiredFields = []string{"job_id", "job_type", "created_at"}

var ValidJobTypes = map[string]bool{
	"render":        true,
	"process_video": true,
	"process_audio": true,
	"health_check":  true,
}

var ValidPriorities = map[int]bool{
	0: true, // Low
	1: true, // Normal
	2: true, // High
	3: true, // Critical
}

// Validate performs fail-fast validation on the RenderPlan.
func (rp *RenderPlan) Validate() error {
	errs := validation.FieldErrors{}

	if rp.JobID == "" {
		errs.AddMsg("job_id", "is required")
	}
	if rp.JobType == "" {
		errs.AddMsg("job_type", "is required")
	} else if !ValidJobTypes[rp.JobType] {
		errs.Add("job_type",
			fmt.Sprintf("must be one of: %s", strings.Join(validJobTypeNames(), ", ")),
			rp.JobType)
	}
	if rp.CreatedAt == "" {
		errs.AddMsg("created_at", "is required")
	} else if _, err := time.Parse(time.RFC3339, rp.CreatedAt); err != nil {
		errs.Add("created_at",
			fmt.Sprintf("must be valid RFC3339 timestamp: %v", err),
			rp.CreatedAt)
	}
	if rp.Priority != 0 && !ValidPriorities[rp.Priority] {
		errs.Add("priority",
			"must be 0 (low), 1 (normal), 2 (high), or 3 (critical)",
			fmt.Sprintf("%d", rp.Priority))
	}
	if rp.MaxRetries < 0 {
		errs.Add("max_retries", "must be >= 0", fmt.Sprintf("%d", rp.MaxRetries))
	}
	if rp.TimeoutSecs < 0 {
		errs.Add("timeout_secs", "must be >= 0", fmt.Sprintf("%d", rp.TimeoutSecs))
	}
	if rp.JobRunID != "" && !validation.IsAlphanumericID(rp.JobRunID) {
		errs.Add("job_run_id", "must be alphanumeric with hyphens/underscores", rp.JobRunID)
	}

	return errs.OrNil()
}

// SetDefaults applies default values to optional fields.
func (rp *RenderPlan) SetDefaults() {
	if rp.Version == "" {
		rp.Version = RenderPlanVersion
	}
	if rp.Priority == 0 {
		rp.Priority = 1
	}
	if rp.MaxRetries == 0 {
		rp.MaxRetries = 3
	}
	if rp.TimeoutSecs == 0 {
		rp.TimeoutSecs = 1800
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

// ValidateAndSetDefaults validates and applies defaults.
func (rp *RenderPlan) ValidateAndSetDefaults() error {
	if err := rp.Validate(); err != nil {
		return err
	}
	rp.SetDefaults()
	return nil
}

// ValidateRenderPlan is the centralized entrypoint for render plan validation.
func ValidateRenderPlan(plan *RenderPlan) error {
	errs := PlanErrors{}

	if plan.Version == "" {
		errs = append(errs, &PlanError{
			Code: ERR_PLAN_REQUIRED_FIELD, Field: "render_plan_version", Message: "render_plan_version is required",
		})
	}
	if plan.JobID == "" {
		errs = append(errs, &PlanError{Code: ERR_PLAN_REQUIRED_FIELD, Field: "job_id", Message: "job_id is required"})
	}
	if plan.JobType == "" {
		errs = append(errs, &PlanError{Code: ERR_PLAN_REQUIRED_FIELD, Field: "job_type", Message: "job_type is required"})
	} else if !ValidJobTypes[plan.JobType] {
		errs = append(errs, &PlanError{
			Code: ERR_PLAN_SCHEMA, Field: "job_type",
			Message: fmt.Sprintf("must be one of: %s", strings.Join(validJobTypeNames(), ", ")),
		})
	}
	if plan.CreatedAt == "" {
		errs = append(errs, &PlanError{Code: ERR_PLAN_REQUIRED_FIELD, Field: "created_at", Message: "created_at is required"})
	} else if _, err := time.Parse(time.RFC3339, plan.CreatedAt); err != nil {
		errs = append(errs, &PlanError{
			Code: ERR_PLAN_SCHEMA, Field: "created_at",
			Message: fmt.Sprintf("must be valid RFC3339 timestamp: %v", err),
		})
	}

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

func validateAnyClipVoiceover(plan *RenderPlan) *PlanError {
	params := plan.Parameters
	if params == nil {
		return &PlanError{
			Code: ERR_PLAN_INCONSISTENT, Field: "parameters",
			Message: "parameters are required for render jobs",
		}
	}

	hasClip := false
	clipFields := []string{"intro_clip_paths", "start_clip_paths", "middle_clip_paths", "end_clip_paths", "stock_clip_paths", "clip_segments"}
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
			Code: ERR_PLAN_INCONSISTENT, Field: "parameters",
			Message: "at least one clip path or scenes payload is required (intro_clip_paths, start_clip_paths, middle_clip_paths, end_clip_paths, stock_clip_paths, clip_segments, or scenes_json)",
		}
	}
	if !hasVoiceover {
		return &PlanError{
			Code: ERR_PLAN_INCONSISTENT, Field: "parameters",
			Message: "at least one voiceover path is required (voiceover_paths, audio_path, or voiceover_path)",
		}
	}
	return nil
}

func validJobTypeNames() []string {
	names := make([]string, 0, len(ValidJobTypes))
	for k := range ValidJobTypes {
		names = append(names, k)
	}
	return names
}
