package renderplan

import (
	"strings"
	"testing"
	"time"
)

// TestValidateRenderPlan_RequiredField_MissingVersion tests that missing render_plan_version fails with ERR_PLAN_REQUIRED_FIELD
func TestValidateRenderPlan_RequiredField_MissingVersion(t *testing.T) {
	plan := &RenderPlan{
		JobID:     "job-123",
		JobType:   "render",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Parameters: map[string]interface{}{
			"start_clip_paths": []string{"/path/to/clip.mp4"},
			"voiceover_paths":  []string{"/path/to/voiceover.mp3"},
		},
	}

	err := ValidateRenderPlan(plan)
	if err == nil {
		t.Fatal("Expected error for missing render_plan_version, got nil")
	}

	planErrs, ok := err.(PlanErrors)
	if !ok {
		t.Fatalf("Expected PlanErrors, got %T", err)
	}

	found := false
	for _, planErr := range planErrs {
		if planErr.Code == ERR_PLAN_REQUIRED_FIELD && planErr.Field == "render_plan_version" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected ERR_PLAN_REQUIRED_FIELD for render_plan_version, got: %v", err)
	}
}

// TestValidateRenderPlan_Inconsistent_NoClip tests that missing clips fails with ERR_PLAN_INCONSISTENT
func TestValidateRenderPlan_Inconsistent_NoClip(t *testing.T) {
	plan := &RenderPlan{
		Version:   "v1",
		JobID:     "job-123",
		JobType:   "render",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Parameters: map[string]interface{}{
			"voiceover_paths": []string{"/path/to/voiceover.mp3"},
		},
	}

	err := ValidateRenderPlan(plan)
	if err == nil {
		t.Fatal("Expected error for missing clips, got nil")
	}

	planErrs, ok := err.(PlanErrors)
	if !ok {
		t.Fatalf("Expected PlanErrors, got %T", err)
	}

	found := false
	for _, planErr := range planErrs {
		if planErr.Code == ERR_PLAN_INCONSISTENT && strings.Contains(planErr.Message, "clip") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected ERR_PLAN_INCONSISTENT for missing clips, got: %v", err)
	}
}

// TestValidateRenderPlan_Inconsistent_NoVoiceover tests that missing voiceover fails with ERR_PLAN_INCONSISTENT
func TestValidateRenderPlan_Inconsistent_NoVoiceover(t *testing.T) {
	plan := &RenderPlan{
		Version:   "v1",
		JobID:     "job-123",
		JobType:   "render",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Parameters: map[string]interface{}{
			"start_clip_paths": []string{"/path/to/clip.mp4"},
		},
	}

	err := ValidateRenderPlan(plan)
	if err == nil {
		t.Fatal("Expected error for missing voiceover, got nil")
	}

	planErrs, ok := err.(PlanErrors)
	if !ok {
		t.Fatalf("Expected PlanErrors, got %T", err)
	}

	found := false
	for _, planErr := range planErrs {
		if planErr.Code == ERR_PLAN_INCONSISTENT && strings.Contains(planErr.Message, "voiceover") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected ERR_PLAN_INCONSISTENT for missing voiceover, got: %v", err)
	}
}

// TestValidateRenderPlan_Valid_MiddleClips tests that middle_clips + voiceover passes validation
func TestValidateRenderPlan_Valid_MiddleClips(t *testing.T) {
	plan := &RenderPlan{
		Version:   "v1",
		JobID:     "job-123",
		JobType:   "render",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Parameters: map[string]interface{}{
			"middle_clip_paths": []string{"/path/to/middle.mp4"},
			"voiceover_paths":   []string{"/path/to/voiceover.mp3"},
		},
	}

	err := ValidateRenderPlan(plan)
	if err != nil {
		t.Fatalf("Expected no error for valid plan with middle_clips + voiceover, got: %v", err)
	}
}

// TestValidateRenderPlan_Valid_StockClips tests that stock_clips + voiceover passes validation
func TestValidateRenderPlan_Valid_StockClips(t *testing.T) {
	plan := &RenderPlan{
		Version:   "v1",
		JobID:     "job-123",
		JobType:   "render",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Parameters: map[string]interface{}{
			"stock_clip_paths": []string{"/path/to/stock.mp4"},
			"voiceover_paths":  []string{"/path/to/voiceover.mp3"},
		},
	}

	err := ValidateRenderPlan(plan)
	if err != nil {
		t.Fatalf("Expected no error for valid plan with stock_clips + voiceover, got: %v", err)
	}
}

// TestValidateRenderPlan_HealthCheck tests that health_check jobs don't require clips/voiceover
func TestValidateRenderPlan_HealthCheck(t *testing.T) {
	plan := &RenderPlan{
		Version:   "v1",
		JobID:     "job-123",
		JobType:   "health_check",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Parameters: map[string]interface{}{},
	}

	err := ValidateRenderPlan(plan)
	if err != nil {
		t.Fatalf("Expected no error for health_check job, got: %v", err)
	}
}

// TestGenerateIdempotencyKey tests idempotency key generation
func TestGenerateIdempotencyKey(t *testing.T) {
	key1 := GenerateIdempotencyKey("job-123", "run-456", "get")
	key2 := GenerateIdempotencyKey("job-123", "run-456", "get")
	key3 := GenerateIdempotencyKey("job-123", "run-456", "start")

	if key1 != key2 {
		t.Errorf("Expected same idempotency key for same inputs, got %s != %s", key1, key2)
	}

	if key1 == key3 {
		t.Errorf("Expected different idempotency key for different operations, got %s == %s", key1, key3)
	}

	if len(key1) != 32 {
		t.Errorf("Expected idempotency key length 32, got %d", len(key1))
	}
}

// TestPlanError_Error tests PlanError.Error() method
func TestPlanError_Error(t *testing.T) {
	err := &PlanError{
		Code:    ERR_PLAN_REQUIRED_FIELD,
		Field:   "job_id",
		Message: "is required",
	}

	expected := "[ERR_PLAN_REQUIRED_FIELD] job_id: is required"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

// TestPlanErrors_Error tests PlanErrors.Error() method
func TestPlanErrors_Error(t *testing.T) {
	errs := PlanErrors{
		&PlanError{Code: ERR_PLAN_REQUIRED_FIELD, Field: "job_id", Message: "is required"},
		&PlanError{Code: ERR_PLAN_INCONSISTENT, Message: "no clips"},
	}

	errStr := errs.Error()
	if !strings.Contains(errStr, "ERR_PLAN_REQUIRED_FIELD") {
		t.Errorf("Expected error string to contain ERR_PLAN_REQUIRED_FIELD, got: %s", errStr)
	}
	if !strings.Contains(errStr, "ERR_PLAN_INCONSISTENT") {
		t.Errorf("Expected error string to contain ERR_PLAN_INCONSISTENT, got: %s", errStr)
	}
}
