package executors

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/api/renderplan"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// validCompiledPlanForTest mirrors the master CompiledRenderPlan document
// (DataServer/internal/renderplan) delivered at claim.
func validCompiledPlanForTest(t *testing.T, jobID, attemptID string) string {
	t.Helper()
	doc := map[string]interface{}{
		"plan_version": renderplan.CompiledPlanVersion,
		"job_id":       jobID,
		"attempt_id":   attemptID,
		"duration_ms":  60000,
		"media_contract": map[string]interface{}{
			"video_codec": "h264", "width": 1920, "height": 1080,
			"fps_num": 30, "fps_den": 1,
		},
		"segments": []interface{}{
			map[string]interface{}{
				"segment_id": "seg_000", "asset_id": "asset-a",
				"source_in_ms": 12000, "source_out_ms": 19000, "timeline_start_ms": 0,
			},
		},
		"assets": []interface{}{map[string]interface{}{"asset_id": "asset-a"}},
	}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal compiled plan: %v", err)
	}
	return string(data)
}

func TestParseCompiledRenderPlanEnvelope_HappyPath(t *testing.T) {
	const jobID = "job-compiled-1"
	plan, err := parseCompiledRenderPlanEnvelope(executor.TaskSpec{
		JobID: jobID,
		Payload: map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: validCompiledPlanForTest(t, jobID, "attempt-1"),
			contract.PayloadKeyCompiledRenderPlanSHA:  strings.Repeat("a", 64),
		},
	})
	if err != nil {
		t.Fatalf("parseCompiledRenderPlanEnvelope: %v", err)
	}
	if plan == nil || plan.JobID != jobID || plan.AttemptID != "attempt-1" || len(plan.Segments) != 1 {
		t.Fatalf("parsed plan = %+v", plan)
	}
}

func TestParseCompiledRenderPlanEnvelope_JobMismatch(t *testing.T) {
	_, err := parseCompiledRenderPlanEnvelope(executor.TaskSpec{
		JobID: "job-other",
		Payload: map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: validCompiledPlanForTest(t, "job-compiled-1", "attempt-1"),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "must match task job") {
		t.Fatalf("expected job mismatch error, got %v", err)
	}
}

func TestParseCompiledRenderPlanEnvelope_Missing(t *testing.T) {
	if _, err := parseCompiledRenderPlanEnvelope(executor.TaskSpec{Payload: map[string]interface{}{"job_id": "x"}}); err == nil {
		t.Fatal("payload without compiled plan must error")
	}
}

func TestRenderPlanExecutor_ValidateRequiresV1Envelope(t *testing.T) {
	e := NewCompose(nil, t.TempDir())
	const jobID = "job-compiled-2"

	// The commands today are driven by the v1 envelope; a compiled-plan-only
	// payload is NOT executable by this executor family yet (the batch
	// executor is a future executor ID). Validate must fail honestly.
	compiledOnly := executor.TaskSpec{
		JobID: jobID,
		Payload: map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: validCompiledPlanForTest(t, jobID, "attempt-2"),
		},
	}
	if err := e.Validate(compiledOnly); err == nil {
		t.Fatal("compiled-plan-only payload must fail Validate until the batch commands exist")
	}

	// v1 + valid compiled plan → pass; the compiled doc must be valid too.
	ok := executor.TaskSpec{
		JobID: jobID,
		Payload: map[string]interface{}{
			"render_plan_json":                          validRenderPlanJSON(t, jobID),
			contract.PayloadKeyCompiledRenderPlanJSON: validCompiledPlanForTest(t, jobID, "attempt-2"),
		},
	}
	if err := e.Validate(ok); err != nil {
		t.Fatalf("v1 + valid compiled plan must pass Validate: %v", err)
	}

	// v1 + INVALID compiled plan → rejected (chain integrity on the wire).
	bad := executor.TaskSpec{
		JobID: jobID,
		Payload: map[string]interface{}{
			"render_plan_json":                          validRenderPlanJSON(t, jobID),
			contract.PayloadKeyCompiledRenderPlanJSON: `{"plan_version": 99}`,
		},
	}
	if err := e.Validate(bad); err == nil {
		t.Fatal("invalid compiled plan must fail Validate even with a valid v1 envelope")
	}

	// Unknown keys are still rejected (the compiled keys join the allowlist).
	unknown := executor.TaskSpec{JobID: jobID, Payload: map[string]interface{}{
		"render_plan_json": validRenderPlanJSON(t, jobID),
		"mystery_key":      "x",
	}}
	if err := e.Validate(unknown); err == nil {
		t.Fatal("unknown payload key must still be rejected")
	}
}

func TestRunCommandExecutor_SurfacesCompiledPlanEvidence(t *testing.T) {
	const jobID = "job-evidence-1"
	outputRoot := t.TempDir()
	fake := &fakeFFmpegRunner{result: ffmpegrunner.FFmpegResult{ExitCode: 0}}
	output := filepath.Join(outputRoot, jobID+".mp4")
	seedOutputFile(t, output)

	e := NewEncode(fake, outputRoot)
	spec := executor.TaskSpec{
		Version:    1,
		JobID:      jobID,
		ExecutorID: EncodeID,
		Payload: map[string]interface{}{
			"render_plan_json": validRenderPlanJSON(t, jobID),
			"input_path":       "/cache/worker/video.mp4",
			// Master-compiled plan delivered at claim (Fase D).
			contract.PayloadKeyCompiledRenderPlanJSON: validCompiledPlanForTest(t, jobID, "attempt-evidence"),
			contract.PayloadKeyCompiledRenderPlanSHA:  strings.Repeat("c", 64),
		},
	}
	result, err := e.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("Execute = %v, want nil", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("Status = %q, want succeeded (%s)", result.Status, result.ErrorDetail)
	}
	if got, _ := result.Metrics["compiled_render_plan_sha256"].(string); got != strings.Repeat("c", 64) {
		t.Errorf("compiled_render_plan_sha256 = %v, want delivered sha", result.Metrics["compiled_render_plan_sha256"])
	}
	if got, _ := result.Metrics["compiled_render_plan_segments"].(int); got != 1 {
		t.Errorf("compiled_render_plan_segments = %v, want 1", result.Metrics["compiled_render_plan_segments"])
	}
	if got, _ := result.Metrics["compiled_render_plan_version"].(int); got != renderplan.CompiledPlanVersion {
		t.Errorf("compiled_render_plan_version = %v, want %d", result.Metrics["compiled_render_plan_version"], renderplan.CompiledPlanVersion)
	}
}

func TestCompiledPlanEvidence_AbsentPayloadReturnsNil(t *testing.T) {
	if got := compiledPlanEvidence(executor.TaskSpec{Payload: map[string]interface{}{"render_plan_json": "{}"}}); got != nil {
		t.Fatalf("evidence without compiled plan = %v, want nil", got)
	}
}
