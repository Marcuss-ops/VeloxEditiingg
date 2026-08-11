package renderplan

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func validCompiledPlanJSON(t *testing.T, jobID, attemptID string) string {
	t.Helper()
	doc := map[string]interface{}{
		"plan_version": CompiledPlanVersion,
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
		"assets": []interface{}{map[string]interface{}{"asset_id": "asset-a", "sha256": strings.Repeat("a", 64)}},
	}
	data := marshalJSONForTest(t, doc)
	return data
}

func marshalJSONForTest(t *testing.T, value interface{}) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}

func TestDecodeCompiledRenderPlan_HappyPath(t *testing.T) {
	plan, err := DecodeCompiledRenderPlan(validCompiledPlanJSON(t, "job-1", "attempt-1"))
	if err != nil {
		t.Fatalf("DecodeCompiledRenderPlan: %v", err)
	}
	if plan.PlanVersion != CompiledPlanVersion || plan.JobID != "job-1" || plan.AttemptID != "attempt-1" {
		t.Fatalf("identity = %+v", plan)
	}
	if plan.MediaContract.Width != 1920 || plan.MediaContract.FpsDen != 1 {
		t.Fatalf("media contract = %+v", plan.MediaContract)
	}
	if len(plan.Segments) != 1 || plan.Segments[0].SegmentID != "seg_000" || plan.Segments[0].TimelineStartMS != 0 {
		t.Fatalf("segments = %+v", plan.Segments)
	}
}

func TestDecodeCompiledRenderPlan_RejectsInvalidDocuments(t *testing.T) {
	base := map[string]interface{}{
		"plan_version": CompiledPlanVersion,
		"job_id":       "job-1",
		"attempt_id":   "attempt-1",
		"duration_ms":  60000,
		"media_contract": map[string]interface{}{
			"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1,
		},
		"segments": []interface{}{
			map[string]interface{}{"segment_id": "seg_000", "asset_id": "asset-a", "timeline_start_ms": 0},
		},
	}
	mutate := func(fn func(m map[string]interface{})) string {
		cp := map[string]interface{}{}
		for k, v := range base {
			cp[k] = v
		}
		fn(cp)
		return marshalJSONForTest(t, cp)
	}
	cases := []struct {
		name string
		json string
	}{
		{"unsupported version", mutate(func(m map[string]interface{}) { m["plan_version"] = 99 })},
		{"missing job_id", mutate(func(m map[string]interface{}) { m["job_id"] = "" })},
		{"missing attempt_id", mutate(func(m map[string]interface{}) { m["attempt_id"] = "" })},
		{"zero fps_den", mutate(func(m map[string]interface{}) { m["media_contract"] = map[string]interface{}{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 0} })},
		{"empty segments", mutate(func(m map[string]interface{}) { m["segments"] = []interface{}{} })},
		{"segment without asset_id", mutate(func(m map[string]interface{}) { m["segments"] = []interface{}{map[string]interface{}{"segment_id": "seg_000", "timeline_start_ms": 0}} })},
		{"negative timeline offset", mutate(func(m map[string]interface{}) { m["segments"] = []interface{}{map[string]interface{}{"segment_id": "seg_000", "asset_id": "a", "timeline_start_ms": -5}} })},
		{"malformed json", `{"plan_version": 1,`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeCompiledRenderPlan(tc.json); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
	if _, err := DecodeCompiledRenderPlan("  "); err == nil {
		t.Fatal("empty document must fail")
	}
}

func TestValidateCompiledRenderPlan_Admission(t *testing.T) {
	valid := validCompiledPlanJSON(t, "job-1", "attempt-1")

	// Present + valid → nil.
	payload := map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: valid,
		contract.PayloadKeyCompiledRenderPlanSHA:  strings.Repeat("a", 64),
	}
	if err := ValidateCompiledRenderPlan(payload); err != nil {
		t.Fatalf("valid compiled plan rejected: %v", err)
	}

	// Absent → nil (legacy fleet unaffected).
	if err := ValidateCompiledRenderPlan(map[string]interface{}{"job_id": "job-1"}); err != nil {
		t.Fatalf("payload without compiled plan rejected: %v", err)
	}
	if err := ValidateCompiledRenderPlan(nil); err != nil {
		t.Fatalf("nil payload rejected: %v", err)
	}

	// Invalid document → error.
	if err := ValidateCompiledRenderPlan(map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: `{"plan_version": 99}`,
	}); err == nil {
		t.Fatal("invalid compiled document must fail admission")
	}

	// Malformed sha → error.
	if err := ValidateCompiledRenderPlan(map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: valid,
		contract.PayloadKeyCompiledRenderPlanSHA:  "not-hex",
	}); err == nil {
		t.Fatal("malformed compiled_render_plan_sha256 must fail admission")
	}

	// Empty sha is tolerated (older master may omit it).
	if err := ValidateCompiledRenderPlan(map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: valid,
		contract.PayloadKeyCompiledRenderPlanSHA:  "",
	}); err != nil {
		t.Fatalf("empty sha rejected: %v", err)
	}
}

func TestValidateTaskPayload_AcceptsCompiledPlanAlongsideLegacyContract(t *testing.T) {
	valid := validCompiledPlanJSON(t, "job-1", "attempt-1")
	payload := map[string]interface{}{
		"payload_contract_version":               2,
		"job_id":                                  "job-1",
		"job_type":                                "process_video",
		"created_at":                              "2026-08-11T00:00:00Z",
		contract.PayloadKeyCompiledRenderPlanJSON: valid,
		contract.PayloadKeyCompiledRenderPlanSHA:  strings.Repeat("b", 64),
	}
	if err := ValidateTaskPayload(payload); err != nil {
		t.Fatalf("compiled plan on legacy envelope must pass admission: %v", err)
	}

	bad := map[string]interface{}{
		"payload_contract_version":               2,
		"job_id":                                  "job-1",
		"job_type":                                "process_video",
		"created_at":                              "2026-08-11T00:00:00Z",
		contract.PayloadKeyCompiledRenderPlanJSON: `{"plan_version": 7}`,
	}
	if err := ValidateTaskPayload(bad); err == nil {
		t.Fatal("malformed compiled plan must fail admission even on a legacy envelope")
	}
}
