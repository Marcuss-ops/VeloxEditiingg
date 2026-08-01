package renderplan

import (
	"strings"
	"testing"
)

func validV2Payload() map[string]interface{} {
	return map[string]interface{}{
		"render_plan_version": "v2",
		"executor_id":         "scene.composite.v1",
		"executor_version":    2,
		"assets": []interface{}{
			map[string]interface{}{"id": "clip-1", "uri": "velox-asset://clip-1"},
		},
		"timeline": []interface{}{
			map[string]interface{}{"asset_id": "clip-1", "start_ms": 0, "duration_ms": 1000},
		},
		"output_contract": map[string]interface{}{
			"container":   "mp4",
			"video_codec": "h264",
			"audio_codec": "aac",
		},
	}
}

func TestValidateTaskPayload_V2RequiresCompiledShape(t *testing.T) {
	if err := ValidateTaskPayload(validV2Payload()); err != nil {
		t.Fatalf("valid v2 payload rejected: %v", err)
	}

	for _, field := range []string{"executor_id", "executor_version", "assets", "timeline", "output_contract"} {
		t.Run("missing_"+field, func(t *testing.T) {
			payload := validV2Payload()
			delete(payload, field)
			if err := ValidateTaskPayload(payload); err == nil {
				t.Fatalf("payload missing %q was accepted", field)
			}
		})
	}
}

func TestValidateTaskPayload_RejectsUnknownAndUnversionedPayloads(t *testing.T) {
	cases := []map[string]interface{}{
		{"version": "v3"},
		{"executor_id": "scene.composite.v1", "assets": []interface{}{}},
	}
	for i, payload := range cases {
		if err := ValidateTaskPayload(payload); err == nil {
			t.Fatalf("case %d was accepted without a supported version", i)
		}
	}

	err := ValidateTaskPayload(map[string]interface{}{"version": "v3"})
	if !strings.Contains(err.Error(), string(ERR_PLAN_UNSUPPORTED_VERSION)) {
		t.Fatalf("unknown payload version error = %v, want unsupported version error", err)
	}
}

func TestValidateTaskPayload_AllowsOnlyExplicitLegacyAdapter(t *testing.T) {
	legacy := map[string]interface{}{
		"version":    "v1",
		"job_id":     "job-legacy",
		"job_type":   "render",
		"created_at": "2026-07-31T00:00:00Z",
		"parameters": map[string]interface{}{
			"start_clip_paths": []interface{}{"clip.mp4"},
			"voiceover_paths":  []interface{}{"voice.mp3"},
		},
	}
	if err := ValidateTaskPayload(legacy); err != nil {
		t.Fatalf("explicit v1 legacy payload rejected: %v", err)
	}

	currentMasterPayload := map[string]interface{}{
		"payload_contract_version": 2,
		"job_id":                   "job-current",
		"job_type":                 "process_video",
		"created_at":               "2026-07-31T00:00:00Z",
	}
	if err := ValidateTaskPayload(currentMasterPayload); err != nil {
		t.Fatalf("explicit payload_contract_version adapter rejected: %v", err)
	}

	delete(currentMasterPayload, "payload_contract_version")
	if err := ValidateTaskPayload(currentMasterPayload); err == nil {
		t.Fatal("unversioned current payload was accepted")
	}
}
