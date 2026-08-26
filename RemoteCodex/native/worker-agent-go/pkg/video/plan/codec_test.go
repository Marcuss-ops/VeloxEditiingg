package plan

import (
	"encoding/json"
	"strings"
	"testing"
)

func validRuntimePlanJSON(t *testing.T, jobID string) []byte {
	t.Helper()
	data, err := json.Marshal(RenderPlan{
		Version: 1,
		JobID:   jobID,
		Canvas:  CanvasSpec{Width: 1920, Height: 1080, Fps: 30},
		Timeline: []TimelineItem{{
			Source:          MediaSource{Type: "video", URL: "velox-asset://clip-1"},
			DurationSeconds: 1,
		}},
	})
	if err != nil {
		t.Fatalf("marshal runtime plan: %v", err)
	}
	return data
}

func TestDecodeJSONValidatesAndOwnsRuntimePlan(t *testing.T) {
	decoded, err := DecodeJSON(validRuntimePlanJSON(t, "job-1"))
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if err := decoded.ValidateForJob("job-1"); err != nil {
		t.Fatalf("ValidateForJob: %v", err)
	}
}

func TestDecodeJSONRejectsUnknownAndTrailingData(t *testing.T) {
	base := string(validRuntimePlanJSON(t, "job-1"))
	cases := []string{
		strings.Replace(base, `"job_id":"job-1"`, `"job_id":"job-1","unknown":true`, 1),
		base + ` {"extra":true}`,
	}
	for _, input := range cases {
		if _, err := DecodeJSON([]byte(input)); err == nil {
			t.Fatalf("DecodeJSON accepted invalid input %q", input)
		}
	}
}

func TestValidateForJobRejectsMismatchedTask(t *testing.T) {
	decoded, err := DecodeJSON(validRuntimePlanJSON(t, "job-1"))
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if err := decoded.ValidateForJob("job-2"); err == nil {
		t.Fatal("ValidateForJob accepted a mismatched task identity")
	}
}
