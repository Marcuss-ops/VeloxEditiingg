package pipeline

import "testing"

func TestValidateSubmitOverlaysAcceptsEndFrameWithoutFrameCount(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "overlay-end-only",
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 10}},
		Overlays: []SubmitOverlay{{
			ID: "overlay", AssetID: "asset", StartFrame: 24, EndFrame: 144,
			Mode: "replace", AudioMode: "preserve_final_audio",
		}},
	}
	if err, bad := ValidateSubmitJobRequest(req); bad {
		t.Fatalf("end_frame-only overlay rejected: %v", err)
	}
}

func TestValidateSubmitOverlaysRejectsInconsistentEndFrame(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "overlay-end-mismatch",
		Scenes:         []SubmitScene{{Text: "scene", DurationSeconds: 10}},
		Overlays: []SubmitOverlay{{
			ID: "overlay", AssetID: "asset", StartFrame: 24, EndFrame: 150, FrameCount: 120,
			Mode: "replace", AudioMode: "preserve_final_audio",
		}},
	}
	if err, bad := ValidateSubmitJobRequest(req); !bad || err == nil {
		t.Fatal("inconsistent end_frame/frame_count was accepted")
	}
}
