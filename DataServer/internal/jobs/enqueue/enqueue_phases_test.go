package enqueue

import (
	"context"
	"errors"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
)

func TestEnqueuePhaseOfPreservesUnderlyingValidationError(t *testing.T) {
	want := errors.New("invalid forwarding identity")
	wrapped := wrapEnqueuePhase(EnqueuePhaseValidateInput, want)

	phase, ok := EnqueuePhaseOf(wrapped)
	if !ok || phase != EnqueuePhaseValidateInput {
		t.Fatalf("EnqueuePhaseOf = %q, %v; want %q, true", phase, ok, EnqueuePhaseValidateInput)
	}
	if !errors.Is(wrapped, want) {
		t.Fatalf("phase error does not preserve underlying error: %v", wrapped)
	}
}

func TestPrepareJobAndTaskClassifiesInputValidation(t *testing.T) {
	enqueuer := &Enqueuer{}
	_, _, _, err := enqueuer.PrepareJobAndTask(context.Background(), nil, costmodel.JobRequirements{})
	if err == nil {
		t.Fatal("PrepareJobAndTask(nil creator) returned nil error")
	}
	phase, ok := EnqueuePhaseOf(err)
	if !ok || phase != EnqueuePhaseValidateInput {
		t.Fatalf("phase = %q, %v; want %q, true (err=%v)", phase, ok, EnqueuePhaseValidateInput, err)
	}
	if !strings.Contains(err.Error(), "creator unavailable") {
		t.Fatalf("error lost input validation detail: %v", err)
	}
}

func TestResolveEnqueueAssetsClassifiesMissingAssetService(t *testing.T) {
	enqueuer := &Enqueuer{}
	err := wrapEnqueuePhase(EnqueuePhaseResolveAssets, enqueuer.resolveEnqueueAssets(context.Background(), map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{"start_seconds": 1, "end_seconds": 2},
			},
		},
	}))
	if phase, ok := EnqueuePhaseOf(err); !ok || phase != EnqueuePhaseResolveAssets {
		t.Fatalf("phase = %q, %v; want %q, true", phase, ok, EnqueuePhaseResolveAssets)
	}
	if !strings.Contains(err.Error(), "require master asset service") {
		t.Fatalf("asset error lost detail: %v", err)
	}
}

func TestPrepareJobAndTaskClassifiesAssetResolution(t *testing.T) {
	enqueuer := newTestEnqueuer(t)
	payload := map[string]interface{}{
		"video_name":     "Timed clip",
		"script_text":    "asset phase",
		"voiceover_path": "/tmp/v.mp3",
		"scenes": []interface{}{
			map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"},
		},
		"clip_segments": []interface{}{
			map[string]interface{}{"start_seconds": 1, "end_seconds": 2},
		},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 1},
		},
	}

	_, _, _, err := enqueuer.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatal("PrepareJobAndTask returned nil error for timed clips without asset service")
	}
	phase, ok := EnqueuePhaseOf(err)
	if !ok || phase != EnqueuePhaseResolveAssets {
		t.Fatalf("phase = %q, %v; want %q, true (err=%v)", phase, ok, EnqueuePhaseResolveAssets, err)
	}
}

func TestPrepareJobAndTaskClassifiesPayloadNormalization(t *testing.T) {
	enqueuer := newTestEnqueuer(t)
	payload := map[string]interface{}{
		"script_text":     "missing video name",
		"voiceover_paths": []string{"voice.mp3"},
		"scenes":          []interface{}{map[string]interface{}{"text": "scene"}},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 1},
		},
	}

	_, _, _, err := enqueuer.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatal("PrepareJobAndTask returned nil error for invalid scene-video payload")
	}
	phase, ok := EnqueuePhaseOf(err)
	if !ok || phase != EnqueuePhaseNormalizePayload {
		t.Fatalf("phase = %q, %v; want %q, true (err=%v)", phase, ok, EnqueuePhaseNormalizePayload, err)
	}
	if !strings.Contains(err.Error(), "video_name") {
		t.Fatalf("normalization error lost field detail: %v", err)
	}
}

func TestNormalizeEnqueuePayloadClassifiesInvalidSceneVideoPayload(t *testing.T) {
	_, err := normalizeEnqueuePayload(nil, map[string]interface{}{
		"script_text":     "missing video name",
		"voiceover_paths": []string{"voice.mp3"},
		"scenes":          []interface{}{map[string]interface{}{"text": "scene"}},
	}, "", false)
	if err == nil {
		t.Fatal("normalizeEnqueuePayload returned nil error for invalid payload")
	}
	classified := wrapEnqueuePhase(EnqueuePhaseNormalizePayload, err)
	if phase, ok := EnqueuePhaseOf(classified); !ok || phase != EnqueuePhaseNormalizePayload {
		t.Fatalf("phase = %q, %v; want %q, true", phase, ok, EnqueuePhaseNormalizePayload)
	}
	if !strings.Contains(err.Error(), "video_name") {
		t.Fatalf("normalization error lost field detail: %v", err)
	}
}

func TestProjectEnqueueJobClassifiesRendererMarshalError(t *testing.T) {
	normalized := map[string]interface{}{
		"job_id":     "marshal-failure",
		"video_name": "Marshal failure",
		"video_metadata": map[string]interface{}{
			"unsupported": func() {},
		},
	}

	_, _, _, err := projectEnqueueJob(normalized, costmodel.JobRequirements{})
	if err == nil {
		t.Fatal("projectEnqueueJob returned nil error for unsupported JSON value")
	}
	if !strings.Contains(err.Error(), "marshal renderer payload") {
		t.Fatalf("error does not identify renderer projection: %v", err)
	}
}

func TestPersistEnqueueJobTaskPreservesCreatorError(t *testing.T) {
	// A nil creator is the phase helper's deterministic failure and avoids
	// touching a database in this classification test.
	var enqueuer *Enqueuer
	err := enqueuer.persistEnqueueJobTask(nil, &jobs.Job{}, nil, 0)
	if err == nil || !strings.Contains(err.Error(), "creator unavailable") {
		t.Fatalf("persist helper error = %v, want creator unavailable", err)
	}
}
