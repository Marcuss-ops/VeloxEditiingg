package rendercompiler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestCompilePayloadCompilesRawV2Payload(t *testing.T) {
	raw := map[string]any{
		"job_type": "process_video", "version": "v2", "job_id": "job-raw",
		"voiceover_paths": []any{"velox-asset://voiceover-raw"},
		"scenes": []any{map[string]any{
			"duration_seconds": 2.5,
			"clip": map[string]any{
				"asset_id": "clip-raw", "uri": "velox-asset://clip-raw", "kind": "video",
				"sha256":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				"size_bytes": 100, "duration_ms": 2500,
			},
			"voiceover": map[string]any{
				"asset_id": "voiceover-raw", "uri": "velox-asset://voiceover-raw", "kind": "audio",
				"sha256":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				"size_bytes": 200, "duration_ms": 2500,
			},
		}},
	}
	plan, err := DefaultRegistry().CompilePayload(context.Background(), raw)
	if err != nil {
		t.Fatalf("CompilePayload: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(plan.JSON(), &decoded); err != nil {
		t.Fatal(err)
	}
	events := decoded["tracks"].([]any)[0].(map[string]any)["events"].([]any)
	if got := events[0].(map[string]any)["duration_ms"]; got != float64(2500) {
		t.Fatalf("duration_ms = %v, want 2500 (2.5 seconds)", got)
	}
}

func TestCompilePayloadDispatchesExplicitJobTypeAndVersion(t *testing.T) {
	raw := map[string]any{
		"job_type": "unsupported_job", "version": "v9",
	}
	if _, err := DefaultRegistry().CompilePayload(context.Background(), raw); err == nil || !strings.Contains(err.Error(), "unsupported_job/v9") {
		t.Fatalf("explicit job routing was not honored: %v", err)
	}
}

func TestCompilePayloadRejectsInvalidRenderManifestShapes(t *testing.T) {
	for name, manifest := range map[string]any{
		"null":   nil,
		"string": "not-an-object",
		"empty":  map[string]any{},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DefaultRegistry().CompilePayload(context.Background(), map[string]any{
				"render_manifest": manifest,
			})
			if err == nil || !strings.Contains(err.Error(), "render_manifest") {
				t.Fatalf("invalid render_manifest shape was accepted: %v", err)
			}
		})
	}
}

func TestDefaultRegistryCompilesProcessVideoDeterministically(t *testing.T) {
	payload := testPayload()
	registry := DefaultRegistry()

	first, err := registry.Compile(context.Background(), payload)
	if err != nil {
		t.Fatalf("first compile: %v", err)
	}
	second, err := registry.Compile(context.Background(), payload)
	if err != nil {
		t.Fatalf("second compile: %v", err)
	}
	if string(first.JSON()) != string(second.JSON()) {
		t.Fatalf("same input produced different JSON:\n%s\n---\n%s", first.JSON(), second.JSON())
	}
	if first.SHA256() != second.SHA256() {
		t.Fatalf("same input produced different hashes: %s != %s", first.SHA256(), second.SHA256())
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("compiled plan failed validation: %v", err)
	}
}

func TestCompileSupportsLegacyClipLinkPayload(t *testing.T) {
	payload := testPayload()
	payload.Scenes[0] = map[string]any{
		"duration_seconds": 1.25,
		"clip_link":        "velox-asset://legacy-clip",
		"clip_sha256":      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"clip_size_bytes":  int64(100),
		"clip_duration_ms": int64(1250),
		"voiceover": map[string]any{
			"asset_id": "legacy-voice", "uri": "velox-asset://legacy-voice", "kind": "audio",
			"sha256":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"size_bytes": int64(200), "duration_ms": int64(1250),
		},
	}
	plan, err := DefaultRegistry().Compile(context.Background(), payload)
	if err != nil {
		t.Fatalf("legacy clip_link compile: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(plan.JSON(), &decoded); err != nil {
		t.Fatal(err)
	}
	event := decoded["tracks"].([]any)[0].(map[string]any)["events"].([]any)[0].(map[string]any)
	if event["duration_ms"] != float64(1250) {
		t.Fatalf("duration_ms = %v, want 1250", event["duration_ms"])
	}
}

func TestCompileHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DefaultRegistry().Compile(ctx, testPayload()); err == nil {
		t.Fatal("canceled context was accepted")
	}
}

func TestCompileDoesNotMutatePayloadOrShareNestedData(t *testing.T) {
	payload := testPayload()
	before, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := DefaultRegistry().Compile(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	payload.Scenes[0]["text"] = "mutated after compile"
	payload.Layers[0]["text"] = "mutated layer"
	if string(before) == mustMarshal(payload) {
		t.Fatal("test mutation did not change the source payload")
	}
	if strings.Contains(string(plan.JSON()), "mutated after compile") || strings.Contains(string(plan.JSON()), "mutated layer") {
		t.Fatal("compiled plan retained references to mutable payload data")
	}

	jsonCopy := plan.JSON()
	jsonCopy[0] ^= 0xff
	if string(jsonCopy) == string(plan.JSON()) {
		t.Fatal("JSON did not return a defensive copy")
	}
	snapshot, err := plan.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Layers[0].Text = "mutated snapshot"
	if strings.Contains(string(plan.JSON()), "mutated snapshot") {
		t.Fatal("snapshot mutation changed the stored plan")
	}
}

func TestCompilerSortsLayersAndAssetsDeterministically(t *testing.T) {
	payload := testPayload()
	payload.Layers = []map[string]any{
		{"id": "z", "type": "text", "start_seconds": 1.0, "text": "z"},
		{"id": "a", "type": "text", "start_seconds": 1.0, "text": "a"},
		{"id": "b", "type": "text", "start_seconds": 0.5, "text": "b"},
	}
	plan, err := DefaultRegistry().Compile(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(plan.JSON(), &decoded); err != nil {
		t.Fatal(err)
	}
	layers := decoded["layers"].([]any)
	if got := layers[0].(map[string]any)["id"]; got != "b" {
		t.Fatalf("first layer = %v, want b", got)
	}
	if got := layers[1].(map[string]any)["id"]; got != "a" {
		t.Fatalf("second layer = %v, want a", got)
	}
	if got := layers[2].(map[string]any)["id"]; got != "z" {
		t.Fatalf("third layer = %v, want z", got)
	}
}

func TestRegistryIsPersistentAndRejectsInvalidRegistration(t *testing.T) {
	base := NewRegistry()
	registered, err := base.Register(ProcessVideoJobType, PayloadVersionV2, compileProcessVideoV2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.Compile(context.Background(), testPayload()); err == nil {
		t.Fatal("base registry was mutated by Register")
	}
	if _, err := registered.Compile(context.Background(), testPayload()); err != nil {
		t.Fatalf("registered compiler unavailable: %v", err)
	}
	for _, tc := range []struct {
		jobType, version string
		compiler         Compiler
	}{
		{"", "v2", compileProcessVideoV2},
		{"process_video", "", compileProcessVideoV2},
		{"process_video", "v2", nil},
	} {
		if _, err := base.Register(tc.jobType, tc.version, tc.compiler); err == nil {
			t.Fatalf("invalid registration %q/%q was accepted", tc.jobType, tc.version)
		}
	}
}

func TestCompilerRejectsMissingAssetsAndUnsupportedJob(t *testing.T) {
	payload := testPayload()
	payload.Scenes[0]["clip"].(map[string]any)["sha256"] = ""
	if _, err := DefaultRegistry().Compile(context.Background(), payload); err == nil {
		t.Fatal("missing asset integrity metadata was accepted")
	}
	payload = testPayload()
	payload.JobType = "other"
	if _, err := DefaultRegistry().Compile(context.Background(), payload); err == nil {
		t.Fatal("unsupported job type was accepted")
	}
}

func TestCanonicalKeysIncludeLayers(t *testing.T) {
	if !contract.IsCanonicalKey("layers") {
		t.Fatal("layers is not a canonical top-level key")
	}
	if err := contract.ValidatePayload(map[string]any{"layers": []any{}}); err != nil {
		t.Fatalf("layers array rejected by canonical validation: %v", err)
	}
}

func testPayload() *contract.JobPayloadV2 {
	return &contract.JobPayloadV2{
		ContractVersion: contract.ContractVersionV2,
		JobID:           "job-001",
		JobRunID:        "run-001",
		CorrelationID:   "corr-001",
		JobType:         ProcessVideoJobType,
		Version:         PayloadVersionV2,
		VideoName:       "Deterministic render",
		ScriptText:      "A deterministic script",
		Scenes: []map[string]any{
			{
				"scene_id": "scene-0", "duration_ms": int64(5000),
				"clip": map[string]any{
					"asset_id": "clip-z", "uri": "velox-asset://clip-z", "kind": "video",
					"sha256":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					"size_bytes": int64(1000), "duration_ms": int64(5000),
				},
				"voiceover": map[string]any{
					"asset_id": "voice-a", "uri": "velox-asset://voice-a", "kind": "audio",
					"sha256":     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					"size_bytes": int64(2000), "duration_ms": int64(5000),
				},
			},
		},
		Layers: []map[string]any{
			{"id": "layer-1", "type": "text", "role": "title", "text": "Title", "start_seconds": 0.0, "duration_seconds": 2.0},
		},
	}
}

func mustMarshal(payload *contract.JobPayloadV2) string {
	data, _ := json.Marshal(payload)
	return string(data)
}
