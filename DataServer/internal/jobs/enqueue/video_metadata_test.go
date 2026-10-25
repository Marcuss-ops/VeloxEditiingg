package enqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
	"velox-shared/contract"
)

func TestNormalizeSceneVideoPayloadPassesThroughCompiledV2WithoutRecompiling(t *testing.T) {
	manifest := strictNormalizerManifest()
	manifest["assets"] = append(manifest["assets"].([]interface{}), map[string]interface{}{
		"id": "final-audio", "uri": "velox-asset://final-audio", "kind": "final_audio",
		"format": "audio/mp4", "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"size_bytes": 200, "duration_ms": 1000,
	})
	canonical, expectedSHA, err := contract.CompileRenderPlanV2JSON(manifest)
	if err != nil {
		t.Fatalf("CompileRenderPlanV2JSON: %v", err)
	}
	// No render_manifest, scenes, or audio instructions are supplied: this
	// is the producer-owned receiver envelope, not an input for the V1
	// compiler. The plan bytes must survive normalization byte-for-byte.
	input := map[string]interface{}{
		"video_name":               "V2 pass-through",
		"script_text":              "Already compiled by PipelineGen",
		"_placement_pin_worker_id": " worker-fmp4 ",
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
		contract.PayloadKeyCompiledRenderPlanSHA:  expectedSHA,
	}
	out, err := normalizeSceneVideoPayloadContext(context.Background(), input)
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayloadContext: %v", err)
	}
	if got := out[contract.PayloadKeyCompiledRenderPlanJSON]; got != string(canonical) {
		t.Fatalf("compiled plan bytes changed: got %v, want exact canonical bytes", got)
	}
	if got := out[contract.PayloadKeyCompiledRenderPlanSHA]; got != expectedSHA {
		t.Fatalf("compiled plan SHA changed: got %v, want %s", got, expectedSHA)
	}
	if got := resolveInternalExecutorID(out); got != "video.assemble.copy.v1@1" {
		t.Fatalf("executor = %q, want video.assemble.copy.v1@1", got)
	}
	if _, present := out["render_plan_json"]; present {
		t.Fatal("V2 pass-through unexpectedly produced a legacy V1 render_plan_json")
	}
	if got := out["_placement_pin_worker_id"]; got != "worker-fmp4" {
		t.Fatalf("compiled V2 placement pin = %#v, want worker-fmp4", got)
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(out); err != nil {
		t.Fatalf("normalized V2 envelope rejected: %v", err)
	}
}

func TestNormalizeSceneVideoPayloadRejectsCompiledV2SHAOrSchemaDrift(t *testing.T) {
	manifest := strictNormalizerManifest()
	manifest["assets"] = append(manifest["assets"].([]interface{}), map[string]interface{}{
		"id": "final-audio", "uri": "velox-asset://final-audio", "kind": "final_audio",
		"format": "audio/mp4", "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"size_bytes": 200, "duration_ms": 1000,
	})
	canonical, expectedSHA, err := contract.CompileRenderPlanV2JSON(manifest)
	if err != nil {
		t.Fatalf("CompileRenderPlanV2JSON: %v", err)
	}
	base := map[string]interface{}{
		"video_name":  "V2 invalid",
		"script_text": "Invalid producer envelope",
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
		contract.PayloadKeyCompiledRenderPlanSHA:  expectedSHA,
	}
	badSHA := cloneRichPayload(base)
	badSHA[contract.PayloadKeyCompiledRenderPlanSHA] = strings.Repeat("f", 64)
	if _, err := normalizeSceneVideoPayloadContext(context.Background(), badSHA); err == nil || !strings.Contains(err.Error(), "producer plan validation failed") {
		t.Fatalf("SHA mismatch error = %v, want producer plan validation failure", err)
	}

	// Keep the transport hash correct for the mutated bytes; strict V2
	// decoding must still reject unknown schema fields rather than silently
	// accepting a producer typo.
	var decoded map[string]interface{}
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatalf("decode canonical plan: %v", err)
	}
	decoded["producer_typo"] = true
	mutated, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("marshal mutated plan: %v", err)
	}
	sum := sha256.Sum256(mutated)
	badSchema := cloneRichPayload(base)
	badSchema[contract.PayloadKeyCompiledRenderPlanJSON] = string(mutated)
	badSchema[contract.PayloadKeyCompiledRenderPlanSHA] = hex.EncodeToString(sum[:])
	if _, err := normalizeSceneVideoPayloadContext(context.Background(), badSchema); err == nil || !strings.Contains(err.Error(), "producer plan validation failed") {
		t.Fatalf("schema drift error = %v, want producer plan validation failure", err)
	}
}

func TestProjectEnqueueJobPreservesCompiledV2InTaskSpec(t *testing.T) {
	manifest := strictNormalizerManifest()
	manifest["assets"] = append(manifest["assets"].([]interface{}), map[string]interface{}{
		"id": "final-audio", "uri": "velox-asset://final-audio", "kind": "final_audio",
		"format": "audio/mp4", "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"size_bytes": 200, "duration_ms": 1000,
	})
	canonical, expectedSHA, err := contract.CompileRenderPlanV2JSON(manifest)
	if err != nil {
		t.Fatalf("CompileRenderPlanV2JSON: %v", err)
	}
	normalized, err := normalizeSceneVideoPayloadContext(context.Background(), map[string]interface{}{
		"job_id": "v2-job", "job_run_id": "v2-run",
		"video_name": "V2 persisted", "script_text": "TaskSpec persistence",
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
		contract.PayloadKeyCompiledRenderPlanSHA:  expectedSHA,
	})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	_, spec, _, err := projectEnqueueJobContext(context.Background(), normalized, costmodel.JobRequirements{})
	if err != nil {
		t.Fatalf("projectEnqueueJobContext: %v", err)
	}
	if spec.ExecutorID != "video.assemble.copy.v1@1" {
		t.Fatalf("TaskSpec executor = %q, want video.assemble.copy.v1@1", spec.ExecutorID)
	}
	if got := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON]; got != string(canonical) {
		t.Fatalf("TaskSpec plan bytes changed: got %v", got)
	}
	if got := spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA]; got != expectedSHA {
		t.Fatalf("TaskSpec plan SHA changed: got %v, want %s", got, expectedSHA)
	}
}

func strictNormalizerManifest() map[string]interface{} {
	const sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	return map[string]interface{}{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]interface{}{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []interface{}{
			map[string]interface{}{"id": "clip", "uri": "velox-asset://clip", "kind": "video", "sha256": sha, "size_bytes": 100, "duration_ms": 1000},
			map[string]interface{}{"id": "voiceover", "uri": "velox-asset://voiceover", "kind": "audio", "sha256": sha, "size_bytes": 100, "duration_ms": 1000},
		},
		"tracks": []interface{}{
			map[string]interface{}{"id": "video", "kind": "video", "events": []interface{}{map[string]interface{}{"asset_id": "clip", "timeline_start_ms": 0, "duration_ms": 1000}}},
			map[string]interface{}{"id": "voiceover", "kind": "voiceover", "events": []interface{}{map[string]interface{}{"asset_id": "voiceover", "timeline_start_ms": 0, "duration_ms": 1000}}},
		},
		"output": map[string]interface{}{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
	}
}
