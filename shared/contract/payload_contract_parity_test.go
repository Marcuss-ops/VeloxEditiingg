package contract

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestJobPayloadV2ContractParity(t *testing.T) {
	jsonTagKeys := jobPayloadV2JSONTagKeys(t)
	canonicalKeys := stringSet(CanonicalTopLevelKeys)
	payloadKeys := mapKeys(t, mustPayloadV2Map(t))

	assertKeySetEqual(t, "JobPayloadV2 JSON tags", jsonTagKeys, "CanonicalTopLevelKeys", canonicalKeys)
	assertKeySetEqual(t, "JobPayloadV2 JSON tags", jsonTagKeys, "ToMap", payloadKeys)
}

func jobPayloadV2JSONTagKeys(t *testing.T) map[string]bool {
	t.Helper()
	typeOfPayload := reflect.TypeOf(JobPayloadV2{})
	keys := make(map[string]bool, typeOfPayload.NumField())
	for index := 0; index < typeOfPayload.NumField(); index++ {
		field := typeOfPayload.Field(index)
		tag := field.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		key := strings.Split(tag, ",")[0]
		if key == "" {
			t.Fatalf("JobPayloadV2.%s has an empty JSON key in tag %q", field.Name, tag)
		}
		if keys[key] {
			t.Fatalf("duplicate JSON key %q in JobPayloadV2", key)
		}
		keys[key] = true
	}
	return keys
}

func mustPayloadV2Map(t *testing.T) map[string]any {
	t.Helper()
	payload := &JobPayloadV2{
		ContractVersion:        2,
		PayloadContractVersion: 2,
		JobID:                  "job-1", JobRunID: "run-1", CorrelationID: "corr-1",
		JobType: "process_video", TemplateID: "test-template", TemplateVersion: 1, Version: "v2",
		CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-01T00:00:00Z",
		VideoName: "video", ScriptText: "script",
		Spec:           map[string]any{"recipe": "canonical"},
		Output:         map[string]any{"width": 1280, "height": 720, "fps": 24},
		RenderManifest: map[string]any{"schema": "manifest"},
		Assets:         []map[string]any{{"id": "asset-1"}},
		ManifestRef:    map[string]any{"uri": "velox-asset://manifest"},
		ManifestSHA256: "manifest-sha",
		RenderPlanJSON: "{}", RenderPlanSHA256: "plan-sha",
		ScenesJSON:     "[]",
		Scenes:         []map[string]any{{"id": "scene-1"}},
		Layers:         []map[string]any{{"id": "layer-1"}},
		Items:          []map[string]any{{"role": "scene"}},
		AudioTracks:    []map[string]any{{"source_url": "audio.mp3"}},
		VoiceoverPaths: []string{"voiceover.mp3"},
		AudioLanguage:  "en", VideoMode: "scene_image", Effect: "slow_zoom", Orientation: "landscape", OutputPath: "/tmp/video.mp4",
		DriveOutput: "drive-folder", ChannelID: "channel-1", OutputVideoID: "output-1",
		SceneImagePaths: []string{"scene.jpg"}, ImageSourceMap: "{}",
		VideoMetadata: map[string]any{"width": 1920},
		Priority:      1, TimeoutSecs: 3600, SceneCount: 1, VoiceoverCount: 1,
		TotalDurationSecs: 5, SceneDurationSecs: 5,
		SubmittedVia: "test", Source: "test", JobFingerprint: "fingerprint", Status: "PENDING",
		DeliveryPlan: []map[string]any{{"destination_id": "destination-1"}},
	}
	output, err := payload.ToMap()
	if err != nil {
		t.Fatalf("JobPayloadV2.ToMap(): %v", err)
	}
	return output
}

func mapKeys(t *testing.T, payload map[string]any) map[string]bool {
	t.Helper()
	keys := make(map[string]bool, len(payload))
	for key := range payload {
		keys[key] = true
	}
	return keys
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func assertKeySetEqual(t *testing.T, leftName string, left map[string]bool, rightName string, right map[string]bool) {
	t.Helper()
	var missing, extra []string
	for key := range left {
		if !right[key] {
			extra = append(extra, key)
		}
	}
	for key := range right {
		if !left[key] {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return
	}
	sort.Strings(missing)
	sort.Strings(extra)
	t.Errorf("%s and %s differ: missing from %s=%v, extra in %s=%v", leftName, rightName, leftName, missing, rightName, extra)
}

func TestNewJobPayloadV2PreservesRenderTimelineArrays(t *testing.T) {
	raw := map[string]any{"assets": []any{map[string]any{"id": "asset-1"}},
		"items":        []any{map[string]any{"role": "scene"}},
		"audio_tracks": []any{map[string]any{"source_url": "audio.mp3"}},
		"scenes":       []any{map[string]any{"text": "scene", "subtitles": map[string]any{"url": "subtitles.srt", "format": "srt"}}},
	}
	payload := NewJobPayloadV2(raw)
	mapped, err := payload.ToMap()
	if err != nil {
		t.Fatalf("JobPayloadV2.ToMap(): %v", err)
	}
	for _, key := range []string{"assets", "items", "audio_tracks"} {
		values, ok := mapped[key].([]map[string]any)
		if !ok || len(values) != 1 {
			t.Fatalf("%s = %#v, want one object", key, mapped[key])
		}
	}
}

func TestJobPayloadV2JSONAndToMapHaveTheSameKeys(t *testing.T) {
	payload := &JobPayloadV2{
		ContractVersion: 2, PayloadContractVersion: 2, JobID: "job-1", JobRunID: "run-1", CorrelationID: "corr-1",
		JobType: "process_video", Version: "v2", CreatedAt: "now", UpdatedAt: "now",
		VideoName: "video", ScriptText: "script", Priority: 1, TimeoutSecs: 1,
		Items:       []map[string]any{{"role": "scene"}},
		AudioTracks: []map[string]any{{"source_url": "audio.mp3"}},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(JobPayloadV2): %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal(JobPayloadV2): %v", err)
	}
	mapped, err := payload.ToMap()
	if err != nil {
		t.Fatalf("JobPayloadV2.ToMap(): %v", err)
	}
	assertKeySetEqual(t, "MarshalJSON", mapKeys(t, raw), "ToMap", mapKeys(t, mapped))
}
