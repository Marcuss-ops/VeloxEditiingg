package native

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCaptureFailedRenderEvidenceSanitizesAndLinksPlan(t *testing.T) {
	root := t.TempDir()
	planPath := filepath.Join(root, "render-plan.json")
	audioPath := filepath.Join(root, "audio.f4a")
	if err := os.WriteFile(audioPath, []byte("audio"), 0o640); err != nil {
		t.Fatal(err)
	}
	plan := []byte(`{"plan_version":1,"job_id":"job-v2","audio":[{"source_url":"https://example.test/audio?token=secret"}]}`)
	if err := os.WriteFile(planPath, plan, 0o640); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VELOX_RENDER_DIAGNOSTICS_DIR", filepath.Join(root, "evidence"))

	captureFailedRenderEvidence("", planPath, filepath.Join(root, "missing.mp4"), "binding_missing", "audio failed", "", nil)

	entries, err := os.ReadDir(filepath.Join(root, "evidence"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("evidence entries = %v, err = %v", entries, err)
	}
	dir := filepath.Join(root, "evidence", entries[0].Name())
	data, err := os.ReadFile(filepath.Join(dir, "audio-diagnostic.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence failedRenderEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.JobID != "job-v2" || evidence.PlanSHA256 == "" {
		t.Fatalf("identity evidence = %+v", evidence)
	}
	if len(evidence.AudioBindings) != 1 {
		t.Fatalf("audio bindings = %+v", evidence.AudioBindings)
	}
	if strings.Contains(string(data), "secret") {
		t.Fatalf("diagnostic contains URL credential: %s", data)
	}
	if _, err := os.Stat(filepath.Join(dir, "render-plan.failed.json")); err != nil {
		t.Fatal(err)
	}
}

func TestCollectAudioBindingsFindsLegacyAndCompiledShapes(t *testing.T) {
	bindings := collectAudioBindings([]byte(`{"audio_tracks":[{"source_url":"/tmp/a"}],"audio":[{"audio_url":"/tmp/b"}]}`))
	if len(bindings) != 2 {
		t.Fatalf("bindings = %+v", bindings)
	}
}
