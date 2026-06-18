package enqueue

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestEnqueueSceneVideoJobRewritesVoiceoverFieldsToVeloxAsset(t *testing.T) {
	tempDir := t.TempDir()
	q := newTestQueue(t, tempDir)

	voiceoverPath := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(voiceoverPath, []byte("ID3fake-voiceover"), 0o600); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	svc := voiceoverassets.NewService(tempDir, []string{tempDir}, 1024*1024, nil)
	SetVoiceoverAssetService(svc)
	t.Cleanup(func() { SetVoiceoverAssetService(nil) })

	jobPayload := map[string]interface{}{
		"job_id":          "voiceover-rewrite-1",
		"video_name":      "Voiceover Rewrite",
		"script_text":     "Voiceover Rewrite script",
		"voiceover_path":  voiceoverPath,
		"audio_path":      voiceoverPath,
		"voiceover_paths": []string{voiceoverPath},
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/i1.png"},
		},
		"parameters": map[string]interface{}{
			"voiceover_path":  voiceoverPath,
			"audio_path":      voiceoverPath,
			"voiceover_paths": []string{voiceoverPath},
		},
	}

	res, err := EnqueueSceneVideoJob(context.Background(), q, jobPayload)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	jobID, _ := res["job_id"].(string)
	job, err := q.GetJobAsMap(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	assertVeloxReference(t, job["voiceover_path"])
	assertVeloxReference(t, job["audio_path"])
	assertVeloxReference(t, firstVoiceoverValue(t, job["voiceover_paths"]))

	params, _ := job["parameters"].(map[string]interface{})
	if params == nil {
		t.Fatalf("missing parameters map: %#v", job)
	}
	assertVeloxReference(t, params["voiceover_path"])
	assertVeloxReference(t, params["audio_path"])
	assertVeloxReference(t, firstVoiceoverValue(t, params["voiceover_paths"]))
}

func TestEnqueueSceneVideoJobDoesNotSubmitJobWhenVoiceoverResolutionFails(t *testing.T) {
	tempDir := t.TempDir()
	q := newTestQueue(t, tempDir)

	svc := voiceoverassets.NewService(tempDir, []string{tempDir}, 1024*1024, nil)
	SetVoiceoverAssetService(svc)
	t.Cleanup(func() { SetVoiceoverAssetService(nil) })

	jobPayload := map[string]interface{}{
		"job_id":         "voiceover-fail-1",
		"video_name":     "Voiceover Fail",
		"script_text":    "Voiceover Fail script",
		"voiceover_path": "https://drive.google.com/file/d/missing/view?usp=sharing",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/i1.png"},
		},
	}

	_, err := EnqueueSceneVideoJob(context.Background(), q, jobPayload)
	if err == nil {
		t.Fatal("want error")
	}
	if assetErr, ok := voiceoverassets.AsAcquisitionError(err); !ok || assetErr.SourceType != "drive" {
		t.Fatalf("want structured drive acquisition error, got %#v", err)
	}
	if _, err := q.GetJobAsMap(context.Background(), "voiceover-fail-1"); err == nil {
		t.Fatal("did not expect job to be submitted")
	}
}

func TestEnqueueSceneVideoJobLeavesVeloxReferencesUnchanged(t *testing.T) {
	tempDir := t.TempDir()
	q := newTestQueue(t, tempDir)

	voiceoverPath := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(voiceoverPath, []byte("ID3idempotent-voiceover"), 0o600); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	svc := voiceoverassets.NewService(tempDir, []string{tempDir}, 1024*1024, nil)
	resolved, err := svc.Resolve(context.Background(), voiceoverPath)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}

	SetVoiceoverAssetService(svc)
	t.Cleanup(func() { SetVoiceoverAssetService(nil) })

	jobPayload := map[string]interface{}{
		"job_id":          "voiceover-idempotent-1",
		"video_name":      "Voiceover Idempotent",
		"script_text":     "Voiceover Idempotent script",
		"voiceover_path":  resolved.Reference,
		"audio_path":      resolved.Reference,
		"voiceover_paths": []string{resolved.Reference},
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/i1.png"},
		},
		"parameters": map[string]interface{}{
			"voiceover_path":  resolved.Reference,
			"audio_path":      resolved.Reference,
			"voiceover_paths": []string{resolved.Reference},
		},
	}

	res, err := EnqueueSceneVideoJob(context.Background(), q, jobPayload)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	jobID, _ := res["job_id"].(string)
	job, err := q.GetJobAsMap(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got, _ := job["voiceover_path"].(string); got != resolved.Reference {
		t.Fatalf("want velox reference unchanged, got %q", got)
	}
	if got, _ := job["audio_path"].(string); got != resolved.Reference {
		t.Fatalf("want velox reference unchanged, got %q", got)
	}
}

func newTestQueue(t *testing.T, tempDir string) *queue.FileQueue {
	t.Helper()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	ts, tsErr := queue.NewTransitionService(jobRepo, db)
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db}, ts)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	return q
}

func firstVoiceoverValue(t *testing.T, value interface{}) interface{} {
	t.Helper()
	switch v := value.(type) {
	case []string:
		if len(v) == 0 {
			t.Fatal("empty voiceover list")
		}
		return v[0]
	case []interface{}:
		if len(v) == 0 {
			t.Fatal("empty voiceover list")
		}
		return v[0]
	default:
		t.Fatalf("unexpected voiceover list type %T", value)
		return nil
	}
}

func assertVeloxReference(t *testing.T, value interface{}) {
	t.Helper()
	got, _ := value.(string)
	if !strings.HasPrefix(got, voiceoverassets.VeloxAssetScheme+"://") {
		t.Fatalf("want velox asset reference, got %#v", value)
	}
}
