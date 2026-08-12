package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/workercache"
)

func TestExtractAssetKeysFromJSON_IncludesAllCompiledPlanV2Assets(t *testing.T) {
	compiledPlan, err := json.Marshal(map[string]interface{}{
		"plan_version": 2,
		"assets": []interface{}{
			map[string]interface{}{"asset_id": "video-a"},
			map[string]interface{}{"asset_id": "velox-asset://video-b"},
		},
		"final_audio": map[string]interface{}{
			"asset_id": "audio-master-001",
		},
		"video_tracks": []interface{}{
			map[string]interface{}{
				"segments": []interface{}{
					map[string]interface{}{"asset_id": "video-a"},
					map[string]interface{}{"asset_id": "video-c"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal compiled plan: %v", err)
	}

	keys := extractAssetKeysFromJSON(map[string]interface{}{
		"scenes": []interface{}{map[string]interface{}{
			"clip_link": "https://drive.google.com/file/d/legacy-video/view",
		}},
		contract.PayloadKeyCompiledRenderPlanJSON: string(compiledPlan),
	})

	want := []string{"audio-master-001", "legacy-video", "video-a", "video-b", "video-c"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("extracted lease keys = %v, want %v", keys, want)
	}
}

func TestCompiledPlanV2AssetsIncludingFinalAudioAreLeased(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cache, err := workercache.Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	compiledPlan, err := json.Marshal(map[string]interface{}{
		"assets": []interface{}{
			map[string]interface{}{"asset_id": "video-a"},
			map[string]interface{}{"asset_id": "video-b"},
		},
		"final_audio": map[string]interface{}{"asset_id": "audio-master-001"},
		"video_tracks": []interface{}{
			map[string]interface{}{"segments": []interface{}{
				map[string]interface{}{"asset_id": "video-a"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("marshal compiled plan: %v", err)
	}
	payload := map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: string(compiledPlan),
	}
	keys := extractAssetKeysFromJSON(payload)
	if len(keys) != 3 {
		t.Fatalf("extracted %d V2 keys = %v, want three unique assets", len(keys), keys)
	}

	for _, key := range keys {
		path := filepath.Join(dir, key+".asset")
		if err := os.WriteFile(path, []byte(key), 0o644); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		if err := cache.Store(ctx, workercache.Entry{
			AssetKey:         workercache.AssetKey(key),
			LocalPath:        path,
			SizeBytes:        int64(len(key)),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
	}

	lease, err := AcquireJobClips(ctx, cache, "JOB-V2", keys)
	if err != nil {
		t.Fatalf("acquire V2 asset lease: %v", err)
	}
	if lease == nil {
		t.Fatal("AcquireJobClips returned nil lease")
	}
	defer lease.ReleaseAll(ctx)

	for _, key := range keys {
		entry, found, findErr := cache.Find(ctx, key)
		if findErr != nil || !found {
			t.Fatalf("find leased asset %s: found=%v err=%v", key, found, findErr)
		}
		if entry.ActiveJobID != "JOB-V2" {
			t.Errorf("asset %s active job = %q, want JOB-V2", key, entry.ActiveJobID)
		}
		if entry.ActiveLeaseCount != 1 {
			t.Errorf("asset %s active lease count = %d, want 1", key, entry.ActiveLeaseCount)
		}
	}
}

func TestAcquireJobClips_RollbackPreservesPreexistingLease(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cache, err := workercache.Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	for _, key := range []string{"first", "already-leased"} {
		path := filepath.Join(dir, key+".asset")
		if err := os.WriteFile(path, []byte(key), 0o644); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		if err := cache.Store(ctx, workercache.Entry{
			AssetKey:         workercache.AssetKey(key),
			LocalPath:        path,
			SizeBytes:        int64(len(key)),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
	}

	const jobID = "JOB-ROLLBACK"
	if err := cache.Acquire(ctx, "already-leased", jobID); err != nil {
		t.Fatalf("pre-acquire already-leased: %v", err)
	}
	_, err = AcquireJobClips(ctx, cache, jobID, []string{"first", "missing", "already-leased"})
	if err == nil {
		t.Fatal("AcquireJobClips unexpectedly succeeded with missing asset")
	}

	entry, found, err := cache.Find(ctx, "already-leased")
	if err != nil || !found {
		t.Fatalf("find pre-existing lease: found=%v err=%v", found, err)
	}
	if entry.ActiveJobID != jobID {
		t.Fatalf("pre-existing lease was released during rollback: active job=%q, want %q", entry.ActiveJobID, jobID)
	}
	if err := cache.Release(ctx, "already-leased", jobID); err != nil {
		t.Fatalf("release pre-existing lease: %v", err)
	}
}

func TestClipLease_ReleaseAllUsesDetachedContext(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cache, err := workercache.Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	path := filepath.Join(dir, "final-audio.asset")
	if err := os.WriteFile(path, []byte("audio"), 0o644); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{AssetKey: "final-audio", LocalPath: path, SizeBytes: 5, DownloadComplete: true}); err != nil {
		t.Fatalf("store asset: %v", err)
	}
	lease, err := AcquireJobClips(ctx, cache, "JOB-CANCELED", []string{"final-audio"})
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := lease.ReleaseAll(canceled); err != nil {
		t.Fatalf("ReleaseAll(canceled context): %v", err)
	}
	entry, found, err := cache.Find(ctx, "final-audio")
	if err != nil || !found {
		t.Fatalf("find released asset: found=%v err=%v", found, err)
	}
	if entry.ActiveLeaseCount != 0 || entry.ActiveJobID != "" {
		t.Fatalf("canceled-context cleanup retained lease: job=%q count=%d", entry.ActiveJobID, entry.ActiveLeaseCount)
	}
}

func TestExtractAssetKeysFromJSON_PrefersAssetKeyForLeaseIdentity(t *testing.T) {
	keys := extractAssetKeysFromJSON(map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: `{"assets":[{"asset_id":"plan-video","asset_key":"cache-video"}],"final_audio":{"asset_id":"plan-audio","asset_key":"cache-audio"}}`,
	})
	want := []string{"cache-audio", "cache-video"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("extracted keyed lease identities = %v, want %v", keys, want)
	}
}

func TestExtractAssetKeysFromJSON_InvalidCompiledPlanDoesNotBreakLegacyLeaseExtraction(t *testing.T) {
	keys := extractAssetKeysFromJSON(map[string]interface{}{
		"scenes": []interface{}{map[string]interface{}{
			"clip_link": "https://drive.google.com/file/d/legacy-only/view",
		}},
		contract.PayloadKeyCompiledRenderPlanJSON: "{not-valid-json",
	})
	want := []string{"legacy-only"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys after invalid V2 document = %v, want %v", keys, want)
	}
}
