package renderplan

import (
	"context"
	"strings"
	"testing"
)

type fakeResolver struct {
	byID map[string]AssetMetadata
}

func (f *fakeResolver) ResolveAssetMetadata(_ context.Context, assetID string) (AssetMetadata, error) {
	if f == nil || f.byID == nil {
		return AssetMetadata{AssetID: assetID}, nil
	}
	if meta, ok := f.byID[assetID]; ok {
		return meta, nil
	}
	return AssetMetadata{AssetID: assetID}, nil
}

func mustCompile(t *testing.T, compiler *RenderPlanCompiler, payload map[string]interface{}, attemptID string) *CompiledRenderPlan {
	t.Helper()
	plan, err := compiler.Compile(context.Background(), payload, attemptID)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return plan
}

func TestCompile_ItemsTimelineAccumulatesAndBuildsAudio(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-1",
		"items": []interface{}{
			map[string]interface{}{"type": "video", "url": "velox-asset://asset-a", "duration": 2.5, "role": "voiceover_bed", "asset_id": "asset-a", "sha256": "sha-a"},
			map[string]interface{}{"type": "video", "url": "velox-asset://asset-b", "duration": 3.0, "role": "scene_clip", "asset_id": "asset-b"},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{"source_url": "velox-asset://asset-vo", "role": "voiceover", "start_time_offset": 0, "duration_seconds": 2.5, "volume": 1.0},
			map[string]interface{}{"source_url": "velox-asset://asset-b", "role": "scene_clip_audio", "start_time_offset": 2.5, "duration_seconds": 3.0},
		},
	}, "attempt-1")

	if len(plan.Segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(plan.Segments))
	}
	if plan.Segments[0].TimelineStartMS != 0 || plan.Segments[1].TimelineStartMS != 2500 {
		t.Fatalf("timeline starts = %d,%d want 0,2500", plan.Segments[0].TimelineStartMS, plan.Segments[1].TimelineStartMS)
	}
	if plan.Segments[0].AssetSHA256 != "sha-a" {
		t.Fatalf("segment sha256 = %q, want sha-a", plan.Segments[0].AssetSHA256)
	}
	if len(plan.Audio) != 2 || plan.Audio[1].StartMS != 2500 {
		t.Fatalf("audio = %+v, want 2 tracks with second starting at 2500ms", plan.Audio)
	}
	if plan.DurationMS != 5500 {
		t.Fatalf("duration = %d, want 5500", plan.DurationMS)
	}
	// Asset list deduped: asset-a, asset-b, asset-vo.
	if len(plan.Assets) != 3 {
		t.Fatalf("assets = %d, want 3", len(plan.Assets))
	}
}

func TestCompile_ScenesWithTrimWindowsAndVoiceover(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-2",
		"scenes_json": `[
			{"scene_id":"s1","clip":{"asset_id":"asset-1","url":"velox-asset://asset-1","start_ms":12000,"end_ms":19000,"duration_ms":7000},"voiceover":{"asset_id":"vo-1","url":"velox-asset://vo-1","duration_ms":7000}}
		]`,
	}, "attempt-2")

	if len(plan.Segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(plan.Segments))
	}
	seg := plan.Segments[0]
	if seg.AssetID != "asset-1" || seg.SourceInMS != 12000 || seg.SourceOutMS != 19000 || seg.TimelineStartMS != 0 {
		t.Fatalf("segment = %+v, want trims 12000/19000 at timeline 0", seg)
	}
	if len(plan.Audio) != 1 || plan.Audio[0].AssetID != "vo-1" || plan.Audio[0].DurationMS != 7000 {
		t.Fatalf("audio = %+v, want vo-1 7000ms", plan.Audio)
	}
}

func TestCompile_ClipSegmentsExplicitTrims(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-3",
		"clip_segments": []interface{}{
			map[string]interface{}{"source": "velox-asset://clip-1", "start_ms": 0, "end_ms": 5000},
			map[string]interface{}{"source": "velox-asset://clip-2", "start_ms": 1000, "end_ms": 4000},
		},
	}, "attempt-3")

	if len(plan.Segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(plan.Segments))
	}
	if plan.Segments[1].TimelineStartMS != 5000 || plan.Segments[1].SourceInMS != 1000 || plan.Segments[1].SourceOutMS != 4000 {
		t.Fatalf("second segment = %+v, want timeline 5000 trims 1000/4000", plan.Segments[1])
	}
}

func TestCompile_NeverEmitsLocalPaths(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id":      "job-4",
		"job_type":    "process_video",
		"output_path": "/var/cache/worker123/final.mp4",
		"items": []interface{}{
			map[string]interface{}{"type": "video", "url": "velox-asset://asset-x", "duration": 1.0},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{"source_url": "velox-asset://vo-x", "duration_seconds": 1.0},
		},
	}, "attempt-4")

	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	text := string(canonical)
	// The plan document carries NO path-like values: local paths are the
	// CacheResolver's concern, and asset ids are forbidden from containing
	// slashes by the assetref contract.
	if strings.Contains(text, "/") {
		t.Fatalf("plan leaked a local path: %s", text)
	}
	for _, seg := range plan.Segments {
		if seg.AssetID == "" {
			t.Fatalf("segment without asset_id: %+v", seg)
		}
	}
	for _, track := range plan.Audio {
		if track.AssetID == "" {
			t.Fatalf("audio track without asset_id: %+v", track)
		}
	}
}

func TestCompile_HashIsDeterministicAndSensitive(t *testing.T) {
	payload := map[string]interface{}{
		"job_id": "job-5",
		"items": []interface{}{
			map[string]interface{}{"type": "video", "url": "velox-asset://asset-a", "duration": 2.0},
			map[string]interface{}{"type": "video", "url": "velox-asset://asset-b", "duration": 3.0},
		},
	}
	compiler := NewCompiler(Options{})
	first := mustCompile(t, compiler, payload, "attempt-5")
	second := mustCompile(t, compiler, payload, "attempt-5")

	h1, err := first.PlanSHA256()
	if err != nil {
		t.Fatalf("PlanSHA256: %v", err)
	}
	h2, err := second.PlanSHA256()
	if err != nil {
		t.Fatalf("PlanSHA256: %v", err)
	}
	if h1 == "" || h1 != h2 {
		t.Fatalf("hash not deterministic: %q vs %q", h1, h2)
	}

	changed := map[string]interface{}{
		"job_id": "job-5",
		"items": []interface{}{
			map[string]interface{}{"type": "video", "url": "velox-asset://asset-a", "duration": 2.0},
			map[string]interface{}{"type": "video", "url": "velox-asset://asset-b", "duration": 3.5},
		},
	}
	h3, err := mustCompile(t, compiler, changed, "attempt-5").PlanSHA256()
	if err != nil {
		t.Fatalf("PlanSHA256: %v", err)
	}
	if h3 == h1 {
		t.Fatalf("hash did not change when timeline changed")
	}
}

func TestCompile_MediaContractDefaultsAndOverrides(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-6",
		"items":  []interface{}{map[string]interface{}{"type": "video", "url": "velox-asset://a", "duration": 1.0}},
	}, "attempt-6")
	if plan.MediaContract.Width != 1920 || plan.MediaContract.Height != 1080 ||
		plan.MediaContract.FpsNum != 30 || plan.MediaContract.FpsDen != 1 || plan.MediaContract.VideoCodec != "h264" {
		t.Fatalf("default contract = %+v", plan.MediaContract)
	}

	plan = mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-6b",
		"output": map[string]interface{}{"width": 1280, "height": 720, "fps": 24, "copy_only": true},
		"items":  []interface{}{map[string]interface{}{"type": "video", "url": "velox-asset://a", "duration": 1.0}},
	}, "attempt-6b")
	if plan.MediaContract.Width != 1280 || plan.MediaContract.Height != 720 || plan.MediaContract.FpsNum != 24 || !plan.MediaContract.CopyOnly {
		t.Fatalf("overridden contract = %+v", plan.MediaContract)
	}

	// clip_stock video_mode implies copy-only (mirrors the worker contract).
	plan = mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-6c", "video_mode": "clip_stock",
		"items": []interface{}{map[string]interface{}{"type": "video", "url": "velox-asset://a", "duration": 1.0}},
	}, "attempt-6c")
	if !plan.MediaContract.CopyOnly {
		t.Fatalf("clip_stock must imply copy_only, got %+v", plan.MediaContract)
	}
}

func TestCompile_MetadataEnrichmentIsBestEffort(t *testing.T) {
	resolver := &fakeResolver{byID: map[string]AssetMetadata{
		"asset-a": {AssetID: "asset-a", SHA256: "registry-sha", Kind: "stock_clip", MimeType: "video/mp4", SizeBytes: 42, DurationMs: 2500, Width: 1920, Height: 1080},
	}}
	compiler := NewCompiler(Options{MetadataResolver: resolver})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-7",
		"items":  []interface{}{map[string]interface{}{"type": "video", "url": "velox-asset://asset-a", "duration": 2.5}},
	}, "attempt-7")

	if len(plan.Assets) != 1 {
		t.Fatalf("assets = %d, want 1", len(plan.Assets))
	}
	asset := plan.Assets[0]
	if asset.SHA256 != "registry-sha" || asset.Kind != "stock_clip" || asset.MimeType != "video/mp4" ||
		asset.SizeBytes != 42 || asset.Width != 1920 || asset.Height != 1080 {
		t.Fatalf("enriched asset = %+v", asset)
	}

	// A failing resolver must not fail the compile and must not invent data.
	plan = mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-7b",
		"items":  []interface{}{map[string]interface{}{"type": "video", "url": "velox-asset://unknown", "duration": 1.0}},
	}, "attempt-7b")
	if len(plan.Assets) != 1 || plan.Assets[0].SHA256 != "" {
		t.Fatalf("unknown asset must stay unenriched: %+v", plan.Assets)
	}
}

func TestCompile_DeferredSourcesStayOutOfThePlan(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-8",
		"items": []interface{}{
			map[string]interface{}{"type": "video", "url": "https://example.com/remote.mp4", "duration": 1.0},
			map[string]interface{}{"type": "video", "url": "velox-drive://fileABC", "duration": 1.0},
			map[string]interface{}{"type": "video", "url": "velox-asset://local-1", "duration": 1.0},
		},
	}, "attempt-8")

	// Remote http(s) sources are not canonical wire refs and stay out of the
	// compiled plan (the worker resolves them from the payload). Deferred
	// velox-drive refs carry an id and are part of the timeline, so they
	// appear as segments but are never enriched (no registry row).
	if len(plan.Segments) != 2 {
		t.Fatalf("segments = %+v, want drive + local only", plan.Segments)
	}
	if plan.Segments[0].AssetID != "fileABC" || plan.Segments[1].AssetID != "local-1" {
		t.Fatalf("segments = %+v, want fileABC then local-1", plan.Segments)
	}
	if len(plan.Assets) != 2 || plan.Assets[0].AssetID != "fileABC" || plan.Assets[1].AssetID != "local-1" {
		t.Fatalf("assets = %+v, want fileABC + local-1 sorted", plan.Assets)
	}
	if plan.Assets[0].SHA256 != "" || plan.Assets[1].SHA256 != "" {
		t.Fatalf("deferred/drive assets must stay unenriched: %+v", plan.Assets)
	}
}

func TestCompile_ValidationErrors(t *testing.T) {
	compiler := NewCompiler(Options{})
	if _, err := compiler.Compile(context.Background(), map[string]interface{}{"items": []interface{}{}}, "attempt-9"); err == nil {
		t.Fatal("missing job_id must fail")
	}
	if _, err := compiler.Compile(context.Background(), map[string]interface{}{"job_id": "job-9"}, "attempt-9"); err == nil {
		t.Fatal("empty timeline must fail")
	}
	if _, err := compiler.Compile(context.Background(), nil, "attempt-9"); err == nil {
		t.Fatal("nil payload must fail")
	}
}

func TestHashCanonical_MatchesPlanSHA256(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-hash",
		"items":  []interface{}{map[string]interface{}{"type": "video", "url": "velox-asset://a", "duration": 1.0}},
	}, "attempt-hash")

	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	viaHelper := HashCanonical(canonical)
	viaPlan, err := plan.PlanSHA256()
	if err != nil {
		t.Fatalf("PlanSHA256: %v", err)
	}
	if viaHelper == "" || viaHelper != viaPlan {
		t.Fatalf("HashCanonical = %q, PlanSHA256 = %q; must agree", viaHelper, viaPlan)
	}
}

func TestPlanRoundTrip(t *testing.T) {
	compiler := NewCompiler(Options{})
	plan := mustCompile(t, compiler, map[string]interface{}{
		"job_id": "job-10",
		"items":  []interface{}{map[string]interface{}{"type": "video", "url": "velox-asset://a", "duration": 1.0}},
	}, "attempt-10")

	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	decoded, err := Decode(canonical)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if decoded.JobID != plan.JobID || decoded.AttemptID != plan.AttemptID || decoded.PlanVersion != PlanVersion {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
	if decoded.DurationMS != plan.DurationMS {
		t.Fatalf("duration mismatch: %d vs %d", decoded.DurationMS, plan.DurationMS)
	}
}
