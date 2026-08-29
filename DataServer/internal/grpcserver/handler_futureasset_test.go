package grpcserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"velox-shared/contract"
	"velox-shared/contract/assembly"
	"velox-shared/futureasset"
)

func TestMattDamonCanonicalFixtureProvidesPrefetchIntegrityManifests(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	fixturePath := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "ops", "jobs", "matt_damon_20_clips_canonical.generate.json")
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read Matt Damon fixture: %v", err)
	}
	assets := futureAssetManifests(payload)
	if len(assets) != 26 {
		t.Fatalf("Matt Damon manifests=%d, want 26", len(assets))
	}
	var total int64
	for _, asset := range assets {
		if asset.AssetKey == "" || asset.SHA256 == "" || asset.SizeBytes <= 0 {
			t.Fatalf("incomplete Matt Damon manifest: %+v", asset)
		}
		total += asset.SizeBytes
	}
	if total != 677541256 {
		t.Fatalf("Matt Damon manifest bytes=%d, want 677541256", total)
	}
}

func TestFutureAssetManifestsIsDeterministicallyOrdered(t *testing.T) {
	payload := []byte(`{
		"audio": {"asset_key": "z-audio", "sha256": "sha-z", "size_bytes": 20},
		"video": [
			{"asset_key": "a-video", "sha256": "sha-a", "size_bytes": 10},
			{"asset_key": "z-audio", "sha256": "sha-z", "size_bytes": 20}
		]
	}`)

	want := futureAssetManifests(payload)
	if len(want) != 2 {
		t.Fatalf("manifest length=%d, want 2", len(want))
	}
	if want[0].AssetKey != "a-video" || want[1].AssetKey != "z-audio" {
		t.Fatalf("manifest order=%q,%q, want a-video,z-audio", want[0].AssetKey, want[1].AssetKey)
	}
	for i := 0; i < 20; i++ {
		if got := futureAssetManifests(payload); !reflect.DeepEqual(got, want) {
			t.Fatalf("manifest changed between runs: got=%v want=%v", got, want)
		}
	}
}

func TestSelectWarmPlacementUsesCacheAwareWorkerRanking(t *testing.T) {
	workers := []assembly.WorkerPlacementSnapshot{
		{WorkerID: "cold", Available: true, CapacityAuthoritative: true, DiskAuthoritative: true, MaxExecutionSlots: 2, FreeDiskBytes: 1 << 30},
		{WorkerID: "warm", Available: true, CapacityAuthoritative: true, DiskAuthoritative: true, MaxExecutionSlots: 2, FreeDiskBytes: 1 << 30, CachedSHA256: []string{"sha-video"}},
	}
	assets := []futureasset.AssetManifest{{AssetKey: "video", SHA256: "sha-video", SizeBytes: 10}}
	decision, err := selectWarmPlacement(workers, assets)
	if err != nil {
		t.Fatalf("selectWarmPlacement() error = %v", err)
	}
	if decision.WorkerID != "warm" || decision.CachedAssets != 1 || decision.MissingAssets != 0 {
		t.Fatalf("decision = %#v, want warm worker with complete cache locality", decision)
	}
}

func TestSelectWarmPlacementRejectsUnknownDiskForPrefetch(t *testing.T) {
	workers := []assembly.WorkerPlacementSnapshot{{
		WorkerID: "unknown-disk", Available: true, CapacityAuthoritative: true,
		MaxExecutionSlots: 1, FreeDiskBytes: 1 << 30,
	}}
	assets := []futureasset.AssetManifest{{AssetKey: "video", SHA256: "sha-video", SizeBytes: 10}}
	if _, err := selectWarmPlacement(workers, assets); err == nil {
		t.Fatal("warm prefetch must fail closed when disk authority is unavailable")
	}
}

func TestFutureAssetManifestsIncludesCompiledPlanAssets(t *testing.T) {
	const timelineSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 1, TimelineSHA256: timelineSHA, DurationUS: 1_000_000,
		Output:      contract.OutputContractV2{Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080, FPSNum: 30, FPSDen: 1},
		FinalAudio:  contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 20, Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1_000_000, TimelineRevision: 1, TimelineSHA256: timelineSHA},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "fragment", AssetID: "fragment", SHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", TimelineStartFrame: 0, FrameCount: 30, SourceInUS: 0, SourceDurationUS: 1_000_000}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "fragment", AssetKey: "fragment-key", SHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", SizeBytes: 10, Kind: "prepared_video_fragment", MIME: "video/mp4", DurationUS: 1_000_000},
			{AssetID: "audio", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 20, Kind: "final_audio", MIME: "audio/mp4", DurationUS: 1_000_000},
		},
	}
	rawPlan, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]interface{}{contract.PayloadKeyCompiledRenderPlanJSON: string(rawPlan)})
	if err != nil {
		t.Fatal(err)
	}
	assets := futureAssetManifests(payload)
	if len(assets) != 2 || assets[0].AssetKey != "audio" || assets[1].AssetKey != "fragment-key" {
		t.Fatalf("compiled plan assets = %+v; want canonical audio + fragment-key", assets)
	}
	if assets[1].Role != "prepared_video_fragment" || assets[1].SHA256 == "" || assets[1].SizeBytes != 10 {
		t.Fatalf("prepared fragment manifest = %+v", assets[1])
	}
}
