package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/worker"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

type receiverOnlyFFmpegRunner struct {
	requests []ffmpegrunner.FFmpegRequest
}

func (r *receiverOnlyFFmpegRunner) Run(_ context.Context, req ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	r.requests = append(r.requests, req)
	output := req.Args[len(req.Args)-1]
	payload := []byte(string(req.Operation) + "\x00" + strings.Join(req.Args, "\x00"))
	digest := sha256.Sum256(payload)
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	if err := os.WriteFile(output, digest[:], 0o640); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	return ffmpegrunner.FFmpegResult{
		Operation:          req.Operation,
		ExitCode:           0,
		ProcessWallMS:      1,
		CommandFingerprint: ffmpegrunner.Fingerprint(req),
		Parameters:         ffmpegrunner.Sanitize(req),
	}, nil
}

func receiverOnlySHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func receiverOnlyManualPlan(t *testing.T, assets map[string][]byte) *contract.CompiledRenderPlanV2 {
	t.Helper()
	const timelineSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	asset := func(id, kind string, durationUS int64) contract.AssetRefV2 {
		data := assets[id]
		return contract.AssetRefV2{
			AssetID: id, SHA256: receiverOnlySHA(data), SizeBytes: int64(len(data)),
			Kind: kind, DurationUS: durationUS, Width: 640, Height: 360,
		}
	}
	return &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 42,
		TimelineSHA256:   timelineSHA,
		DurationUS:       4_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "libx264", Width: 640, Height: 360,
			FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p",
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio-master",
			SHA256: receiverOnlySHA(assets["audio-master"]), SizeBytes: int64(len(assets["audio-master"])),
			Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 4_000_000,
			TimelineRevision: 42, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{
			TrackID: "main", Segments: []contract.VideoSegmentV2{
				{SegmentID: "manual-a", AssetID: "video-a", SHA256: receiverOnlySHA(assets["video-a"]), TimelineStartFrame: 0, FrameCount: 30, SourceInUS: 0, SourceDurationUS: 1_000_000},
				{SegmentID: "manual-b", AssetID: "video-b", SHA256: receiverOnlySHA(assets["video-b"]), TimelineStartFrame: 45, FrameCount: 30, SourceInUS: 250_000, SourceDurationUS: 1_000_000},
				{SegmentID: "manual-c", AssetID: "video-c", SHA256: receiverOnlySHA(assets["video-c"]), TimelineStartFrame: 90, FrameCount: 30, SourceInUS: 500_000, SourceDurationUS: 1_000_000},
			},
		}},
		Assets: []contract.AssetRefV2{
			asset("video-a", "video", 2_000_000),
			asset("video-b", "video", 2_000_000),
			asset("video-c", "video", 2_000_000),
			asset("audio-master", "final_audio", 4_000_000),
		},
	}
}

func receiverOnlyTaskSpec(t *testing.T, plan *contract.CompiledRenderPlanV2) executor.TaskSpec {
	t.Helper()
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical manual V2 plan: %v", err)
	}
	digest := sha256.Sum256(canonical)
	return executor.TaskSpec{
		Version: 1, JobID: "receiver-only-job", ExecutorID: RenderBatchID,
		Payload: map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
			contract.PayloadKeyCompiledRenderPlanSHA:  hex.EncodeToString(digest[:]),
		},
	}
}

func TestRenderBatchReceiverOnly_ManualPlanLeaseRenderMuxDeterministic(t *testing.T) {
	ctx := context.Background()
	assets := map[string][]byte{
		"video-a":      []byte("receiver-only-video-a"),
		"video-b":      []byte("receiver-only-video-b"),
		"video-c":      []byte("receiver-only-video-c"),
		"audio-master": []byte("receiver-only-final-audio"),
	}
	plan := receiverOnlyManualPlan(t, assets)
	spec := receiverOnlyTaskSpec(t, plan)
	canonicalPlan := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	planSHA := spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string)

	cache, err := workercache.Open(filepath.Join(t.TempDir(), "receiver-only-cache.db"))
	if err != nil {
		t.Fatalf("open lease cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	assetDir := t.TempDir()
	bindings := make(runtimeassets.Bindings, len(assets))
	for assetID, data := range assets {
		path := filepath.Join(assetDir, assetID+".asset")
		if err := os.WriteFile(path, data, 0o640); err != nil {
			t.Fatalf("write asset %s: %v", assetID, err)
		}
		sha := receiverOnlySHA(data)
		if err := cache.Store(ctx, workercache.Entry{
			AssetKey:         workercache.AssetKey(assetID),
			LocalPath:        path,
			SizeBytes:        int64(len(data)),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("store asset %s: %v", assetID, err)
		}
		bindings[assetID] = runtimeassets.Binding{AssetID: assetID, Path: path, SHA256: sha, Size: int64(len(data))}
	}

	leaseKeys := []string{"audio-master", "video-a", "video-b", "video-c"}
	outputRoot := t.TempDir()
	runner := &receiverOnlyFFmpegRunner{}
	exec := NewRenderBatch(runner, outputRoot)
	exec.(*renderBatchExecutor).probe = func(_ context.Context, path string) (publisher.MediaProbe, error) {
		if strings.Contains(path, "audio-master") {
			return publisher.MediaProbe{HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 4}, nil
		}
		return publisher.MediaProbe{HasVideo: true, VideoTrackCount: 1, HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 4}, nil
	}

	run := func(runNumber int) string {
		t.Helper()
		lease, err := worker.AcquireJobClips(ctx, cache, "receiver-only-job", leaseKeys)
		if err != nil {
			t.Fatalf("run %d acquire asset lease: %v", runNumber, err)
		}
		defer func() {
			if releaseErr := lease.ReleaseAll(ctx); releaseErr != nil {
				t.Errorf("run %d deferred lease release: %v", runNumber, releaseErr)
			}
		}()
		if !reflect.DeepEqual(lease.AssetKeys(), leaseKeys) {
			t.Fatalf("run %d lease keys = %v, want %v", runNumber, lease.AssetKeys(), leaseKeys)
		}
		for _, assetID := range leaseKeys {
			entry, found, findErr := cache.Find(ctx, assetID)
			if findErr != nil || !found {
				t.Fatalf("run %d leased asset %s: found=%v err=%v", runNumber, assetID, found, findErr)
			}
			if entry.ActiveJobID != "receiver-only-job" || entry.ActiveLeaseCount != 1 {
				t.Fatalf("run %d asset %s lease state = job:%q count:%d", runNumber, assetID, entry.ActiveJobID, entry.ActiveLeaseCount)
			}
		}

		result, execErr := exec.Execute(runtimeassets.WithBindings(ctx, bindings), nil, spec)
		if execErr != nil {
			t.Fatalf("run %d execute: %v", runNumber, execErr)
		}
		if result.Status != "succeeded" || len(result.Outputs) != 1 {
			t.Fatalf("run %d result = %+v", runNumber, result)
		}
		if result.Metrics["audio_mix_count"] != int64(0) || result.Metrics["audio_encode_count"] != int64(0) || result.Metrics["final_audio_copy"] != int64(1) {
			t.Fatalf("run %d audio metrics = %#v", runNumber, result.Metrics)
		}
		if err := lease.ReleaseAll(ctx); err != nil {
			t.Fatalf("run %d release asset lease: %v", runNumber, err)
		}
		for _, assetID := range leaseKeys {
			entry, found, findErr := cache.Find(ctx, assetID)
			if findErr != nil || !found {
				t.Fatalf("run %d released asset %s: found=%v err=%v", runNumber, assetID, found, findErr)
			}
			if entry.ActiveJobID != "" || entry.ActiveLeaseCount != 0 {
				t.Fatalf("run %d asset %s retained lease: job:%q count:%d", runNumber, assetID, entry.ActiveJobID, entry.ActiveLeaseCount)
			}
		}
		return result.Outputs[0].Hash
	}

	firstHash := run(1)
	if len(runner.requests) != 2 {
		t.Fatalf("first run FFmpeg requests = %d, want visual + mux", len(runner.requests))
	}
	firstRequests := append([]ffmpegrunner.FFmpegRequest(nil), runner.requests...)
	secondHash := run(2)
	if firstHash != secondHash {
		t.Fatalf("deterministic output hash changed: first=%s second=%s", firstHash, secondHash)
	}
	if len(runner.requests) != 4 {
		t.Fatalf("two-run FFmpeg requests = %d, want 4", len(runner.requests))
	}
	if !reflect.DeepEqual(firstRequests, runner.requests[2:]) {
		t.Fatalf("FFmpeg command requests are not deterministic across receiver retries")
	}
	if runner.requests[0].Operation != ffmpegrunner.OperationCompose || runner.requests[1].Operation != ffmpegrunner.OperationEncode {
		t.Fatalf("FFmpeg operation order = %q then %q, want compose then encode", runner.requests[0].Operation, runner.requests[1].Operation)
	}
	visualArgs := strings.Join(runner.requests[0].Args, " ")
	muxArgs := strings.Join(runner.requests[1].Args, " ")
	if !strings.Contains(visualArgs, "-an") || !strings.Contains(visualArgs, "overlay") {
		t.Fatalf("visual-only command = %s", visualArgs)
	}
	for _, required := range []string{"-map 0:v:0", "-map 1:a:0", "-c:v copy", "-c:a copy"} {
		if !strings.Contains(muxArgs, required) {
			t.Errorf("mux command missing %q: %s", required, muxArgs)
		}
	}
	if strings.Contains(muxArgs, "-shortest") {
		t.Fatal("receiver-only mux must not use -shortest")
	}
	if spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string) != canonicalPlan || spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string) != planSHA {
		t.Fatal("manual canonical V2 payload changed during receiver execution")
	}
	if _, err := os.Stat(filepath.Join(outputRoot, "receiver-only-job.video-only.mp4")); !os.IsNotExist(err) {
		t.Fatalf("video-only intermediate was not cleaned up: err=%v", err)
	}
}
