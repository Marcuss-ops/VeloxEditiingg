package worker

import (
	"context"
	"io"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

type fastAssemblyIntegrationResolver struct{}

func (fastAssemblyIntegrationResolver) Resolve(_ context.Context, request downloader.DownloadRequest) (downloader.CacheResolution, error) {
	return downloader.CacheResolution{
		AssetID: request.AssetID, LocalPath: "/cache/" + request.AssetID,
		SHA256: request.SHA256, DownloadBytes: request.SizeBytes,
		Outcome: downloader.CacheOutcomeMissNotFound,
	}, nil
}

type fastAssemblyIntegrationExecutor struct{}

func (fastAssemblyIntegrationExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: fastAssemblyExecutorID, Version: 1,
		ResourceClass: executor.ResourceIO, TemporalMode: executor.TemporalGlobal,
	}
}

func (fastAssemblyIntegrationExecutor) Validate(executor.TaskSpec) error { return nil }

func (fastAssemblyIntegrationExecutor) Execute(context.Context, executor.ExecutionContext, executor.TaskSpec) (executor.ExecutionResult, error) {
	return executor.ExecutionResult{
		Status:  "succeeded",
		Outputs: []executor.ArtifactRef{{Hash: strings.Repeat("f", 64), SizeBytes: 123}},
		Metrics: map[string]interface{}{
			"concat_mode": "packet_copy", "packet_copy": int64(1),
			"frames_decoded": int64(0), "frames_encoded": int64(0),
		},
	}, nil
}

func TestFinalManifestReadyFastAssemblyIntegration(t *testing.T) {
	const (
		jobID        = "integration-fast-job"
		prepHash     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		timelineHash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		videoHash    = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		audioHash    = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	)
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 7, TimelineSHA256: timelineHash, DurationUS: 1_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080,
			FPSNum: 24, FPSDen: 1, PixelFormat: "yuv420p",
			ProfileID: contract.CanonicalVideoProfileIDV1, CodecProfile: "high", CodecLevel: "4.0",
			GOPSize: 48, BFrames: 0, ClosedGOP: true, TimeBaseNum: 1, TimeBaseDen: 90000,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: audioHash, SizeBytes: 50,
			Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1_000_000,
			TimelineRevision: 7, TimelineSHA256: timelineHash,
		},
		VideoTracks: []contract.VideoTrackV2{{
			TrackID: "main", Segments: []contract.VideoSegmentV2{{
				SegmentID: "video-segment", AssetID: "video", SHA256: videoHash,
				TimelineStartFrame: 0, FrameCount: 24, SourceInUS: 0, SourceDurationUS: 1_000_000,
			}},
		}},
		Assets: []contract.AssetRefV2{
			{
				AssetID: "video", SHA256: videoHash, SizeBytes: 100, Kind: "prepared_video_fragment",
				MIME: "video/mp4", DurationUS: 1_000_000, Width: 1920, Height: 1080,
				ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 24,
				TimelineRevision: 7, TimelineSHA256: timelineHash, TimelineStartFrame: 0,
				FirstFrameKeyframe: true, ClosedGOP: true,
			},
			{AssetID: "audio", SHA256: audioHash, SizeBytes: 50, Kind: "final_audio", MIME: "audio/mp4", DurationUS: 1_000_000},
		},
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	spec := executor.TaskSpec{
		Version: 1, JobID: jobID, ExecutorID: fastAssemblyExecutorID,
		Payload: map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: string(canonical),
			contract.PayloadKeyCompiledRenderPlanSHA:  contract.HashCompiledPlanV2(canonical),
		},
	}

	base := assembly.FinalAssemblyManifest{
		ContractVersion: assembly.ContractVersion, JobID: jobID, Revision: 1,
		PreparationHash: prepHash, TimelineRevision: 7, TimelineHash: timelineHash,
		ExpectedProfile: contract.CanonicalVideoProfileIDV1, Artifacts: []assembly.PublishedArtifact{},
	}
	reconciler, err := prefetch.NewFinalManifestReconciler(base, fastAssemblyIntegrationResolver{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry := executor.NewRegistry()
	registry.MustRegister(fastAssemblyIntegrationExecutor{})
	w := &Worker{logger: logger.New(logger.InfoLevel, io.Discard),
		taskRunner:              taskrunner.NewTaskRunner(registry, nil),
		finalManifestReconciler: reconciler,
		config:                  &config.WorkerConfig{WorkerID: "test-worker-fast-assembly"},
	}

	delta := assembly.FinalManifestDelta{
		ContractVersion: assembly.ContractVersion, JobID: jobID, BaseRevision: 1, Revision: 2,
		PreparationHash: prepHash, TimelineRevision: 7, TimelineHash: timelineHash,
		ExpectedProfile: contract.CanonicalVideoProfileIDV1,
		UpsertedArtifacts: []assembly.PublishedArtifact{
			{JobID: jobID, TimelineRevision: 7, AssetID: "video", StorageURL: "https://master.test/video", SHA256: videoHash, SizeBytes: 100, ProfileID: contract.CanonicalVideoProfileIDV1, Producer: assembly.ProducerChronon},
			{JobID: jobID, TimelineRevision: 7, AssetID: "audio", StorageURL: "https://master.test/audio", SHA256: audioHash, SizeBytes: 50, ProfileID: contract.CanonicalVideoProfileIDV1, Producer: assembly.ProducerRenderingGen},
		},
	}
	result, outcome, err := w.ApplyFinalManifestDeltaAndFastAssemble(context.Background(), delta, spec)
	if err != nil {
		t.Fatalf("FinalManifest -> fast assembly integration: %v", err)
	}
	if !result.Ready || result.State != prefetch.FinalManifestReady || len(result.PreparedArtifacts) != 2 {
		t.Fatalf("final manifest result=%#v, want READY with two prepared artifacts", result)
	}
	if outcome.Report == nil || outcome.Report.Status != "succeeded" {
		t.Fatalf("assembly report=%#v, want succeeded", outcome.Report)
	}
	if !outcome.Certificate.PacketCopy || outcome.Certificate.ConcatMode != "packet_copy" || outcome.Certificate.AssetCount != 2 {
		t.Fatalf("assembly certificate=%#v, want packet-copy certificate for both assets", outcome.Certificate)
	}
	if outcome.Certificate.ProfileID != contract.CanonicalVideoProfileIDV1 || outcome.Certificate.TimelineRevision != 7 {
		t.Fatalf("certificate identity=%#v", outcome.Certificate)
	}
}
