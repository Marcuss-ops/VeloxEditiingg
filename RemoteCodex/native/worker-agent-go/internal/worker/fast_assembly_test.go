package worker

import (
	"context"
	"io"
	"strings"
	"testing"

	videoContract "velox-shared/contract"
	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/logger"
)

func TestRunReadyFastAssemblyRefusesIncompleteFinalManifest(t *testing.T) {
	base := assembly.FinalAssemblyManifest{
		ContractVersion: assembly.ContractVersion, JobID: "job", Revision: 1,
		PreparationHash: "sha256:" + strings.Repeat("0", 64), TimelineRevision: 1,
		TimelineHash: "sha256:" + strings.Repeat("1", 64), ExpectedProfile: assembly.CanonicalAssemblyProfileIDV1,
		Artifacts: []assembly.PublishedArtifact{},
	}
	resolver := downloader.NewManager(downloader.Config{Concurrency: 1}, downloader.TransfererFunc(func(context.Context, context.Context, downloader.DownloadRequest, bool, func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, nil
	}))
	defer resolver.Close()
	reconciler, err := prefetch.NewFinalManifestReconciler(base, downloader.NewCacheResolver(resolver, nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{finalManifestReconciler: reconciler, logger: logger.New(logger.InfoLevel, io.Discard)}
	_, err = w.RunReadyFastAssembly(context.Background(), executorTaskSpecForFastPath("job"))
	if err == nil || !strings.Contains(err.Error(), "want READY") {
		t.Fatalf("incomplete final manifest error = %v", err)
	}
}

func TestFastAssemblyBindingsRequireEveryPlanAssetAndMatchingIntegrity(t *testing.T) {
	w := &Worker{}
	manifest := prefetch.FinalManifestResult{
		Manifest: assembly.FinalAssemblyManifest{JobID: "job", Revision: 2, TimelineRevision: 3},
		PreparedArtifacts: []prefetch.FinalArtifactEvidence{{
			Artifact:  assembly.PublishedArtifact{JobID: "job", AssetID: "video", SHA256: strings.Repeat("a", 64), SizeBytes: 12},
			LocalPath: "/cache/video.mp4",
		}},
	}
	plan := &videoContract.CompiledRenderPlanV2{Assets: []videoContract.AssetRefV2{
		{AssetID: "video", SHA256: strings.Repeat("a", 64), SizeBytes: 12},
		{AssetID: "audio", SHA256: strings.Repeat("b", 64), SizeBytes: 8},
	}}
	if _, err := w.fastAssemblyBindings(plan, manifest); err == nil || !strings.Contains(err.Error(), "audio") {
		t.Fatalf("missing asset gate = %v", err)
	}
}

func TestFastAssemblyBindingsIncludeFinalAudioOutsideAssetsList(t *testing.T) {
	const audioID = "audio-master"
	audioSHA := strings.Repeat("b", 64)
	w := &Worker{}
	manifest := prefetch.FinalManifestResult{
		Manifest: assembly.FinalAssemblyManifest{JobID: "job", Revision: 2, TimelineRevision: 3},
		PreparedArtifacts: []prefetch.FinalArtifactEvidence{{
			Artifact:  assembly.PublishedArtifact{JobID: "job", AssetID: audioID, SHA256: audioSHA, SizeBytes: 8},
			LocalPath: "/cache/final-audio.m4a",
		}},
	}
	plan := &videoContract.CompiledRenderPlanV2{FinalAudio: videoContract.FinalAudioV2{
		Mode: videoContract.AudioModeFinalAudioCopy, AssetID: audioID, SHA256: audioSHA, SizeBytes: 8,
	}}

	bindings, err := w.fastAssemblyBindings(plan, manifest)
	if err != nil {
		t.Fatalf("fastAssemblyBindings() error = %v", err)
	}
	binding, ok := bindings[audioID]
	if !ok || binding.Path != "/cache/final-audio.m4a" || !binding.Verified {
		t.Fatalf("final audio binding = %#v, want verified prepared path", binding)
	}
}

func TestCertifyFastAssemblyRequiresPacketCopyAndVerifiedOutput(t *testing.T) {
	manifest := assembly.FinalAssemblyManifest{JobID: "job", Revision: 2, PreparationHash: "sha256:" + strings.Repeat("0", 64), TimelineRevision: 3}
	plan := &videoContract.CompiledRenderPlanV2{Output: videoContract.OutputContractV2{ProfileID: videoContract.CanonicalVideoProfileIDV1}}
	report := taskrunner.TaskExecutionReport{
		Metrics: map[string]interface{}{"concat_mode": "packet_copy", "packet_copy": int64(1), "frames_decoded": int64(0), "frames_encoded": int64(0)},
		Outputs: []executor.ArtifactRef{{Hash: "sha256:output", SizeBytes: 100}},
	}
	certificate, err := certifyFastAssembly("job", manifest, plan, report)
	if err != nil {
		t.Fatalf("certifyFastAssembly() error = %v", err)
	}
	if !certificate.PacketCopy || certificate.ConcatMode != "packet_copy" || certificate.AssetCount != 0 {
		t.Fatalf("certificate = %#v", certificate)
	}
	report.Metrics["concat_mode"] = "stream_copy"
	if _, err := certifyFastAssembly("job", manifest, plan, report); err == nil {
		t.Fatal("certificate must reject non packet-copy concat")
	}
}

// executorTaskSpecForFastPath is intentionally only used by the pre-READY
// gate test; the plan payload is not reached when the manifest is incomplete.
func executorTaskSpecForFastPath(jobID string) executor.TaskSpec {
	return executor.TaskSpec{JobID: jobID, ExecutorID: fastAssemblyExecutorID, Payload: map[string]interface{}{}}
}
