package executors

import (
	"context"
	"errors"
	"fmt"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/performance"
	"velox-worker-agent/pkg/video/pipeline"
)

// scene_composite_output.go owns the fail-closed artifact boundary of
// SceneComposite.Execute: primary output manifest verification and the
// progress-receipt (sidecar) manifest verification. The executor
// orchestration lives in scene_composite.go.

// verifyAndBuildOutputs is the fail-closed boundary that prevents a
// mock/partial renderer from producing a misleading succeeded task. It
// requires the primary output AND its progress receipt to both exist and
// have a real manifest before building the output artifact references.
//
// On failure it returns a non-nil failResult that Execute returns
// verbatim (outputs and outputManifest are nil). On success it returns the
// artifact references and the primary manifest for the caller's final
// quality telemetry and plan completion.
func verifyAndBuildOutputs(ctx context.Context, outputPath string, startedAt time.Time, metrics map[string]interface{}, rawMetrics *telemetry.RawExecutionMetrics, runMetrics pipeline.RunMetrics, clipCount int, planHandle *telemetry.EventHandle, rec *telemetry.EventRecorder) (outputs []executor.ArtifactRef, outputManifest *publisher.OutputManifest, failResult *executor.ExecutionResult) {
	// Compute output file hash and size for artifact metadata. The artifact
	// clock starts only after rendering has completed: it must not include
	// compile/render time, otherwise artifact_total_ms is mislabeled.
	// A successful renderer invocation is not sufficient: both the
	// primary output and its progress receipt must exist and have a
	// real manifest before this executor can report success.
	var outputHash string
	var outputSize int64
	artifactStarted := time.Now()
	if rec != nil {
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginValidation, Scope: telemetry.ScopeAttempt, Component: "quality", Action: "sha256"}, telemetry.StatusOK, "", "")
	}
	outputManifest, manifestErr := publisher.ComputeLocalManifest(ctx, outputPath)
	if manifestErr != nil {
		planHandle.Abort("quality_manifest", manifestErr.Error())
		metrics["output.manifest_error"] = manifestErr.Error()
		return nil, nil, &executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "output_manifest_missing",
			ErrorDetail: fmt.Sprintf("render output manifest: %v", manifestErr),
			RawMetrics:  rawMetrics,
			Metrics:     metrics,
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}
	}
	// Prefer the native engine identity only for a verified append-only
	// output. Any backward seek, missing digest, or invalid size forces the
	// canonical on-disk manifest path; never publish an unverified native SHA.
	if rm := runMetrics.RenderMetrics; rm.SHA256Valid && !rm.BackwardSeekSeen && rm.SHA256 != "" && rm.OutputSizeBytes > 0 {
		outputManifest.SHA256Hex = rm.SHA256
		outputManifest.SizeBytes = rm.OutputSizeBytes
	}
	outputHash = outputManifest.SHA256Hex
	outputSize = outputManifest.SizeBytes
	// Keep the historical output.hash_ms key, but make it mean exactly the
	// streaming SHA phase rather than the complete manifest operation.
	metrics["output.hash_ms"] = outputManifest.Timings.SHA256MS
	if outputSize <= 0 {
		planHandle.Abort("quality_empty", "render output manifest has zero bytes")
		metrics["output.manifest_error"] = "render output is empty"
		return nil, nil, &executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "output_manifest_empty",
			ErrorDetail: "render output manifest has zero bytes",
			RawMetrics:  rawMetrics,
			Metrics:     metrics,
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}
	}
	metrics["output.bytes"] = outputSize
	rawMetrics.OutputBytes = outputSize
	rawMetrics.OutputFileSize = outputSize
	rawMetrics.OutputSha256 = outputHash
	rawMetrics.FfprobeValid = int32(boolToInt(outputManifest.FfprobeValid))
	rawMetrics.HasVideoStream = outputManifest.HasVideoStream
	rawMetrics.HasAudioStream = outputManifest.HasAudioStream
	rawMetrics.AudioTrackCount = int32(outputManifest.AudioTrackCount)
	// Re-project amplification with the VERIFIED artifact size (the
	// manifest is the publisher's authoritative byte count). The other
	// derived KPIs do not depend on the output size and are already
	// final.
	derivedVerified := performance.DerivedFromRenderMetrics(runMetrics.RenderMetrics, runMetrics.TotalMs, clipCount, outputSize)
	metrics["derived.read_amplification"] = derivedVerified.ReadAmplification
	metrics["derived.write_amplification"] = derivedVerified.WriteAmplification
	// Quality telemetry must describe the artifact that was actually
	// produced. ComputeLocalManifest has already hashed and ffprobed this
	// final file; do not infer these values from the render plan or emit a
	// synthetic success flag.
	metrics["quality.ffprobe.valid"] = int64(boolToInt(outputManifest.FfprobeValid))
	metrics["quality.ffprobe.ok"] = int64(boolToInt(outputManifest.FfprobeOK))
	metrics["quality.has.video.stream"] = outputManifest.HasVideoStream
	metrics["quality.has.audio.stream"] = outputManifest.HasAudioStream
	metrics["quality.audio.track.count"] = int64(outputManifest.AudioTrackCount)
	metrics["quality.video.codec"] = outputManifest.Codec
	metrics["quality.audio.codec"] = outputManifest.AudioCodec
	metrics["quality.output.file.size"] = outputManifest.SizeBytes
	metrics["output.file.size"] = outputManifest.SizeBytes
	if outputManifest.FfprobeErr != "" {
		metrics["quality.ffprobe.error"] = outputManifest.FfprobeErr
	}
	metrics["executor.total_ms"] = time.Since(startedAt).Milliseconds()

	outputs = []executor.ArtifactRef{{Type: "render.output", Hash: outputHash, URI: outputPath, SizeBytes: outputSize}}
	sidecarPath := outputPath + ".progress.json"
	// The progress sidecar is a JSON receipt, not a media artifact: the
	// light manifest (SHA + size + sniff) is the correct computation and
	// avoids a wasted ffprobe process spawn on the hot path.
	if sidecarManifest, sidecarErr := publisher.ComputeLocalManifestLight(ctx, sidecarPath); sidecarErr == nil && sidecarManifest.SizeBytes > 0 {
		// The renderer owns this file as a durable receipt. Do not remove it
		// here: the worker must keep it available through declaration,
		// upload, and the master's commit acknowledgement.
		outputs = append(outputs, executor.ArtifactRef{
			Type:      "engine.progress.sidecar",
			Hash:      sidecarManifest.SHA256Hex,
			URI:       sidecarPath,
			SizeBytes: sidecarManifest.SizeBytes,
		})
		metrics["sidecar.present"] = true
		metrics["sidecar.bytes"] = sidecarManifest.SizeBytes
		projectRenderProfile(metrics, runMetrics, outputManifest.Timings.SHA256MS, outputManifest.Timings.FfprobeMS, sidecarManifest.Timings.TotalMS, time.Since(artifactStarted).Milliseconds())
	} else {
		// The sidecar is the renderer's progress receipt and is part of
		// the artifact contract. Do not silently report success without
		// it: the worker cannot register a complete operation receipt.
		metrics["sidecar.present"] = false
		if sidecarErr == nil {
			sidecarErr = errors.New("render progress sidecar is empty")
		}
		metrics["sidecar.error"] = sidecarErr.Error()
		return nil, nil, &executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "progress_sidecar_missing",
			ErrorDetail: fmt.Sprintf("render progress sidecar manifest: %v", sidecarErr),
			RawMetrics:  rawMetrics,
			Metrics:     metrics,
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}
	}

	return outputs, outputManifest, nil
}
