package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	videoContract "velox-shared/contract"
	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

const fastAssemblyExecutorID = "video.assemble.copy.v1"

// FastAssemblyCertificate is emitted only after profile validation, complete
// local bindings, the strict copy-only executor, and output hash validation
// have all succeeded. It is a certificate of the path taken, not a lifecycle
// mutation of the master-owned task.
type FastAssemblyCertificate struct {
	JobID            string    `json:"job_id"`
	ProfileID        string    `json:"profile_id"`
	TimelineRevision uint64    `json:"timeline_revision"`
	PreparationHash  string    `json:"preparation_hash"`
	AssetCount       int       `json:"asset_count"`
	ConcatMode       string    `json:"concat_mode"`
	PacketCopy       bool      `json:"packet_copy"`
	FramesDecoded    int64     `json:"frames_decoded"`
	FramesEncoded    int64     `json:"frames_encoded"`
	CertifiedAt      time.Time `json:"certified_at"`
}

// FastAssemblyOutcome combines the task report and the copy-only certificate.
type FastAssemblyOutcome struct {
	Report      *taskrunner.TaskExecutionReport
	Certificate FastAssemblyCertificate
}

// ApplyFinalManifestDeltaAndFastAssemble is the atomic control-plane entry
// point for late-bound assembly. It accepts the delta, waits for its local
// artifact resolution, and starts assembly only when the resulting manifest
// is READY. A PREPARING manifest can therefore never consume an execution
// slot through this method.
func (w *Worker) ApplyFinalManifestDeltaAndFastAssemble(ctx context.Context, delta assembly.FinalManifestDelta, spec executor.TaskSpec) (prefetch.FinalManifestResult, FastAssemblyOutcome, error) {
	manifest, err := w.ApplyFinalManifestDelta(ctx, delta)
	if err != nil {
		return prefetch.FinalManifestResult{}, FastAssemblyOutcome{}, err
	}
	if !manifest.Ready {
		return manifest, FastAssemblyOutcome{}, fmt.Errorf("fast assembly deferred: final manifest state is %s", manifest.State)
	}
	outcome, err := w.RunReadyFastAssembly(ctx, spec)
	return manifest, outcome, err
}

// RunReadyFastAssembly runs the zero-download assembly path for a compiled
// RenderPlanV2. It refuses to start unless the Final Manifest is READY and
// every plan asset has a verified local binding. The existing
// video.assemble.copy.v1 executor then performs its own independent profile,
// stream-identity, keyframe, concat and packet-copy gates.
func (w *Worker) RunReadyFastAssembly(ctx context.Context, spec executor.TaskSpec) (FastAssemblyOutcome, error) {
	if w == nil {
		return FastAssemblyOutcome{}, fmt.Errorf("worker: nil worker")
	}
	if spec.ExecutorID != fastAssemblyExecutorID {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly requires executor %q, got %q", fastAssemblyExecutorID, spec.ExecutorID)
	}
	manifestResult := w.FinalAssemblyManifestSnapshot()
	if !manifestResult.Ready || manifestResult.State != prefetch.FinalManifestReady {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly refused: final manifest is %s, want READY", manifestResult.State)
	}
	if manifestResult.Manifest.JobID != spec.JobID {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly job_id=%q does not match manifest job_id=%q", spec.JobID, manifestResult.Manifest.JobID)
	}
	if w.taskRunner == nil {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly requires a configured task runner")
	}
	plan, err := decodeFastAssemblyPlan(spec)
	if err != nil {
		return FastAssemblyOutcome{}, err
	}
	profile, err := videoContract.KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly profile gate: %w", err)
	}
	if manifestResult.Manifest.ExpectedProfile != profile.ProfileID {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly profile mismatch: manifest=%q plan=%q", manifestResult.Manifest.ExpectedProfile, profile.ProfileID)
	}
	if uint64(plan.TimelineRevision) != manifestResult.Manifest.TimelineRevision || !sameFastDigest(plan.TimelineSHA256, manifestResult.Manifest.TimelineHash) {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly timeline identity does not match final manifest")
	}

	assetsReady, assetsMissing := w.fastAssemblyAssetAvailability(plan, manifestResult)
	telemetry.GetPrometheusMetrics().RecordAssemblyExecution(assetsReady, assetsMissing, 0)
	if assetsMissing > 0 {
		return FastAssemblyOutcome{}, fmt.Errorf("fast assembly asset gate: %d assets are missing or unverifiable at execution", assetsMissing)
	}
	bindings, err := w.fastAssemblyBindings(plan, manifestResult)
	if err != nil {
		return FastAssemblyOutcome{}, err
	}
	report, err := w.taskRunner.RunWithBindings(ctx, spec, bindings)
	if err != nil {
		return FastAssemblyOutcome{Report: &report}, fmt.Errorf("fast assembly execution: %w", err)
	}
	if report.Status != "succeeded" {
		return FastAssemblyOutcome{Report: &report}, fmt.Errorf("fast assembly failed: code=%q detail=%q", report.ErrorCode, report.ErrorDetail)
	}
	certificate, err := certifyFastAssembly(spec.JobID, manifestResult.Manifest, plan, report)
	if err != nil {
		return FastAssemblyOutcome{Report: &report}, err
	}
	return FastAssemblyOutcome{Report: &report, Certificate: certificate}, nil
}

func decodeFastAssemblyPlan(spec executor.TaskSpec) (*videoContract.CompiledRenderPlanV2, error) {
	plan, err := videoContract.DecodeCompiledRenderPlanV2Payload(spec.Payload)
	if err != nil {
		return nil, fmt.Errorf("fast assembly plan decode: %w", err)
	}
	if plan == nil {
		return nil, fmt.Errorf("fast assembly requires a compiled RenderPlanV2 JSON payload")
	}
	if plan.Output.ProfileID == "" {
		return nil, fmt.Errorf("fast assembly profile gate: output.profile_id is required")
	}
	return plan, nil
}

func (w *Worker) fastAssemblyAssetAvailability(plan *videoContract.CompiledRenderPlanV2, manifest prefetch.FinalManifestResult) (ready, missing int) {
	byID := make(map[string]prefetch.FinalArtifactEvidence, len(manifest.PreparedArtifacts))
	for _, evidence := range manifest.PreparedArtifacts {
		byID[evidence.Artifact.AssetID] = evidence
	}
	preparedByID := make(map[string]prefetch.PreparedAssetMetadata)
	if w.prefetchScheduler != nil {
		for _, job := range w.prefetchScheduler.PreparedJobs() {
			for _, metadata := range job.Assets {
				if metadata.AssetID != "" {
					preparedByID[metadata.AssetID] = metadata
				}
				preparedByID[metadata.AssetKey] = metadata
			}
		}
	}
	for _, asset := range plan.Assets {
		path, digest, size := "", "", int64(0)
		if evidence, ok := byID[asset.AssetID]; ok {
			path, digest, size = evidence.LocalPath, evidence.Artifact.SHA256, evidence.Artifact.SizeBytes
		} else if metadata, ok := preparedByID[asset.AssetID]; ok {
			path, digest, size = metadata.LocalPath, metadata.SHA256, metadata.SizeBytes
		} else if metadata, ok := preparedByID[asset.AssetKey]; ok {
			path, digest, size = metadata.LocalPath, metadata.SHA256, metadata.SizeBytes
		}
		if strings.TrimSpace(path) != "" && sameFastDigest(digest, asset.SHA256) && size == asset.SizeBytes {
			ready++
		} else {
			missing++
		}
	}
	return ready, missing
}

func (w *Worker) fastAssemblyBindings(plan *videoContract.CompiledRenderPlanV2, manifest prefetch.FinalManifestResult) (runtimeassets.Bindings, error) {
	byID := make(map[string]prefetch.FinalArtifactEvidence, len(manifest.PreparedArtifacts))
	for _, evidence := range manifest.PreparedArtifacts {
		byID[evidence.Artifact.AssetID] = evidence
	}
	// Source clips can be prepared by the early FutureAssetPlan and need not
	// be repeated in every late FinalManifestDelta. Include that local read
	// model as a fallback, while keeping the same hash/size gate.
	preparedByID := make(map[string]prefetch.PreparedAssetMetadata)
	if w.prefetchScheduler != nil {
		for _, job := range w.prefetchScheduler.PreparedJobs() {
			for _, metadata := range job.Assets {
				if metadata.AssetID != "" {
					preparedByID[metadata.AssetID] = metadata
				}
				preparedByID[metadata.AssetKey] = metadata
			}
		}
	}

	bindings := make(runtimeassets.Bindings, len(plan.Assets))
	for _, asset := range plan.Assets {
		path := ""
		digest := ""
		size := int64(0)
		if evidence, ok := byID[asset.AssetID]; ok {
			path = evidence.LocalPath
			digest = evidence.Artifact.SHA256
			size = evidence.Artifact.SizeBytes
		} else if metadata, ok := preparedByID[asset.AssetID]; ok {
			path = metadata.LocalPath
			digest = metadata.SHA256
			size = metadata.SizeBytes
		} else if metadata, ok := preparedByID[asset.AssetKey]; ok {
			path = metadata.LocalPath
			digest = metadata.SHA256
			size = metadata.SizeBytes
		}
		if strings.TrimSpace(path) == "" || !sameFastDigest(digest, asset.SHA256) || size != asset.SizeBytes {
			return nil, fmt.Errorf("fast assembly asset gate: %q is not locally verified with matching SHA256/size", asset.AssetID)
		}
		bindings[asset.AssetID] = runtimeassets.Binding{
			AssetID: asset.AssetID, Path: path, SHA256: asset.SHA256, Size: asset.SizeBytes, Verified: true,
		}
	}
	return bindings, nil
}

func certifyFastAssembly(jobID string, manifest assembly.FinalAssemblyManifest, plan *videoContract.CompiledRenderPlanV2, report taskrunner.TaskExecutionReport) (FastAssemblyCertificate, error) {
	concatMode, ok := report.Metrics["concat_mode"].(string)
	if !ok || concatMode != "packet_copy" {
		return FastAssemblyCertificate{}, fmt.Errorf("fast assembly certificate rejected: concat_mode=%v, want packet_copy", report.Metrics["concat_mode"])
	}
	packetCopy, ok := report.Metrics["packet_copy"].(int64)
	if !ok || packetCopy != 1 {
		return FastAssemblyCertificate{}, fmt.Errorf("fast assembly certificate rejected: packet_copy=%v, want 1", report.Metrics["packet_copy"])
	}
	if len(report.Outputs) != 1 || strings.TrimSpace(report.Outputs[0].Hash) == "" || report.Outputs[0].SizeBytes <= 0 {
		return FastAssemblyCertificate{}, fmt.Errorf("fast assembly certificate rejected: final output lacks verified hash/size")
	}
	return FastAssemblyCertificate{
		JobID: jobID, ProfileID: plan.Output.ProfileID, TimelineRevision: manifest.TimelineRevision,
		PreparationHash: manifest.PreparationHash, AssetCount: len(plan.Assets), ConcatMode: concatMode,
		PacketCopy: true, FramesDecoded: metricInt64(report.Metrics, "frames_decoded"),
		FramesEncoded: metricInt64(report.Metrics, "frames_encoded"), CertifiedAt: time.Now().UTC(),
	}, nil
}

func metricInt64(metrics map[string]interface{}, key string) int64 {
	if value, ok := metrics[key].(int64); ok {
		return value
	}
	return 0
}

func sameFastDigest(left, right string) bool {
	left = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(left)), "sha256:")
	right = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(right)), "sha256:")
	return left != "" && left == right
}
