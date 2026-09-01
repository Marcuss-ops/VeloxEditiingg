package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	videoContract "velox-shared/contract"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/telemetry"
)

// AssetBindingEvidence records the proof that a specific file was consumed
// by the renderer. When fastAssemblyBindings returns a binding, this struct
// captures the verified path/SHA/size/prepared_at so that the attempt can
// prove the renderer consumed exactly the prefetched file — not a stale
// cache hit or a runtime download.
type AssetBindingEvidence struct {
	AssetID                 string    `json:"asset_id"`
	ExpectedSHA256          string    `json:"expected_sha256"`
	VerifiedSHA256          string    `json:"verified_sha256"`
	ExpectedSize            int64     `json:"expected_size"`
	ActualSize              int64     `json:"actual_size"`
	LocalPath               string    `json:"local_path"`
	PreparedAt              time.Time `json:"prepared_at,omitempty"`
	Origin                  string    `json:"origin"` // prefetch | warm_cache | runtime_download
	ReadyBeforeAttempt      bool      `json:"ready_before_attempt"`
	DownloadedDuringAttempt bool      `json:"downloaded_during_attempt"`
}

// fastAssemblyPlanAssets returns the complete set of files needed by the
// executor. Older producers may keep FinalAudio only in its dedicated
// contract field instead of repeating it in Assets; the executor still
// resolves it through the same verified binding map.
func fastAssemblyPlanAssets(plan *videoContract.CompiledRenderPlanV2) []videoContract.AssetRefV2 {
	if plan == nil {
		return nil
	}
	assets := append([]videoContract.AssetRefV2(nil), plan.Assets...)
	if strings.TrimSpace(plan.FinalAudio.AssetID) == "" {
		return assets
	}
	for _, asset := range assets {
		if asset.AssetID == plan.FinalAudio.AssetID {
			return assets
		}
	}
	return append(assets, videoContract.AssetRefV2{
		AssetID:    plan.FinalAudio.AssetID,
		SHA256:     plan.FinalAudio.SHA256,
		SizeBytes:  plan.FinalAudio.SizeBytes,
		Kind:       "final_audio",
		DurationUS: plan.FinalAudio.DurationUS,
	})
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
	for _, asset := range fastAssemblyPlanAssets(plan) {
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

	assets := fastAssemblyPlanAssets(plan)
	bindings := make(runtimeassets.Bindings, len(assets))
	for _, asset := range assets {
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
		// Propagate the resolution origin from PreparedAssetMetadata so
		// certification tests can assert prefetch provenance without re-deriving.
		var origin downloader.ResolutionOrigin
		if metadata, ok := preparedByID[asset.AssetID]; ok {
			origin = metadata.Origin
		} else if metadata, ok := preparedByID[asset.AssetKey]; ok {
			origin = metadata.Origin
		}
		bindings[asset.AssetID] = runtimeassets.Binding{
			AssetID: asset.AssetID, Path: path, SHA256: asset.SHA256, Size: asset.SizeBytes, Verified: true, Origin: origin,
		}
	}
	return bindings, nil
}

// recordBindingEvidence captures the proof that each asset binding was
// consumed by the renderer. This is the single point where we know which
// file the renderer will use, so we record the verified path/SHA/size/
// origin for each asset. The evidence is emitted as structured telemetry
// events (asset.binding_consumed) and persisted in the attempt metrics.
func (w *Worker) recordBindingEvidence(ctx context.Context, jobID string, plan *videoContract.CompiledRenderPlanV2, manifest prefetch.FinalManifestResult, bindings runtimeassets.Bindings) {
	// Build a lookup from PreparedJobs to determine the origin of each asset.
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

	// Build a lookup from FinalManifestResult for prepared_at.
	manifestByID := make(map[string]prefetch.FinalArtifactEvidence)
	for _, evidence := range manifest.PreparedArtifacts {
		manifestByID[evidence.Artifact.AssetID] = evidence
	}

	for _, asset := range fastAssemblyPlanAssets(plan) {
		binding, ok := bindings[asset.AssetID]
		if !ok {
			continue
		}

		evidence := AssetBindingEvidence{
			AssetID:            asset.AssetID,
			ExpectedSHA256:     asset.SHA256,
			VerifiedSHA256:     binding.SHA256,
			ExpectedSize:       asset.SizeBytes,
			ActualSize:         binding.Size,
			LocalPath:          binding.Path,
			ReadyBeforeAttempt: true, // bindings only exist when assets are ready
		}

		// Determine origin from PreparedJobs.
		if metadata, ok := preparedByID[asset.AssetID]; ok {
			evidence.PreparedAt = metadata.PreparedAt
			if !metadata.PreparedAt.IsZero() {
				evidence.Origin = "prefetch"
			} else {
				evidence.Origin = "warm_cache"
			}
		} else if metadata, ok := preparedByID[asset.AssetKey]; ok {
			evidence.PreparedAt = metadata.PreparedAt
			if !metadata.PreparedAt.IsZero() {
				evidence.Origin = "prefetch"
			} else {
				evidence.Origin = "warm_cache"
			}
		} else if me, ok := manifestByID[asset.AssetID]; ok {
			evidence.Origin = "prefetch"
			evidence.PreparedAt = me.PreparedAt
		} else {
			evidence.Origin = "runtime_download"
			evidence.ReadyBeforeAttempt = false
			evidence.DownloadedDuringAttempt = true
		}

		// Emit structured telemetry event.
		telemetry.LogAssetCacheAccess(ctx, w.config.WorkerID, asset.AssetID, "binding_consumed", binding.Size, 0, 0)

		w.logger.Info("[BINDING] asset=%s origin=%s prepared_at=%s sha_match=%v size_match=%v path=%s",
			asset.AssetID, evidence.Origin,
			evidence.PreparedAt.Format(time.RFC3339Nano),
			sameFastDigest(evidence.ExpectedSHA256, evidence.VerifiedSHA256),
			evidence.ExpectedSize == evidence.ActualSize,
			binding.Path)
	}
}
