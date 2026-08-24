package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/prefetch"
)

// ApplyFinalManifestDelta accepts a late-bound Final Manifest update from
// RenderingGen/Chronon. The first delta establishes the final-manifest base
// from its immutable preparation/timeline/profile identity; later deltas are
// CAS-checked by FinalManifestReconciler. Artifact bytes are resolved through
// the worker's canonical SHA-256 cache, so an existing local artifact is a
// cache hit and is never downloaded again.
func (w *Worker) ApplyFinalManifestDelta(ctx context.Context, delta assembly.FinalManifestDelta) (prefetch.FinalManifestResult, error) {
	if w == nil {
		return prefetch.FinalManifestResult{}, fmt.Errorf("worker: nil worker")
	}
	if err := delta.Validate(); err != nil {
		return prefetch.FinalManifestResult{}, err
	}

	w.finalManifestMu.Lock()
	if w.finalManifestReconciler == nil {
		base, err := finalManifestBaseFromDelta(delta)
		if err != nil {
			w.finalManifestMu.Unlock()
			return prefetch.FinalManifestResult{}, err
		}
		w.finalManifestReconciler, err = prefetch.NewFinalManifestReconciler(base, w.assetCacheResolver(), time.Now)
		if err != nil {
			w.finalManifestMu.Unlock()
			return prefetch.FinalManifestResult{}, err
		}
	}
	reconciler := w.finalManifestReconciler
	w.finalManifestMu.Unlock()

	result, err := reconciler.ApplyDelta(ctx, delta)
	if err != nil {
		if w.logger != nil {
			w.logger.Warn("[FINAL_MANIFEST] rejected job=%s revision=%d: %v", delta.JobID, delta.Revision, err)
		}
		return prefetch.FinalManifestResult{}, err
	}
	if w.logger != nil {
		w.logger.Info("[FINAL_MANIFEST] job=%s revision=%d disposition=%s state=%s artifacts_resolved=%d ready=%t", delta.JobID, result.Manifest.Revision, result.Disposition, result.State, len(result.ResolvedArtifacts), result.Ready)
	}
	return result, nil
}

// InstallFinalAssemblyManifest installs a previously accepted full manifest.
// It is useful when a worker reconnects and receives the control-plane
// snapshot before the next delta; it does not resolve anything by itself.
func (w *Worker) InstallFinalAssemblyManifest(manifest assembly.FinalAssemblyManifest) error {
	if w == nil {
		return fmt.Errorf("worker: nil worker")
	}
	w.finalManifestMu.Lock()
	defer w.finalManifestMu.Unlock()
	reconciler, err := prefetch.NewFinalManifestReconciler(manifest, w.assetCacheResolver(), time.Now)
	if err != nil {
		return err
	}
	w.finalManifestReconciler = reconciler
	return nil
}

// FinalAssemblyManifestSnapshot returns the worker's current late-artifact
// read model. The zero value means no final manifest has been installed yet.
func (w *Worker) FinalAssemblyManifestSnapshot() prefetch.FinalManifestResult {
	if w == nil {
		return prefetch.FinalManifestResult{}
	}
	w.finalManifestMu.Lock()
	reconciler := w.finalManifestReconciler
	w.finalManifestMu.Unlock()
	if reconciler == nil {
		return prefetch.FinalManifestResult{}
	}
	return reconciler.Snapshot()
}

func finalManifestBaseFromDelta(delta assembly.FinalManifestDelta) (assembly.FinalAssemblyManifest, error) {
	if delta.BaseRevision == 0 {
		return assembly.FinalAssemblyManifest{}, fmt.Errorf("worker: first final manifest delta requires positive base_revision")
	}
	if delta.TimelineRevision == 0 || strings.TrimSpace(delta.TimelineHash) == "" || strings.TrimSpace(delta.ExpectedProfile) == "" {
		return assembly.FinalAssemblyManifest{}, fmt.Errorf("worker: first final manifest delta requires timeline and profile identity")
	}
	base := assembly.FinalAssemblyManifest{
		ContractVersion:  delta.ContractVersion,
		JobID:            delta.JobID,
		Revision:         delta.BaseRevision,
		PreparationHash:  delta.PreparationHash,
		TimelineRevision: delta.TimelineRevision,
		TimelineHash:     delta.TimelineHash,
		ExpectedProfile:  delta.ExpectedProfile,
		Artifacts:        []assembly.PublishedArtifact{},
	}
	if err := base.Validate(); err != nil {
		return assembly.FinalAssemblyManifest{}, fmt.Errorf("worker: invalid final manifest base: %w", err)
	}
	return base, nil
}
