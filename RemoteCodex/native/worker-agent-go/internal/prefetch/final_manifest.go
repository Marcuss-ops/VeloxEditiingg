package prefetch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"velox-shared/assetref"
	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/downloader"
)

// FinalManifestState is the worker read-model state after applying a final
// manifest delta. PREPARED means at least one artifact is locally verified;
// READY means every artifact in the current manifest is locally verified.
type FinalManifestState string

const (
	FinalManifestPreparing FinalManifestState = "PREPARING"
	FinalManifestPrepared  FinalManifestState = "PREPARED"
	FinalManifestReady     FinalManifestState = "READY"
)

// FinalArtifactEvidence records the local verification result for one
// published artifact. The cache resolver has already performed the SHA-256 and
// size gate before this evidence is stored.
type FinalArtifactEvidence struct {
	Artifact   assembly.PublishedArtifact `json:"artifact"`
	LocalPath  string                     `json:"local_path,omitempty"`
	CacheHit   bool                       `json:"cache_hit"`
	PreparedAt time.Time                  `json:"prepared_at"`
}

// FinalManifestResult is the atomic result of applying one delta. Replays are
// reported as RevisionReplay and do not call the resolver when their evidence
// is already present.
type FinalManifestResult struct {
	Manifest    assembly.FinalAssemblyManifest
	Disposition assembly.RevisionDisposition
	State       FinalManifestState
	Ready       bool
	// ResolvedArtifacts contains only artifacts touched by this delta.
	ResolvedArtifacts []FinalArtifactEvidence
	// PreparedArtifacts contains all currently verified artifacts in the
	// manifest, allowing the READY assembly path to build complete bindings
	// after several incremental deltas.
	PreparedArtifacts []FinalArtifactEvidence
}

// FinalArtifactResolver is intentionally narrower than the full download
// manager. Production wires CacheResolver; tests can count calls with a fake.
type FinalArtifactResolver interface {
	Resolve(context.Context, downloader.DownloadRequest) (downloader.CacheResolution, error)
}

// FinalManifestReconciler owns only the final-manifest read model and local
// evidence. It does not own bytes: CacheResolver remains the single download,
// SHA verification, and cache-hit implementation.
type FinalManifestReconciler struct {
	mu       sync.Mutex
	manifest assembly.FinalAssemblyManifest
	resolver FinalArtifactResolver
	now      func() time.Time
	evidence map[string]FinalArtifactEvidence
}

func NewFinalManifestReconciler(manifest assembly.FinalAssemblyManifest, resolver FinalArtifactResolver, now func() time.Time) (*FinalManifestReconciler, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if resolver == nil {
		return nil, fmt.Errorf("prefetch: final manifest resolver is required")
	}
	if now == nil {
		now = time.Now
	}
	return &FinalManifestReconciler{
		manifest: manifest,
		resolver: resolver,
		now:      now,
		evidence: make(map[string]FinalArtifactEvidence, len(manifest.Artifacts)),
	}, nil
}

// ApplyDelta validates and atomically applies a revisioned update. Resolution
// happens before committing the new read model, so a failed artifact never
// makes the manifest appear complete. A retry is safe: verified bytes are
// found by SHA-256 in the canonical cache and are not downloaded again.
func (r *FinalManifestReconciler) ApplyDelta(ctx context.Context, delta assembly.FinalManifestDelta) (FinalManifestResult, error) {
	if r == nil {
		return FinalManifestResult{}, fmt.Errorf("prefetch: nil final manifest reconciler")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	current := r.manifest
	candidate, disposition, err := current.ApplyDelta(delta)
	if err != nil {
		r.mu.Unlock()
		return FinalManifestResult{}, err
	}

	// Only artifacts whose verified evidence is absent or whose content
	// identity changed need resolution. Existing source clips and previous
	// Chronon outputs are deliberately not touched by a delta that does not
	// mention them.
	toResolve := make([]assembly.PublishedArtifact, 0, len(delta.UpsertedArtifacts))
	for _, artifact := range delta.UpsertedArtifacts {
		if evidence, ok := r.evidence[artifact.AssetID]; ok && sameArtifactIdentity(evidence.Artifact, artifact) {
			continue
		}
		toResolve = append(toResolve, artifact)
	}
	resolver := r.resolver
	r.mu.Unlock()

	resolved := make([]FinalArtifactEvidence, 0, len(toResolve))
	for _, artifact := range toResolve {
		asset, err := resolver.Resolve(ctx, downloader.DownloadRequest{
			JobID:     artifact.JobID,
			AssetKey:  assetref.AssetKey(artifact.AssetID),
			AssetID:   artifact.AssetID,
			Source:    "final_manifest_artifact",
			SHA256:    assetref.ContentHash(artifact.SHA256),
			SizeBytes: artifact.SizeBytes,
			MIMEType:  artifact.MIMEType,
			Priority:  downloader.PriorityForeground,
		})
		if err != nil {
			return FinalManifestResult{}, fmt.Errorf("prefetch: resolve final artifact %q: %w", artifact.AssetID, err)
		}
		sizeMatches := asset.DownloadBytes == artifact.SizeBytes || (asset.CacheHit && asset.DownloadBytes == 0)
		if asset.LocalPath == "" || asset.SHA256 != assetref.ContentHash(artifact.SHA256) || !sizeMatches {
			return FinalManifestResult{}, fmt.Errorf("prefetch: resolver returned unverifiable final artifact %q", artifact.AssetID)
		}
		resolved = append(resolved, FinalArtifactEvidence{
			Artifact: artifact, LocalPath: asset.LocalPath, CacheHit: asset.CacheHit, PreparedAt: r.now().UTC(),
		})
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Another delta may have committed while this one was resolving. Never
	// overwrite it with a stale revision; the caller must replay/rebase.
	if r.manifest.Revision != current.Revision {
		return FinalManifestResult{}, fmt.Errorf("prefetch: final manifest changed while resolving revision %d", delta.Revision)
	}
	if disposition == assembly.RevisionApplied {
		r.manifest = candidate
	}
	for _, evidence := range resolved {
		r.evidence[evidence.Artifact.AssetID] = evidence
	}
	for _, id := range delta.InvalidatedArtifactIDs {
		delete(r.evidence, id)
	}
	result := r.resultLocked(disposition, resolved)
	return result, nil
}

func sameArtifactIdentity(a, b assembly.PublishedArtifact) bool {
	return a.JobID == b.JobID && a.AssetID == b.AssetID && a.SHA256 == b.SHA256 && a.SizeBytes == b.SizeBytes && a.TimelineRevision == b.TimelineRevision
}

func (r *FinalManifestReconciler) resultLocked(disposition assembly.RevisionDisposition, resolved []FinalArtifactEvidence) FinalManifestResult {
	ready := len(r.manifest.Artifacts) > 0 && len(r.evidence) == len(r.manifest.Artifacts)
	if ready {
		for _, artifact := range r.manifest.Artifacts {
			if !sameArtifactIdentity(r.evidence[artifact.AssetID].Artifact, artifact) {
				ready = false
				break
			}
		}
	}
	state := FinalManifestPreparing
	if len(r.evidence) > 0 {
		state = FinalManifestPrepared
	}
	if ready {
		state = FinalManifestReady
	}
	prepared := make([]FinalArtifactEvidence, 0, len(r.evidence))
	for _, artifact := range r.manifest.Artifacts {
		if evidence, ok := r.evidence[artifact.AssetID]; ok {
			prepared = append(prepared, evidence)
		}
	}
	return FinalManifestResult{Manifest: r.manifest, Disposition: disposition, State: state, Ready: ready, ResolvedArtifacts: append([]FinalArtifactEvidence(nil), resolved...), PreparedArtifacts: prepared}
}

// Snapshot returns the current final-manifest read model and a stable copy of
// its local evidence. It is safe to call from heartbeat/status publishers.
func (r *FinalManifestReconciler) Snapshot() FinalManifestResult {
	if r == nil {
		return FinalManifestResult{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	resolved := make([]FinalArtifactEvidence, 0, len(r.evidence))
	for _, evidence := range r.evidence {
		resolved = append(resolved, evidence)
	}
	return r.resultLocked("", resolved)
}
