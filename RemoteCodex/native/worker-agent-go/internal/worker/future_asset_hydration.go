package worker

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"velox-shared/assetref"
	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
)

// hydrateDeferredFutureAssetPlan is the worker-side source resolver. The
// Master sends only a Drive locator and the trusted byte size; this method
// downloads the source directly on the selected worker, verifies the bytes,
// and turns the result into the strict content-addressed manifest consumed by
// the normal FutureAsset scheduler.
func (w *Worker) hydrateDeferredFutureAssetPlan(ctx context.Context, plan futureasset.Plan) (futureasset.Plan, error) {
	if len(plan.DeferredJobs) == 0 {
		return plan, nil
	}
	resolver := w.assetCacheResolver()
	if resolver == nil {
		return futureasset.Plan{}, fmt.Errorf("future asset hydration: cache resolver unavailable")
	}
	hydrated := plan
	hydrated.PrefetchJobs = append([]futureasset.Job(nil), plan.PrefetchJobs...)
	hydrated.DeferredJobs = nil
	contentKeys := make(map[string]string)
	for _, sourceJob := range plan.DeferredJobs {
		job := sourceJob
		job.Assets = make([]futureasset.AssetManifest, 0, len(sourceJob.Assets))
		for _, source := range sourceJob.Assets {
			w.rememberSourceLocator(source.AssetID, source.SourceURI)
			resolved, err := resolver.Resolve(ctx, downloader.DownloadRequest{
				JobID: sourceJob.JobID, TaskID: sourceJob.TaskID, WorkerID: w.config.WorkerID,
				AssetKey: assetref.AssetKey(source.AssetKey), AssetID: source.AssetID,
				Role: downloader.RoleFromString(source.Role), Source: "worker_direct_source",
				SourceURI: source.SourceURI, SizeBytes: source.SizeBytes, MIMEType: source.MIMEType,
				Priority: downloader.DefaultPriority,
			})
			if err != nil {
				return futureasset.Plan{}, fmt.Errorf("hydrate job %s asset %s: %w", sourceJob.JobID, source.AssetID, err)
			}
			sha := strings.TrimSpace(string(resolved.SHA256))
			if sha == "" || resolved.SizeBytes <= 0 {
				return futureasset.Plan{}, fmt.Errorf("hydrate job %s asset %s returned incomplete identity", sourceJob.JobID, source.AssetID)
			}
			canonicalKey := "sha256:" + strings.TrimPrefix(strings.ToLower(sha), "sha256:")
			contentKeys[source.AssetKey] = canonicalKey
			job.Assets = append(job.Assets, futureasset.AssetManifest{
				AssetKey: canonicalKey, AssetID: source.AssetID, SHA256: sha,
				SizeBytes: resolved.SizeBytes, MIMEType: source.MIMEType, Role: source.Role, SourceURI: source.SourceURI,
			})
		}
		hydrated.PrefetchJobs = append(hydrated.PrefetchJobs, job)
	}
	protected := make(map[string]futureasset.ProtectedAsset, len(plan.Protect))
	for _, asset := range plan.Protect {
		key := asset.AssetKey
		if canonical, ok := contentKeys[key]; ok {
			key = canonical
		}
		current := protected[key]
		current.AssetKey = key
		current.FutureRefCount += asset.FutureRefCount
		if current.NextUseDistance == 0 || asset.NextUseDistance < current.NextUseDistance {
			current.NextUseDistance = asset.NextUseDistance
		}
		protected[key] = current
	}
	hydrated.Protect = make([]futureasset.ProtectedAsset, 0, len(protected))
	for _, asset := range protected {
		hydrated.Protect = append(hydrated.Protect, asset)
	}
	sort.Slice(hydrated.PrefetchJobs, func(i, j int) bool { return hydrated.PrefetchJobs[i].Distance < hydrated.PrefetchJobs[j].Distance })
	sort.Slice(hydrated.Protect, func(i, j int) bool { return hydrated.Protect[i].AssetKey < hydrated.Protect[j].AssetKey })
	if err := hydrated.Validate(); err != nil {
		return futureasset.Plan{}, fmt.Errorf("hydrated future asset plan invalid: %w", err)
	}
	return hydrated, nil
}
