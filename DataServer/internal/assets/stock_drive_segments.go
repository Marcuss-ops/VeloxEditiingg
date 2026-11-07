package assets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"

	"velox-shared/contract"
)

// Keep Drive reads wide enough to overlap network latency, but bounded so a
// large stock plan cannot exhaust Drive quotas, file descriptors, or disk I/O.
const stockSourceDownloadConcurrency = 8

// Source preparation uses the same eight-way bound as Drive downloads. This
// keeps independent window encodes overlapping without launching an unbounded
// number of CPU encoders on one Master.
const stockWindowMaterializationConcurrency = 4

// PrepareDriveStockWindows downloads each Drive source once, verifies and
// probes it once, then replaces every source window with a certified,
// assembly-ready fragment in the canonical plan. Prepared fragments are
// content-addressed and cached by source identity, window and output profile.
func (s *AssetService) PrepareDriveStockWindows(ctx context.Context, planJSON string, runtimeAssets []map[string]interface{}) (string, string, error) {
	if s == nil || s.repo == nil || s.blobStore == nil || s.videoTrimmer == nil {
		return "", "", fmt.Errorf("stock window preparation capability is unavailable")
	}
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return "", "", fmt.Errorf("decode compiled plan: %w", err)
	}
	if plan.StockPool != nil {
		return planJSON, contract.HashCompiledPlanV2([]byte(planJSON)), nil
	}
	bindings := stockRuntimeBindings(runtimeAssets)
	assetsByID := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assetsByID[asset.AssetID] = asset
	}
	sourceRequests, err := collectDriveStockSourceRequests(&plan, assetsByID, bindings)
	if err != nil {
		return "", "", err
	}
	sources, err := s.materializeDriveStockSources(ctx, sourceRequests)
	if err != nil {
		return "", "", err
	}
	defer cleanupDriveStockSources(sources)

	windowJobs, placements, replacedStockIDs, changed, err := collectDriveStockWindows(&plan, assetsByID, bindings, sources)
	if err != nil {
		return "", "", err
	}
	if !changed {
		return planJSON, contract.HashCompiledPlanV2([]byte(planJSON)), nil
	}
	windowResults, windowKeys, err := s.materializeDriveStockWindows(ctx, &plan, windowJobs)
	if err != nil {
		return "", "", err
	}
	if err := s.registerPreparedDriveStockWindows(ctx, windowKeys, windowResults); err != nil {
		return "", "", err
	}
	return rewritePreparedDriveStockPlan(&plan, placements, replacedStockIDs, windowResults)
}

func matchesCanonicalStockVideo(probe VideoProbe, profile contract.CanonicalVideoProfileV1) bool {
	if probe.Width != profile.Width || probe.Height != profile.Height || probe.FPSNum != profile.FPSNum || probe.FPSDen != profile.FPSDen ||
		probe.VideoCodec != strings.ToLower(profile.Codec) || probe.PixelFormat != strings.ToLower(profile.PixelFormat) ||
		probe.VideoProfile != strings.ToLower(profile.CodecProfile) || probe.TimebaseNum != profile.TimeBaseNum || probe.TimebaseDen != profile.TimeBaseDen {
		return false
	}
	if profile.CodecLevel != "" {
		var level float64
		if _, err := fmt.Sscanf(profile.CodecLevel, "%f", &level); err == nil && probe.VideoLevel != int(math.Round(level*10)) {
			return false
		}
	}
	return canonicalGOPAligned(probe.Keyframes, profile)
}

func runBoundedStockJobs(ctx context.Context, limit int, jobs []func(context.Context) error) error {
	if len(jobs) == 0 {
		return nil
	}
	if limit < 1 {
		limit = 1
	}
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobQueue := make(chan func(context.Context) error)
	var group sync.WaitGroup
	var errorMu sync.Mutex
	var firstErr error
	workerCount := limit
	if workerCount > len(jobs) {
		workerCount = len(jobs)
	}
	group.Add(workerCount)
	for range workerCount {
		go func() {
			defer group.Done()
			for job := range jobQueue {
				if jobCtx.Err() != nil {
					return
				}
				if err := job(jobCtx); err != nil {
					errorMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errorMu.Unlock()
					return
				}
			}
		}()
	}

dispatch:
	for _, job := range jobs {
		select {
		case jobQueue <- job:
		case <-jobCtx.Done():
			break dispatch
		}
	}
	close(jobQueue)
	group.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func isDriveStock(asset contract.AssetRefV2) bool {
	return strings.EqualFold(strings.TrimSpace(asset.Kind), "video") &&
		(strings.EqualFold(strings.TrimSpace(asset.SourceKind), "drive") || strings.TrimSpace(asset.DriveFileID) != "" || strings.HasPrefix(strings.ToLower(asset.AssetID), "drive:"))
}

func stockRuntimeBindings(assets []map[string]interface{}) map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{}, len(assets)*2)
	for _, asset := range assets {
		for _, key := range []string{"drive_file_id", "asset_id", "asset_key", "sha256", "asset_sha256"} {
			id := strings.TrimSpace(fmt.Sprint(asset[key]))
			if id != "" && id != "<nil>" {
				out[id] = asset
			}
		}
	}
	return out
}

func stockRuntimeBindingForAsset(asset contract.AssetRefV2, bindings map[string]map[string]interface{}) map[string]interface{} {
	for _, key := range []string{asset.DriveFileID, asset.AssetID, asset.AssetKey, asset.SHA256} {
		if binding := bindings[strings.TrimSpace(key)]; binding != nil {
			return binding
		}
	}
	return nil
}

func isDriveRuntimeBinding(binding map[string]interface{}) bool {
	if binding == nil {
		return false
	}
	if strings.TrimSpace(bindingString(binding, "drive_file_id")) != "" || strings.EqualFold(strings.TrimSpace(bindingString(binding, "source_kind")), "drive") {
		return true
	}
	for _, key := range []string{"source_uri", "url", "drive_link"} {
		value := strings.ToLower(strings.TrimSpace(bindingString(binding, key)))
		if strings.Contains(value, "drive.google.com/") || strings.Contains(value, "drive.usercontent.google.com/") || strings.HasPrefix(value, "velox-drive://") {
			return true
		}
	}
	return false
}

// PlanHasDriveStock reports whether a compiled plan has any source-backed video
// segment that the Master must materialize. The historical name is retained at
// the API boundary; the classifier now covers Drive stock, clips and overlays.
func PlanHasDriveStock(planJSON string, runtimeAssets []map[string]interface{}) bool {
	var plan contract.CompiledRenderPlanV2
	if strings.TrimSpace(planJSON) == "" || json.Unmarshal([]byte(planJSON), &plan) != nil || plan.StockPool != nil {
		return false
	}
	bindings := stockRuntimeBindings(runtimeAssets)
	assetsByID := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assetsByID[asset.AssetID] = asset
	}
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if segment.StockPoolSlot {
				continue
			}
			asset, ok := assetsByID[segment.AssetID]
			if !ok {
				continue
			}
			binding := stockRuntimeBindingForAsset(asset, bindings)
			if isDriveStock(asset) || isDriveRuntimeBinding(binding) || stockSourceURI(binding, "") != "" {
				return true
			}
		}
	}
	return false
}

// RetainPlanSourceBindings keeps only runtime sources still named by the
// rewritten plan. Materialized source windows now resolve from the Master asset
// registry; unchanged, already-certified whole assets remain available to the
// worker without a second fetch entry.
func RetainPlanSourceBindings(planJSON string, assets []map[string]interface{}) ([]map[string]interface{}, error) {
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return nil, fmt.Errorf("decode prepared plan while pruning source bindings: %w", err)
	}
	ids := make(map[string]struct{}, len(plan.Assets)*4)
	for _, asset := range plan.Assets {
		for _, id := range []string{asset.AssetID, asset.AssetKey, asset.SHA256, asset.DriveFileID} {
			if id = strings.TrimSpace(id); id != "" {
				ids[id] = struct{}{}
			}
		}
	}
	filtered := make([]map[string]interface{}, 0, len(assets))
	for _, asset := range assets {
		keep := false
		for _, key := range []string{"asset_id", "asset_key", "sha256", "asset_sha256", "drive_file_id"} {
			if id := strings.TrimSpace(bindingString(asset, key)); id != "" {
				if _, ok := ids[id]; ok {
					keep = true
					break
				}
			}
		}
		if keep {
			filtered = append(filtered, asset)
		}
	}
	return filtered, nil
}

func bindingString(binding map[string]interface{}, keys ...string) string {
	if binding == nil {
		return ""
	}
	for _, key := range keys {
		if value, ok := binding[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func bindingInt64(binding map[string]interface{}, key string) int64 {
	if binding == nil {
		return 0
	}
	switch value := binding[key].(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case json.Number:
		parsed, _ := value.Int64()
		return parsed
	case string:
		var parsed int64
		_, _ = fmt.Sscan(strings.TrimSpace(value), &parsed)
		return parsed
	default:
		return 0
	}
}

func stockSourceURI(binding map[string]interface{}, driveID string) string {
	for _, key := range []string{"source_uri", "url", "drive_link"} {
		if value, ok := binding[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	uri, _ := DeferredDriveReference(driveID)
	return uri
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
