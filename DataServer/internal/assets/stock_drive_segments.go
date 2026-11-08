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

// Shared resource-class gates keep parallel FINALIZE requests from
// multiplying these per-request limits into unbounded fleet-control-plane
// work. Download/probe and window encoding have separate budgets.
var (
	stockSourceSlots = make(chan struct{}, stockSourceDownloadConcurrency)
	stockWindowSlots = make(chan struct{}, stockWindowMaterializationConcurrency)
)

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
	cachedWindows, sourceIDsToFetch, err := s.lookupCachedDriveStockWindows(ctx, &plan, assetsByID, bindings)
	if err != nil {
		return "", "", err
	}
	requestsToFetch := make(map[string]driveStockSourceRequest, len(sourceIDsToFetch))
	for sourceID := range sourceIDsToFetch {
		if request, exists := sourceRequests[sourceID]; exists {
			requestsToFetch[sourceID] = request
		}
	}
	sources, err := s.materializeDriveStockSources(ctx, requestsToFetch)
	if err != nil {
		return "", "", err
	}
	defer cleanupDriveStockSources(sources)

	windowJobs, placements, replacedStockIDs, changed, err := collectDriveStockWindows(&plan, assetsByID, bindings, sources, cachedWindows, s.videoTrimmer.PreparationRevision())
	if err != nil {
		return "", "", err
	}
	if !changed {
		return planJSON, contract.HashCompiledPlanV2([]byte(planJSON)), nil
	}
	windowResults := cachedWindows
	materializedResults, windowKeys, err := s.materializeDriveStockWindows(ctx, &plan, windowJobs)
	if err != nil {
		return "", "", err
	}
	for key, result := range materializedResults {
		windowResults[key] = result
	}
	if err := s.registerPreparedDriveStockWindows(ctx, windowKeys, windowResults); err != nil {
		return "", "", err
	}
	return rewritePreparedDriveStockPlan(&plan, placements, replacedStockIDs, windowResults)
}

// lookupCachedDriveStockWindows checks every window with a known immutable
// source identity before downloading source bytes. It returns only the source
// IDs that have an unknown identity or at least one cache miss.
func (s *AssetService) lookupCachedDriveStockWindows(
	ctx context.Context,
	plan *contract.CompiledRenderPlanV2,
	assetsByID map[string]contract.AssetRefV2,
	bindings map[string]map[string]interface{},
) (map[string]*driveStockWindowResult, map[string]struct{}, error) {
	windowResults := make(map[string]*driveStockWindowResult)
	sourceIDsToFetch := make(map[string]struct{})
	seenWindows := make(map[string]struct{})
	for trackIndex := range plan.VideoTracks {
		for segmentIndex := range plan.VideoTracks[trackIndex].Segments {
			segment := &plan.VideoTracks[trackIndex].Segments[segmentIndex]
			if segment.StockPoolSlot {
				continue
			}
			original, exists := assetsByID[segment.AssetID]
			if !exists || videoSegmentAlreadyPrepared(*plan, *segment, original) {
				continue
			}
			binding := stockRuntimeBindingForAsset(original, bindings)
			if binding == nil && !isDriveStock(original) {
				return nil, nil, fmt.Errorf("video segment %q has no runtime source binding for asset %q", segment.SegmentID, original.AssetID)
			}
			sourceID := stockDriveID(original, binding)
			if sourceID == "" {
				sourceID = original.AssetID
			}
			if binding == nil || stockSourceURI(binding, sourceID) == "" {
				sourceIDsToFetch[sourceID] = struct{}{}
				continue
			}
			sourceSHA := strings.TrimSpace(original.SHA256)
			if sourceSHA == "" {
				sourceSHA = strings.TrimSpace(bindingString(binding, "sha256", "asset_sha256"))
			}
			sourceSHA = strings.ToLower(sourceSHA)
			decodedSHA, decodeErr := hex.DecodeString(sourceSHA)
			if decodeErr != nil || len(decodedSHA) != sha256.Size {
				sourceIDsToFetch[sourceID] = struct{}{}
				continue
			}
			startUS, durationUS := segment.SourceInUS, segment.SourceDurationUS
			if startUS < 0 || durationUS <= 0 || durationUS > math.MaxInt64-startUS {
				return nil, nil, fmt.Errorf("stock segment %q has invalid source window: source_in_us=%d source_duration_us=%d", segment.SegmentID, startUS, durationUS)
			}
			wantFrames := int64(math.Round(float64(durationUS) * float64(plan.Output.FPSNum) / (1_000_000 * float64(plan.Output.FPSDen))))
			if wantFrames <= 0 || segment.FrameCount != wantFrames {
				return nil, nil, fmt.Errorf("video segment %q frame count mismatch: plan=%d expected=%d from %d us at %d/%d fps", segment.SegmentID, segment.FrameCount, wantFrames, durationUS, plan.Output.FPSNum, plan.Output.FPSDen)
			}
			windowRef := stockWindowSourceReference(sourceID, sourceSHA, startUS, durationUS, plan.Output.ProfileID, s.videoTrimmer.PreparationRevision())
			if _, exists := seenWindows[windowRef]; exists {
				continue
			}
			seenWindows[windowRef] = struct{}{}
			cached, err := s.GetBySourceReference(ctx, windowRef)
			if err != nil {
				return nil, nil, fmt.Errorf("stock window cache lookup: %w", err)
			}
			if cached == nil {
				sourceIDsToFetch[sourceID] = struct{}{}
				continue
			}
			if cached.SHA256 == "" || cached.SizeBytes <= 0 || cached.Status != AssetStatusReady {
				return nil, nil, fmt.Errorf("cached stock window %q is not a certified READY asset", windowRef)
			}
			if !cachedDriveStockWindowEvidence(cached, sourceSHA, startUS, durationUS, wantFrames, s.videoTrimmer.Profile(), s.videoTrimmer.PreparationRevision()) {
				return nil, nil, fmt.Errorf("cached stock window %q has missing or mismatched preparation evidence", windowRef)
			}
			windowResults[windowRef] = &driveStockWindowResult{fragment: cached}
		}
	}
	return windowResults, sourceIDsToFetch, nil
}

func stockWindowSourceReference(sourceID, sourceSHA string, startUS, durationUS int64, profileID, preparationRevision string) string {
	return fmt.Sprintf("velox-video-window://%s/%s/%d/%d/%s/%s", sourceID, sourceSHA, startUS, durationUS, profileID, preparationRevision)
}

func cachedDriveStockWindowEvidence(
	asset *Asset,
	sourceSHA string,
	startUS, durationUS, expectedFrames int64,
	profile contract.CanonicalVideoProfileV1,
	preparationRevision string,
) bool {
	if asset == nil || asset.MetadataJSON == "" || preparationRevision == "" {
		return false
	}
	var evidence struct {
		StartSeconds        float64 `json:"start_seconds"`
		EndSeconds          float64 `json:"end_seconds"`
		ProfileID           string  `json:"canonical_profile_id"`
		StreamProfileID     string  `json:"canonical_stream_profile_id"`
		ProfileVersion      int     `json:"canonical_profile_version"`
		PreparationRevision string  `json:"preparation_revision"`
		SourceSHA256        string  `json:"source_sha256"`
		FrameCount          int64   `json:"frame_count"`
		FirstFrameKeyframe  bool    `json:"first_frame_keyframe"`
		ClosedGOP           bool    `json:"closed_gop"`
		CopyEligible        bool    `json:"copy_eligible"`
	}
	if err := json.Unmarshal([]byte(asset.MetadataJSON), &evidence); err != nil {
		return false
	}
	wantStart := float64(startUS) / 1_000_000
	wantEnd := float64(startUS+durationUS) / 1_000_000
	const tolerance = 0.000001
	return strings.EqualFold(evidence.SourceSHA256, sourceSHA) &&
		evidence.PreparationRevision == preparationRevision &&
		evidence.ProfileID == profile.ProfileID && evidence.StreamProfileID == profile.StreamProfile() &&
		evidence.ProfileVersion == profile.Version && evidence.FrameCount == expectedFrames &&
		evidence.FirstFrameKeyframe && evidence.ClosedGOP == profile.ClosedGOP && evidence.CopyEligible &&
		math.Abs(evidence.StartSeconds-wantStart) <= tolerance && math.Abs(evidence.EndSeconds-wantEnd) <= tolerance
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
	return runBoundedStockJobsWithSlots(ctx, limit, nil, jobs)
}

func runBoundedStockJobsWithSlots(ctx context.Context, limit int, sharedSlots chan struct{}, jobs []func(context.Context) error) error {
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
				if sharedSlots != nil {
					select {
					case sharedSlots <- struct{}{}:
					case <-jobCtx.Done():
						return
					}
				}
				if err := job(jobCtx); err != nil {
					if sharedSlots != nil {
						<-sharedSlots
					}
					errorMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errorMu.Unlock()
					return
				}
				if sharedSlots != nil {
					<-sharedSlots
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
