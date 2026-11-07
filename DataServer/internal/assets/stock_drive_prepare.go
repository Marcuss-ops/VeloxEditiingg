package assets

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"velox-shared/contract"
)

type driveStockSourceState struct {
	path    string
	cleanup func()
	probe   VideoProbe
	sha     string
	size    int64
}

type driveStockSourceRequest struct {
	driveID string
	uri     string
}

type driveStockWindowJob struct {
	ref            string
	segmentID      string
	state          *driveStockSourceState
	startUS        int64
	durationUS     int64
	fileDurationUS int64
	wantFrames     int64
}

type driveStockPlacement struct {
	trackIndex   int
	segmentIndex int
	windowRef    string
	wantFrames   int64
}

type driveStockWindowResult struct {
	fragment   *Asset
	outputPath string
	trimPlan   TrimPlan
}

func collectDriveStockSourceRequests(
	plan *contract.CompiledRenderPlanV2,
	assetsByID map[string]contract.AssetRefV2,
	bindings map[string]map[string]interface{},
) (map[string]driveStockSourceRequest, error) {
	requests := make(map[string]driveStockSourceRequest)
	for trackIndex := range plan.VideoTracks {
		for segmentIndex := range plan.VideoTracks[trackIndex].Segments {
			segment := plan.VideoTracks[trackIndex].Segments[segmentIndex]
			if segment.StockPoolSlot {
				continue
			}
			original, ok := assetsByID[segment.AssetID]
			if !ok {
				continue
			}
			binding := stockRuntimeBindingForAsset(original, bindings)
			if !isDriveStock(original) && !isDriveRuntimeBinding(binding) {
				continue
			}
			driveID := stockDriveID(original, binding)
			if binding == nil {
				return nil, fmt.Errorf("stock segment %q has no runtime Drive binding for file %q", segment.SegmentID, driveID)
			}
			if driveID == "" {
				return nil, fmt.Errorf("stock segment %q Drive binding does not declare drive_file_id", segment.SegmentID)
			}
			uri := stockSourceURI(binding, driveID)
			if uri == "" {
				return nil, fmt.Errorf("stock segment %q has no fetchable Drive source", segment.SegmentID)
			}
			if previous, exists := requests[driveID]; exists && previous.uri != uri {
				return nil, fmt.Errorf("Drive file %q has conflicting source references in one plan", driveID)
			}
			requests[driveID] = driveStockSourceRequest{driveID: driveID, uri: uri}
		}
	}
	return requests, nil
}

func stockDriveID(asset contract.AssetRefV2, binding map[string]interface{}) string {
	driveID := strings.TrimSpace(asset.DriveFileID)
	if driveID == "" {
		driveID = strings.TrimPrefix(strings.TrimSpace(asset.AssetID), "drive:")
	}
	if driveID == "" || driveID == asset.AssetID {
		driveID = strings.TrimSpace(bindingString(binding, "drive_file_id"))
	}
	return driveID
}

func (s *AssetService) materializeDriveStockSources(
	ctx context.Context,
	requests map[string]driveStockSourceRequest,
) (map[string]*driveStockSourceState, error) {
	sources := make(map[string]*driveStockSourceState, len(requests))
	if len(requests) == 0 {
		return sources, nil
	}
	jobs := make([]func(context.Context) error, 0, len(requests))
	var sourcesMu sync.Mutex
	for _, request := range requests {
		request := request
		jobs = append(jobs, func(sourceCtx context.Context) error {
			path, cleanup, err := s.materializeVideoSource(sourceCtx, request.uri)
			if err != nil {
				return fmt.Errorf("stock file %q download: %w", request.driveID, err)
			}
			state := &driveStockSourceState{path: path, cleanup: cleanup}
			state.sha, state.size, err = hashFile(path)
			if err != nil {
				cleanup()
				return fmt.Errorf("stock file %q identity: %w", request.driveID, err)
			}
			state.probe, err = s.videoTrimmer.Probe(sourceCtx, path)
			if err != nil {
				cleanup()
				return fmt.Errorf("stock file %q certification: %w", request.driveID, err)
			}
			if state.probe.FrameCount <= 0 && state.probe.FPSNum > 0 && state.probe.FPSDen > 0 {
				// Some MP4 muxers omit nb_frames. For certified constant-frame-rate
				// stock sources, derive it from the measured source duration.
				state.probe.FrameCount = int64(math.Round(state.probe.DurationSeconds * float64(state.probe.FPSNum) / float64(state.probe.FPSDen)))
			}
			if state.probe.FrameCount <= 0 {
				cleanup()
				return fmt.Errorf("stock file %q certification could not derive a positive frame_count", request.driveID)
			}
			sourcesMu.Lock()
			sources[request.driveID] = state
			sourcesMu.Unlock()
			return nil
		})
	}
	if err := runBoundedStockJobs(ctx, stockSourceDownloadConcurrency, jobs); err != nil {
		cleanupDriveStockSources(sources)
		return nil, err
	}
	return sources, nil
}

func cleanupDriveStockSources(sources map[string]*driveStockSourceState) {
	for _, source := range sources {
		if source != nil && source.cleanup != nil {
			source.cleanup()
		}
	}
}

func collectDriveStockWindows(
	plan *contract.CompiledRenderPlanV2,
	assetsByID map[string]contract.AssetRefV2,
	bindings map[string]map[string]interface{},
	sources map[string]*driveStockSourceState,
) (map[string]driveStockWindowJob, []driveStockPlacement, map[string]struct{}, bool, error) {
	windowJobs := make(map[string]driveStockWindowJob)
	placements := make([]driveStockPlacement, 0)
	replacedStockIDs := make(map[string]struct{})
	changed := false
	for trackIndex := range plan.VideoTracks {
		for segmentIndex := range plan.VideoTracks[trackIndex].Segments {
			segment := &plan.VideoTracks[trackIndex].Segments[segmentIndex]
			if segment.StockPoolSlot {
				continue
			}
			original, ok := assetsByID[segment.AssetID]
			if !ok {
				continue
			}
			binding := stockRuntimeBindingForAsset(original, bindings)
			if !isDriveStock(original) && !isDriveRuntimeBinding(binding) {
				continue
			}
			changed = true
			replacedStockIDs[original.AssetID] = struct{}{}
			driveID := stockDriveID(original, binding)
			if binding == nil {
				return nil, nil, nil, false, fmt.Errorf("stock segment %q has no runtime Drive binding for file %q", segment.SegmentID, driveID)
			}
			if driveID == "" {
				return nil, nil, nil, false, fmt.Errorf("stock segment %q Drive binding does not declare drive_file_id", segment.SegmentID)
			}
			state := sources[driveID]
			if state == nil {
				return nil, nil, nil, false, fmt.Errorf("stock file %q was not prefetched", driveID)
			}
			if err := validateDriveStockIdentity(original, binding, state, driveID); err != nil {
				return nil, nil, nil, false, err
			}
			startUS, durationUS := segment.SourceInUS, segment.SourceDurationUS
			if startUS < 0 || durationUS <= 0 || durationUS > math.MaxInt64-startUS {
				return nil, nil, nil, false, fmt.Errorf("stock segment %q has invalid source window: source_in_us=%d source_duration_us=%d", segment.SegmentID, startUS, durationUS)
			}
			frameUS := int64(math.Round(1_000_000 * float64(state.probe.FPSDen) / float64(state.probe.FPSNum)))
			fileDurationUS := int64(math.Round(state.probe.DurationSeconds * 1_000_000))
			if frameUS <= 0 || startUS >= fileDurationUS || startUS+durationUS > fileDurationUS+frameUS {
				return nil, nil, nil, false, fmt.Errorf("stock segment %q window exceeds certified source duration: end_us=%d file_duration_us=%d tolerance_us=%d", segment.SegmentID, startUS+durationUS, fileDurationUS, frameUS)
			}
			wantFrames := int64(math.Round(float64(durationUS) * float64(plan.Output.FPSNum) / (1_000_000 * float64(plan.Output.FPSDen))))
			if wantFrames <= 0 || segment.FrameCount != wantFrames {
				return nil, nil, nil, false, fmt.Errorf("stock segment %q frame count mismatch: plan=%d expected=%d from %d us at %d/%d fps", segment.SegmentID, segment.FrameCount, wantFrames, durationUS, plan.Output.FPSNum, plan.Output.FPSDen)
			}
			windowRef := fmt.Sprintf("velox-stock-window://%s/%s/%d/%d/%s", driveID, state.sha, startUS, durationUS, plan.Output.ProfileID)
			if _, exists := windowJobs[windowRef]; !exists {
				windowJobs[windowRef] = driveStockWindowJob{
					ref: windowRef, segmentID: segment.SegmentID, state: state,
					startUS: startUS, durationUS: durationUS,
					fileDurationUS: fileDurationUS, wantFrames: wantFrames,
				}
			}
			placements = append(placements, driveStockPlacement{
				trackIndex: trackIndex, segmentIndex: segmentIndex,
				windowRef: windowRef, wantFrames: wantFrames,
			})
		}
	}
	return windowJobs, placements, replacedStockIDs, changed, nil
}

func validateDriveStockIdentity(asset contract.AssetRefV2, binding map[string]interface{}, state *driveStockSourceState, driveID string) error {
	expectedSHA := strings.TrimSpace(asset.SHA256)
	if expectedSHA == "" {
		expectedSHA = strings.TrimSpace(bindingString(binding, "sha256", "asset_sha256"))
	}
	if expectedSHA != "" && !strings.EqualFold(expectedSHA, state.sha) {
		return fmt.Errorf("stock file %q sha256 mismatch", driveID)
	}
	expectedSize := asset.SizeBytes
	if expectedSize <= 0 {
		expectedSize = bindingInt64(binding, "size_bytes")
	}
	if expectedSize > 0 && expectedSize != state.size {
		return fmt.Errorf("stock file %q size mismatch: got %d want %d", driveID, state.size, expectedSize)
	}
	return nil
}

func (s *AssetService) materializeDriveStockWindows(
	ctx context.Context,
	plan *contract.CompiledRenderPlanV2,
	windowJobs map[string]driveStockWindowJob,
) (map[string]*driveStockWindowResult, []string, error) {
	keys := make([]string, 0, len(windowJobs))
	for key := range windowJobs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	results := make(map[string]*driveStockWindowResult, len(windowJobs))
	var resultsMu sync.Mutex
	jobs := make([]func(context.Context) error, 0, len(keys))
	for _, key := range keys {
		key := key
		request := windowJobs[key]
		jobs = append(jobs, func(taskCtx context.Context) error {
			result, err := s.materializeDriveStockWindow(taskCtx, plan, request)
			if err != nil {
				return err
			}
			resultsMu.Lock()
			results[key] = result
			resultsMu.Unlock()
			return nil
		})
	}
	if err := runBoundedStockJobs(ctx, stockWindowMaterializationConcurrency, jobs); err != nil {
		cleanupDriveStockWindowResults(results)
		return nil, nil, err
	}
	return results, keys, nil
}

func (s *AssetService) materializeDriveStockWindow(
	ctx context.Context,
	plan *contract.CompiledRenderPlanV2,
	request driveStockWindowJob,
) (*driveStockWindowResult, error) {
	result := &driveStockWindowResult{}
	cached, err := s.GetBySourceReference(ctx, request.ref)
	if err != nil {
		return nil, fmt.Errorf("stock window cache lookup: %w", err)
	}
	if cached != nil {
		result.fragment = cached
		return result, nil
	}
	out, err := os.CreateTemp(filepath.Dir(request.state.path), ".velox-stock-window-*.mp4")
	if err != nil {
		return nil, fmt.Errorf("create stock window output: %w", err)
	}
	result.outputPath = out.Name()
	if err := out.Close(); err != nil {
		_ = os.Remove(result.outputPath)
		return nil, err
	}
	start := float64(request.startUS) / 1_000_000
	end := float64(request.startUS+request.durationUS) / 1_000_000
	trimmed, err := s.videoTrimmer.TrimFramesWithProbe(ctx, request.state.probe, request.state.path, result.outputPath, VideoSegment{StartSeconds: start, EndSeconds: end}, request.wantFrames)
	if err != nil {
		_ = os.Remove(result.outputPath)
		return nil, fmt.Errorf("stock segment %q materialization: %w", request.segmentID, err)
	}
	cert, err := s.videoTrimmer.Probe(ctx, result.outputPath)
	if err != nil {
		_ = os.Remove(result.outputPath)
		return nil, fmt.Errorf("stock segment %q output certification: %w", request.segmentID, err)
	}
	cert.FrameCount, err = s.videoTrimmer.countFrames(ctx, result.outputPath)
	if err != nil {
		_ = os.Remove(result.outputPath)
		return nil, fmt.Errorf("stock segment %q output frame count: %w", request.segmentID, err)
	}
	if err := validateDriveStockWindowOutput(cert, request, plan, s.videoTrimmer.Profile()); err != nil {
		_ = os.Remove(result.outputPath)
		return nil, err
	}
	trimmed.Plan = certifyDriveStockTrimPlan(trimmed.Plan, cert, request, s.videoTrimmer.Profile())
	result.trimPlan = trimmed.Plan
	return result, nil
}

func validateDriveStockWindowOutput(
	probe VideoProbe,
	request driveStockWindowJob,
	plan *contract.CompiledRenderPlanV2,
	profile contract.CanonicalVideoProfileV1,
) error {
	if probe.FrameCount != request.wantFrames {
		return fmt.Errorf("stock segment %q output frame count mismatch: got %d want %d", request.segmentID, probe.FrameCount, request.wantFrames)
	}
	expectedDuration := float64(request.wantFrames) * float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum)
	frameDuration := float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum)
	if math.Abs(probe.DurationSeconds-expectedDuration) > frameDuration {
		return fmt.Errorf("stock segment %q output duration mismatch: got %.6f s want %.6f s", request.segmentID, probe.DurationSeconds, expectedDuration)
	}
	if len(probe.Keyframes) == 0 || math.Abs(probe.Keyframes[0]) > 0.0005 {
		return fmt.Errorf("stock segment %q output does not start with a keyframe", request.segmentID)
	}
	if !probe.StartTimeKnown || math.Abs(probe.StartTimeSeconds) > 0.0005 {
		return fmt.Errorf("stock segment %q output starts at %.6f s, want PTS 0", request.segmentID, probe.StartTimeSeconds)
	}
	if !matchesCanonicalStockVideo(probe, profile) {
		return fmt.Errorf("stock segment %q output does not match canonical assembly profile", request.segmentID)
	}
	return nil
}

func certifyDriveStockTrimPlan(
	trimPlan TrimPlan,
	probe VideoProbe,
	request driveStockWindowJob,
	profile contract.CanonicalVideoProfileV1,
) TrimPlan {
	trimPlan.OutputFrameCount = probe.FrameCount
	trimPlan.DurationSeconds = float64(probe.FrameCount) * float64(profile.FPSDen) / float64(profile.FPSNum)
	trimPlan.FirstFrameKeyframe = true
	trimPlan.ClosedGOP = profile.ClosedGOP
	trimPlan.CopyEligible = true
	trimPlan.SourceSHA256 = request.state.sha
	trimPlan.SourceSizeBytes = request.state.size
	trimPlan.SourceDurationUS = request.fileDurationUS
	trimPlan.SourceFrameCount = request.state.probe.FrameCount
	trimPlan.SourceFPSNum, trimPlan.SourceFPSDen = request.state.probe.FPSNum, request.state.probe.FPSDen
	trimPlan.SourceCodec = request.state.probe.VideoCodec
	trimPlan.SourceProfile = request.state.probe.VideoProfile
	trimPlan.SourceLevel = request.state.probe.VideoLevel
	trimPlan.SourceWidth, trimPlan.SourceHeight = request.state.probe.Width, request.state.probe.Height
	trimPlan.SourcePixelFormat = request.state.probe.PixelFormat
	trimPlan.SourceKeyframes = append([]float64(nil), request.state.probe.Keyframes...)
	return trimPlan
}

func (s *AssetService) registerPreparedDriveStockWindows(
	ctx context.Context,
	keys []string,
	results map[string]*driveStockWindowResult,
) error {
	for _, key := range keys {
		result := results[key]
		if result.outputPath == "" {
			continue
		}
		fragment, err := s.registerPreparedVideoFile(ctx, result.outputPath, key, result.trimPlan)
		_ = os.Remove(result.outputPath)
		if err != nil {
			cleanupDriveStockWindowResults(results)
			return fmt.Errorf("register prepared stock window: %w", err)
		}
		result.fragment = fragment
	}
	return nil
}

func cleanupDriveStockWindowResults(results map[string]*driveStockWindowResult) {
	for _, result := range results {
		if result != nil && result.outputPath != "" {
			_ = os.Remove(result.outputPath)
		}
	}
}

func rewritePreparedDriveStockPlan(
	plan *contract.CompiledRenderPlanV2,
	placements []driveStockPlacement,
	replacedStockIDs map[string]struct{},
	results map[string]*driveStockWindowResult,
) (string, string, error) {
	newAssets := make(map[string]contract.AssetRefV2)
	for _, placement := range placements {
		result := results[placement.windowRef]
		if result == nil || result.fragment == nil {
			return "", "", fmt.Errorf("stock window %q produced no certified fragment", placement.windowRef)
		}
		fragment := result.fragment
		assetRef := contract.AssetRefV2{
			AssetID: fragment.AssetID, AssetKey: fragment.AssetID, SHA256: fragment.SHA256,
			SizeBytes: fragment.SizeBytes, Kind: "video", MIME: fragment.MimeType,
			DurationUS: int64(math.Round(float64(placement.wantFrames) * 1_000_000 * float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum))),
			Width:      plan.Output.Width, Height: plan.Output.Height, ProfileID: plan.Output.ProfileID,
			FrameCount: placement.wantFrames, FirstFrameKeyframe: true, ClosedGOP: true,
		}
		newAssets[assetRef.AssetID] = assetRef
		segment := &plan.VideoTracks[placement.trackIndex].Segments[placement.segmentIndex]
		segment.AssetID, segment.SHA256 = assetRef.AssetID, assetRef.SHA256
		segment.SourceInUS = 0
		segment.SourceDurationUS = assetRef.DurationUS
	}
	for _, asset := range plan.Assets {
		_, replaced := replacedStockIDs[asset.AssetID]
		if !isDriveStock(asset) && !replaced {
			newAssets[asset.AssetID] = asset
		}
	}
	plan.Assets = make([]contract.AssetRefV2, 0, len(newAssets))
	for _, asset := range newAssets {
		plan.Assets = append(plan.Assets, asset)
	}
	if err := contract.ValidateCompiledRenderPlanV2(plan); err != nil {
		return "", "", fmt.Errorf("prepared compiled render plan is invalid: %w", err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return "", "", err
	}
	return string(canonical), contract.HashCompiledPlanV2(canonical), nil
}
