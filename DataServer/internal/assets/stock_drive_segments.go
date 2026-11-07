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
	"path/filepath"
	"sort"
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
	type sourceState struct {
		path    string
		cleanup func()
		probe   VideoProbe
		sha     string
		size    int64
	}
	sources := map[string]*sourceState{}
	defer func() {
		for _, source := range sources {
			source.cleanup()
		}
	}()
	type sourceRequest struct {
		driveID string
		uri     string
	}
	sourceRequests := map[string]sourceRequest{}
	// Collect only unique files actually referenced by the compiled timeline.
	// Never enumerate or download the whole stock folder here.
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
			driveID := strings.TrimSpace(original.DriveFileID)
			if driveID == "" {
				driveID = strings.TrimPrefix(strings.TrimSpace(original.AssetID), "drive:")
			}
			if driveID == "" || driveID == original.AssetID {
				driveID = strings.TrimSpace(bindingString(binding, "drive_file_id"))
			}
			if binding == nil {
				return "", "", fmt.Errorf("stock segment %q has no runtime Drive binding for file %q", segment.SegmentID, driveID)
			}
			if driveID == "" {
				return "", "", fmt.Errorf("stock segment %q Drive binding does not declare drive_file_id", segment.SegmentID)
			}
			uri := stockSourceURI(binding, driveID)
			if uri == "" {
				return "", "", fmt.Errorf("stock segment %q has no fetchable Drive source", segment.SegmentID)
			}
			if previous, exists := sourceRequests[driveID]; exists && previous.uri != uri {
				return "", "", fmt.Errorf("Drive file %q has conflicting source references in one plan", driveID)
			}
			sourceRequests[driveID] = sourceRequest{driveID: driveID, uri: uri}
		}
	}
	if len(sourceRequests) > 0 {
		sourceJobs := make([]func(context.Context) error, 0, len(sourceRequests))
		var sourcesMu sync.Mutex
		for _, request := range sourceRequests {
			request := request
			sourceJobs = append(sourceJobs, func(sourceCtx context.Context) error {
				path, cleanup, err := s.materializeVideoSource(sourceCtx, request.uri)
				if err != nil {
					return fmt.Errorf("stock file %q download: %w", request.driveID, err)
				}
				state := &sourceState{path: path, cleanup: cleanup}
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
					// Some MP4 muxers omit nb_frames. For the stock pool's certified
					// constant-frame-rate sources, derive it from measured file duration.
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
		if err := runBoundedStockJobs(ctx, stockSourceDownloadConcurrency, sourceJobs); err != nil {
			return "", "", err
		}
	}
	type windowJob struct {
		ref            string
		segmentID      string
		state          *sourceState
		startUS        int64
		durationUS     int64
		fileDurationUS int64
		wantFrames     int64
	}
	type placement struct {
		trackIndex   int
		segmentIndex int
		windowRef    string
		wantFrames   int64
	}
	windowJobs := map[string]windowJob{}
	placements := make([]placement, 0)
	newAssets := map[string]contract.AssetRefV2{}
	replacedStockIDs := map[string]struct{}{}
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
			driveID := strings.TrimSpace(original.DriveFileID)
			if driveID == "" {
				driveID = strings.TrimPrefix(strings.TrimSpace(original.AssetID), "drive:")
			}
			if driveID == "" || driveID == original.AssetID {
				driveID = strings.TrimSpace(bindingString(binding, "drive_file_id"))
			}
			if binding == nil {
				return "", "", fmt.Errorf("stock segment %q has no runtime Drive binding for file %q", segment.SegmentID, driveID)
			}
			if driveID == "" {
				return "", "", fmt.Errorf("stock segment %q Drive binding does not declare drive_file_id", segment.SegmentID)
			}
			uri := stockSourceURI(binding, driveID)
			if uri == "" {
				return "", "", fmt.Errorf("stock segment %q has no fetchable Drive source", segment.SegmentID)
			}
			state := sources[driveID]
			if state == nil {
				return "", "", fmt.Errorf("stock file %q was not prefetched", driveID)
			}
			expectedSHA := strings.TrimSpace(original.SHA256)
			if expectedSHA == "" {
				expectedSHA = strings.TrimSpace(bindingString(binding, "sha256", "asset_sha256"))
			}
			if expectedSHA != "" && !strings.EqualFold(expectedSHA, state.sha) {
				return "", "", fmt.Errorf("stock file %q sha256 mismatch", driveID)
			}
			expectedSize := original.SizeBytes
			if expectedSize <= 0 {
				expectedSize = bindingInt64(binding, "size_bytes")
			}
			if expectedSize > 0 && expectedSize != state.size {
				return "", "", fmt.Errorf("stock file %q size mismatch: got %d want %d", driveID, state.size, expectedSize)
			}
			startUS, durationUS := segment.SourceInUS, segment.SourceDurationUS
			if startUS < 0 || durationUS <= 0 || durationUS > math.MaxInt64-startUS {
				return "", "", fmt.Errorf("stock segment %q has invalid source window: source_in_us=%d source_duration_us=%d", segment.SegmentID, startUS, durationUS)
			}
			frameUS := int64(math.Round(1_000_000 * float64(state.probe.FPSDen) / float64(state.probe.FPSNum)))
			fileDurationUS := int64(math.Round(state.probe.DurationSeconds * 1_000_000))
			if frameUS <= 0 || startUS >= fileDurationUS || startUS+durationUS > fileDurationUS+frameUS {
				return "", "", fmt.Errorf("stock segment %q window exceeds certified source duration: end_us=%d file_duration_us=%d tolerance_us=%d", segment.SegmentID, startUS+durationUS, fileDurationUS, frameUS)
			}
			wantFrames := int64(math.Round(float64(durationUS) * float64(plan.Output.FPSNum) / (1_000_000 * float64(plan.Output.FPSDen))))
			if wantFrames <= 0 || segment.FrameCount != wantFrames {
				return "", "", fmt.Errorf("stock segment %q frame count mismatch: plan=%d expected=%d from %d us at %d/%d fps", segment.SegmentID, segment.FrameCount, wantFrames, durationUS, plan.Output.FPSNum, plan.Output.FPSDen)
			}
			windowRef := fmt.Sprintf("velox-stock-window://%s/%s/%d/%d/%s", driveID, state.sha, startUS, durationUS, plan.Output.ProfileID)
			if _, exists := windowJobs[windowRef]; !exists {
				windowJobs[windowRef] = windowJob{
					ref: windowRef, segmentID: segment.SegmentID,
					state: state, startUS: startUS, durationUS: durationUS,
					fileDurationUS: fileDurationUS, wantFrames: wantFrames,
				}
			}
			placements = append(placements, placement{trackIndex: trackIndex, segmentIndex: segmentIndex, windowRef: windowRef, wantFrames: wantFrames})
		}
	}
	if !changed {
		return planJSON, contract.HashCompiledPlanV2([]byte(planJSON)), nil
	}
	type windowResult struct {
		fragment   *Asset
		outputPath string
		trimPlan   TrimPlan
	}
	windowResults := make(map[string]*windowResult, len(windowJobs))
	var resultsMu sync.Mutex
	windowKeys := make([]string, 0, len(windowJobs))
	for key := range windowJobs {
		windowKeys = append(windowKeys, key)
	}
	sort.Strings(windowKeys)
	windowTasks := make([]func(context.Context) error, 0, len(windowKeys))
	for _, key := range windowKeys {
		request := windowJobs[key]
		windowTasks = append(windowTasks, func(taskCtx context.Context) error {
			result := &windowResult{}
			cached, err := s.GetBySourceReference(taskCtx, request.ref)
			if err != nil {
				return fmt.Errorf("stock window cache lookup: %w", err)
			}
			if cached != nil {
				result.fragment = cached
			} else {
				out, err := os.CreateTemp(filepath.Dir(request.state.path), ".velox-stock-window-*.mp4")
				if err != nil {
					return fmt.Errorf("create stock window output: %w", err)
				}
				result.outputPath = out.Name()
				if err := out.Close(); err != nil {
					_ = os.Remove(result.outputPath)
					return err
				}
				start := float64(request.startUS) / 1_000_000
				end := float64(request.startUS+request.durationUS) / 1_000_000
				trimmed, err := s.videoTrimmer.TrimFramesWithProbe(taskCtx, request.state.probe, request.state.path, result.outputPath, VideoSegment{StartSeconds: start, EndSeconds: end}, request.wantFrames)
				if err != nil {
					_ = os.Remove(result.outputPath)
					return fmt.Errorf("stock segment %q materialization: %w", request.segmentID, err)
				}
				cert, err := s.videoTrimmer.Probe(taskCtx, result.outputPath)
				if err != nil {
					_ = os.Remove(result.outputPath)
					return fmt.Errorf("stock segment %q output certification: %w", request.segmentID, err)
				}
				if cert.FrameCount != request.wantFrames {
					_ = os.Remove(result.outputPath)
					return fmt.Errorf("stock segment %q output frame count mismatch: got %d want %d", request.segmentID, cert.FrameCount, request.wantFrames)
				}
				expectedDuration := float64(request.wantFrames) * float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum)
				frameDuration := float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum)
				if math.Abs(cert.DurationSeconds-expectedDuration) > frameDuration {
					_ = os.Remove(result.outputPath)
					return fmt.Errorf("stock segment %q output duration mismatch: got %.6f s want %.6f s", request.segmentID, cert.DurationSeconds, expectedDuration)
				}
				if len(cert.Keyframes) == 0 || math.Abs(cert.Keyframes[0]) > 0.0005 {
					_ = os.Remove(result.outputPath)
					return fmt.Errorf("stock segment %q output does not start with a keyframe", request.segmentID)
				}
				profile := s.videoTrimmer.Profile()
				if !matchesCanonicalStockVideo(cert, profile) {
					_ = os.Remove(result.outputPath)
					return fmt.Errorf("stock segment %q output does not match canonical assembly profile", request.segmentID)
				}
				trimmed.Plan.OutputFrameCount = cert.FrameCount
				trimmed.Plan.DurationSeconds = float64(cert.FrameCount) * float64(profile.FPSDen) / float64(profile.FPSNum)
				trimmed.Plan.FirstFrameKeyframe = true
				trimmed.Plan.ClosedGOP = profile.ClosedGOP
				trimmed.Plan.CopyEligible = true
				trimmed.Plan.SourceSHA256 = request.state.sha
				trimmed.Plan.SourceSizeBytes = request.state.size
				trimmed.Plan.SourceDurationUS = request.fileDurationUS
				trimmed.Plan.SourceFrameCount = request.state.probe.FrameCount
				trimmed.Plan.SourceFPSNum, trimmed.Plan.SourceFPSDen = request.state.probe.FPSNum, request.state.probe.FPSDen
				trimmed.Plan.SourceCodec = request.state.probe.VideoCodec
				trimmed.Plan.SourceProfile = request.state.probe.VideoProfile
				trimmed.Plan.SourceLevel = request.state.probe.VideoLevel
				trimmed.Plan.SourceWidth, trimmed.Plan.SourceHeight = request.state.probe.Width, request.state.probe.Height
				trimmed.Plan.SourcePixelFormat = request.state.probe.PixelFormat
				trimmed.Plan.SourceKeyframes = append([]float64(nil), request.state.probe.Keyframes...)
				result.trimPlan = trimmed.Plan
			}
			resultsMu.Lock()
			windowResults[request.ref] = result
			resultsMu.Unlock()
			return nil
		})
	}
	if err := runBoundedStockJobs(ctx, stockWindowMaterializationConcurrency, windowTasks); err != nil {
		for _, result := range windowResults {
			if result.outputPath != "" {
				_ = os.Remove(result.outputPath)
			}
		}
		return "", "", err
	}
	for _, key := range windowKeys {
		result := windowResults[key]
		if result.outputPath == "" {
			continue
		}
		fragment, err := s.registerPreparedVideoFile(ctx, result.outputPath, key, result.trimPlan)
		_ = os.Remove(result.outputPath)
		if err != nil {
			for _, pending := range windowResults {
				if pending.outputPath != "" {
					_ = os.Remove(pending.outputPath)
				}
			}
			return "", "", fmt.Errorf("register prepared stock window: %w", err)
		}
		result.fragment = fragment
	}
	for _, placement := range placements {
		result := windowResults[placement.windowRef]
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
	if err := contract.ValidateCompiledRenderPlanV2(&plan); err != nil {
		return "", "", fmt.Errorf("prepared compiled render plan is invalid: %w", err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return "", "", err
	}
	return string(canonical), contract.HashCompiledPlanV2(canonical), nil
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
		if !strings.EqualFold(strings.TrimSpace(fmt.Sprint(asset["role"])), "stock") {
			continue
		}
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

// PlanHasDriveStock reports whether a compiled plan uses a Drive-backed stock
// asset in its video timeline. It shares the same asset and binding rules as
// PrepareDriveStockWindows so intake routing cannot drift from preparation.
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
			if isDriveStock(asset) || isDriveRuntimeBinding(binding) {
				return true
			}
		}
	}
	return false
}

// RemovePreparedStockBindings drops only runtime bindings for Drive-backed
// stock that has been materialized into certified plan assets. Keeping the
// classifier beside preparation makes the submit and finalize projections use
// the same definition of a Drive binding.
func RemovePreparedStockBindings(assets []map[string]interface{}) []map[string]interface{} {
	filtered := make([]map[string]interface{}, 0, len(assets))
	for _, asset := range assets {
		role, _ := asset["role"].(string)
		if strings.EqualFold(strings.TrimSpace(role), "stock") && isDriveRuntimeBinding(asset) {
			continue
		}
		filtered = append(filtered, asset)
	}
	return filtered
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
