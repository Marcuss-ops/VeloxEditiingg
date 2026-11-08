package assets

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"velox-shared/contract"
)

type PreparedWindowAssetBinding struct {
	AssetID         string `json:"asset_id"`
	SHA256          string `json:"sha256"`
	SizeBytes       int64  `json:"size_bytes"`
	ExtradataSHA256 string `json:"extradata_sha256"`
}

// PlanDriveStockWindowTasks converts unresolved Drive/HTTPS stock windows
// into worker preparation requests without materializing source bytes on the
// Master. It requires an immutable source SHA and size; callers must reject
// or explicitly route unknown identities through a separate identity stage.
func (s *AssetService) PlanDriveStockWindowTasks(planJSON string, runtimeAssets []map[string]interface{}) ([]contract.VideoPrepareWindowTaskV1, error) {
	if s == nil || s.videoTrimmer == nil {
		return nil, fmt.Errorf("worker stock-window planning is unavailable")
	}
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return nil, fmt.Errorf("decode compiled plan: %w", err)
	}
	if len(plan.VideoTracks) == 0 || len(plan.Assets) == 0 {
		return nil, nil
	}
	profile := s.videoTrimmer.Profile()
	revision := s.videoTrimmer.PreparationRevision()
	bindings := stockRuntimeBindings(runtimeAssets)
	assetsByID := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assetsByID[asset.AssetID] = asset
	}
	byIdentity := make(map[string]contract.VideoPrepareWindowTaskV1)
	for trackIndex := range plan.VideoTracks {
		for _, segment := range plan.VideoTracks[trackIndex].Segments {
			if segment.StockPoolSlot {
				continue
			}
			asset, exists := assetsByID[segment.AssetID]
			if !exists || videoSegmentAlreadyPrepared(plan, segment, asset) {
				continue
			}
			binding := stockRuntimeBindingForAsset(asset, bindings)
			if binding == nil && !isDriveStock(asset) {
				continue
			}
			if binding == nil {
				return nil, fmt.Errorf("video segment %q has no runtime source binding", segment.SegmentID)
			}
			sha := strings.ToLower(strings.TrimSpace(asset.SHA256))
			if sha == "" {
				sha = strings.ToLower(bindingString(binding, "sha256", "asset_sha256"))
			}
			size := asset.SizeBytes
			if size <= 0 {
				size = bindingInt64(binding, "size_bytes")
			}
			if size <= 0 {
				return nil, fmt.Errorf("video segment %q source needs a positive immutable size before worker preparation", segment.SegmentID)
			}
			if segment.SourceInUS < 0 || segment.SourceDurationUS <= 0 || segment.FrameCount <= 0 {
				return nil, fmt.Errorf("video segment %q has an invalid source window", segment.SegmentID)
			}
			// An unknown SHA is not a reason to materialize bytes on the
			// control plane: the worker establishes the identity from the same
			// verified download it needs for the window and returns it in the
			// certificate.
			discovery := !validStockSourceSHA256(sha)
			source := asset
			if discovery {
				source.Kind, source.SHA256, source.SizeBytes = "video", "", size
			} else {
				source.Kind, source.SHA256, source.SizeBytes = "video", sha, size
			}
			if source.AssetID == "" {
				source.AssetID = segment.AssetID
			}
			request := contract.VideoPrepareWindowV1{
				Version: contract.VideoPrepareWindowVersion, Source: source,
				SourceIdentityDiscovery: discovery,
				SourceStartUS:           segment.SourceInUS, SourceDurationUS: segment.SourceDurationUS,
				OutputFrameCount: segment.FrameCount, TimelineRevision: plan.TimelineRevision,
				TimelineSHA256: plan.TimelineSHA256, TimelineStartFrame: segment.TimelineStartFrame,
				OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
			}
			var identity string
			var err error
			if discovery {
				identity, err = contract.VideoPrepareWindowDiscoveryIdentity(request)
			} else {
				identity, err = contract.VideoPrepareWindowCacheIdentity(request)
			}
			if err != nil {
				return nil, fmt.Errorf("video segment %q preparation contract: %w", segment.SegmentID, err)
			}
			if _, err := request.CanonicalJSON(); err != nil {
				return nil, fmt.Errorf("video segment %q preparation contract: %w", segment.SegmentID, err)
			}
			// Keep the earliest timeline placement as the certificate's attempt
			// binding; all duplicate placements reuse the same output content.
			entry, ok := byIdentity[identity]
			if !ok {
				entry = contract.VideoPrepareWindowTaskV1{Identity: identity, Request: request}
			}
			entry.SegmentIDs = append(entry.SegmentIDs, segment.SegmentID)
			if !ok || request.TimelineStartFrame < entry.Request.TimelineStartFrame {
				entry.Request = request
			}
			byIdentity[identity] = entry
		}
	}
	identities := make([]string, 0, len(byIdentity))
	for identity := range byIdentity {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	tasks := make([]contract.VideoPrepareWindowTaskV1, 0, len(identities))
	for _, identity := range identities {
		tasks = append(tasks, byIdentity[identity])
	}
	return tasks, nil
}

func validStockSourceSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

// RewriteDriveStockPlanFromWorkerResults binds registered fragment assets
// into the original immutable plan after every preparation dependency has
// succeeded and its certificate was verified by the coordinator.
func (s *AssetService) RewriteDriveStockPlanFromWorkerResults(planJSON string, runtimeAssets []map[string]interface{}, results map[string]PreparedWindowAssetBinding) (string, string, []map[string]interface{}, error) {
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return "", "", nil, fmt.Errorf("decode compiled plan: %w", err)
	}
	tasks, err := s.PlanDriveStockWindowTasks(planJSON, runtimeAssets)
	if err != nil {
		return "", "", nil, err
	}
	identityBySegment := make(map[string]string)
	taskByIdentity := make(map[string]contract.VideoPrepareWindowTaskV1, len(tasks))
	for _, task := range tasks {
		taskByIdentity[task.Identity] = task
		for _, segmentID := range task.SegmentIDs {
			identityBySegment[segmentID] = task.Identity
		}
		result, ok := results[task.Identity]
		if !ok || strings.TrimSpace(result.AssetID) == "" || !validStockSourceSHA256(result.SHA256) || result.SizeBytes <= 0 || !validStockSourceSHA256(result.ExtradataSHA256) {
			return "", "", nil, fmt.Errorf("prepared stock window %q has no registered asset binding", task.Identity)
		}
	}
	newAssets := make(map[string]contract.AssetRefV2, len(plan.Assets)+len(tasks))
	for _, asset := range plan.Assets {
		newAssets[asset.AssetID] = asset
	}
	for trackIndex := range plan.VideoTracks {
		for segmentIndex := range plan.VideoTracks[trackIndex].Segments {
			segment := &plan.VideoTracks[trackIndex].Segments[segmentIndex]
			identity, prepared := identityBySegment[segment.SegmentID]
			if !prepared {
				continue
			}
			task := taskByIdentity[identity]
			result := results[identity]
			profile, profileErr := contract.KnownCanonicalVideoProfileV1(task.Request.OutputProfileID)
			if profileErr != nil {
				return "", "", nil, profileErr
			}
			durationUS := int64(math.Round(float64(task.Request.OutputFrameCount) * 1_000_000 * float64(profile.FPSDen) / float64(profile.FPSNum)))
			fragment := contract.AssetRefV2{
				AssetID: result.AssetID, AssetKey: result.AssetID, SHA256: result.SHA256,
				SizeBytes: result.SizeBytes, Kind: "video", MIME: "video/mp4", DurationUS: durationUS,
				Width: profile.Width, Height: profile.Height, ProfileID: profile.ProfileID,
				FrameCount: task.Request.OutputFrameCount, TimelineRevision: plan.TimelineRevision,
				TimelineSHA256: plan.TimelineSHA256, TimelineStartFrame: task.Request.TimelineStartFrame,
				FirstFrameKeyframe: true, ClosedGOP: profile.ClosedGOP,
				ExtradataSHA256: result.ExtradataSHA256,
			}
			if existing, ok := newAssets[fragment.AssetID]; ok && (existing.SHA256 != fragment.SHA256 || existing.SizeBytes != fragment.SizeBytes) {
				return "", "", nil, fmt.Errorf("registered fragment asset %q has conflicting identity", fragment.AssetID)
			}
			newAssets[fragment.AssetID] = fragment
			segment.AssetID, segment.SHA256 = fragment.AssetID, fragment.SHA256
			segment.SourceInUS, segment.SourceDurationUS = 0, durationUS
		}
	}
	used := make(map[string]struct{})
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			used[segment.AssetID] = struct{}{}
		}
	}
	if plan.FinalAudio.AssetID != "" {
		used[plan.FinalAudio.AssetID] = struct{}{}
	}
	plan.Assets = plan.Assets[:0]
	for id, asset := range newAssets {
		if _, ok := used[id]; ok {
			plan.Assets = append(plan.Assets, asset)
		}
	}
	if err := contract.ValidateCompiledRenderPlanV2(&plan); err != nil {
		return "", "", nil, fmt.Errorf("prepared worker plan is invalid: %w", err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return "", "", nil, err
	}
	retained, err := RetainPlanSourceBindings(string(canonical), runtimeAssets)
	if err != nil {
		return "", "", nil, err
	}
	return string(canonical), contract.HashCompiledPlanV2(canonical), retained, nil
}
