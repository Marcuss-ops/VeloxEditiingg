package contract

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
)

// StockPoolCandidateV2 is the source metadata needed to choose a stable,
// sufficiently long subset for a CompiledRenderPlanV2 stock pool.
type StockPoolCandidateV2 struct {
	AssetID         string `json:"asset_id"`
	DriveFileID     string `json:"drive_file_id"`
	SizeBytes       int64  `json:"size_bytes"`
	DurationMS      int64  `json:"duration_ms"`
	StockPoolMember bool   `json:"stock_pool_member"`
	SourceSHA256    string `json:"source_sha256,omitempty"`
	SHA256          string `json:"sha256,omitempty"`
	AssetSHA256     string `json:"asset_sha256,omitempty"`
}

// StockPoolSnapshotV1 records the stable per-job member selection in the task
// payload. The compiled render plan remains immutable.
type StockPoolSnapshotV1 struct {
	Version        int                    `json:"version"`
	JobID          string                 `json:"job_id"`
	TimelineSHA256 string                 `json:"timeline_sha256"`
	DriveFolderID  string                 `json:"drive_folder_id"`
	Members        []StockPoolCandidateV2 `json:"members"`
}

// SelectCompiledStockPoolMembers deterministically selects Drive stock
// members for all stock-pool slots. If the pool is shorter than the timeline,
// the finalized timeline cycles through these members until every slot is
// covered. The Master persists the exact selection before dispatch.
func SelectCompiledStockPoolMembers(
	plan *CompiledRenderPlanV2,
	jobID string,
	candidates []StockPoolCandidateV2,
) ([]StockPoolCandidateV2, error) {
	if plan == nil || plan.StockPool == nil {
		return nil, fmt.Errorf("compiled render plan v2: stock_pool is required")
	}
	if plan.Output.FPSNum <= 0 || plan.Output.FPSDen <= 0 {
		return nil, fmt.Errorf("compiled render plan v2: stock_pool output frame rate is invalid")
	}
	var framesNeeded int64
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if !segment.StockPoolSlot {
				continue
			}
			if segment.FrameCount <= 0 || segment.FrameCount > math.MaxInt64-framesNeeded {
				return nil, fmt.Errorf("compiled render plan v2: stock_pool slot duration is invalid")
			}
			framesNeeded += segment.FrameCount
		}
	}
	if framesNeeded <= 0 {
		return nil, fmt.Errorf("compiled render plan v2: stock_pool has no positive slot duration")
	}

	pool := make([]StockPoolCandidateV2, 0, len(candidates))
	seen := make(map[string]StockPoolCandidateV2, len(candidates))
	indexes := make(map[string]int, len(candidates))
	for _, candidate := range candidates {
		candidate.DriveFileID = strings.TrimSpace(candidate.DriveFileID)
		if !candidate.StockPoolMember || candidate.DriveFileID == "" || candidate.DurationMS <= 0 || candidate.SizeBytes <= 0 {
			continue
		}
		if _, overflow := checkedStockPoolFrames(candidate.DurationMS, plan.Output.FPSNum, plan.Output.FPSDen); overflow {
			return nil, fmt.Errorf("compiled render plan v2: stock_pool member %q duration overflows frame count", candidate.DriveFileID)
		}
		if previous, exists := seen[candidate.DriveFileID]; exists {
			if previous.SizeBytes != candidate.SizeBytes || previous.DurationMS != candidate.DurationMS ||
				stockPoolCandidateSHA(previous) != stockPoolCandidateSHA(candidate) {
				return nil, fmt.Errorf("compiled render plan v2: stock_pool member %q has conflicting metadata or identity", candidate.DriveFileID)
			}
			if candidate.AssetID < previous.AssetID {
				seen[candidate.DriveFileID] = candidate
				pool[indexes[candidate.DriveFileID]] = candidate
			}
			continue
		}
		seen[candidate.DriveFileID] = candidate
		indexes[candidate.DriveFileID] = len(pool)
		pool = append(pool, candidate)
	}
	if len(pool) == 0 {
		return nil, fmt.Errorf("compiled render plan v2: stock_pool has no eligible source members")
	}
	sort.Slice(pool, func(i, j int) bool { return pool[i].DriveFileID < pool[j].DriveFileID })
	seed := sha256.Sum256([]byte(jobID + ":" + plan.TimelineSHA256 + ":" + plan.StockPool.DriveFolderID))
	rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(seed[:8])))).Shuffle(len(pool), func(i, j int) {
		pool[i], pool[j] = pool[j], pool[i]
	})

	selected := make([]StockPoolCandidateV2, 0, len(pool))
	var framesAvailable int64
	for _, candidate := range pool {
		selected = append(selected, candidate)
		frames, _ := checkedStockPoolFrames(candidate.DurationMS, plan.Output.FPSNum, plan.Output.FPSDen)
		if frames > math.MaxInt64-framesAvailable {
			framesAvailable = math.MaxInt64
		} else {
			framesAvailable += frames
		}
		if framesAvailable >= framesNeeded {
			return selected, ValidateCompiledStockPoolMembers(plan, selected)
		}
	}
	// A shorter pool is still usable: assembly repeats the deterministically
	// shuffled member list from the beginning. Keep the metadata check so an
	// empty or malformed pool still fails before the job becomes claimable.
	if err := ValidateCompiledStockPoolMembers(plan, selected); err != nil {
		return nil, err
	}
	return selected, nil
}

// ValidateCompiledStockPoolMembers checks a persisted stock-pool snapshot
// without choosing a new subset. Callers use it when retrying a plan with an
// already selected member list.
func ValidateCompiledStockPoolMembers(plan *CompiledRenderPlanV2, members []StockPoolCandidateV2) error {
	if plan == nil || plan.StockPool == nil {
		return fmt.Errorf("compiled render plan v2: stock_pool is required")
	}
	if plan.Output.FPSNum <= 0 || plan.Output.FPSDen <= 0 {
		return fmt.Errorf("compiled render plan v2: stock_pool output frame rate is invalid")
	}
	var framesNeeded int64
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if !segment.StockPoolSlot {
				continue
			}
			if segment.FrameCount <= 0 || segment.FrameCount > math.MaxInt64-framesNeeded {
				return fmt.Errorf("compiled render plan v2: stock_pool slot duration is invalid")
			}
			framesNeeded += segment.FrameCount
		}
	}
	if framesNeeded <= 0 {
		return fmt.Errorf("compiled render plan v2: stock_pool has no positive slot duration")
	}
	var framesAvailable int64
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		member.DriveFileID = strings.TrimSpace(member.DriveFileID)
		if !member.StockPoolMember || member.DriveFileID == "" || member.DurationMS <= 0 || member.SizeBytes <= 0 {
			return fmt.Errorf("compiled render plan v2: persisted stock_pool member is invalid")
		}
		if _, duplicate := seen[member.DriveFileID]; duplicate {
			return fmt.Errorf("compiled render plan v2: persisted stock_pool member %q is duplicated", member.DriveFileID)
		}
		seen[member.DriveFileID] = struct{}{}
		frames, overflow := checkedStockPoolFrames(member.DurationMS, plan.Output.FPSNum, plan.Output.FPSDen)
		if overflow {
			return fmt.Errorf("compiled render plan v2: stock_pool member %q duration overflows frame count", member.DriveFileID)
		}
		if frames > math.MaxInt64-framesAvailable {
			framesAvailable = math.MaxInt64
		} else {
			framesAvailable += frames
		}
	}
	if len(seen) == 0 || framesAvailable <= 0 {
		return fmt.Errorf("compiled render plan v2: stock_pool has no positive member duration")
	}
	return nil
}

// ResolveCompiledStockPoolMembers consumes a persisted member snapshot when
// available and derives the deterministic selection only for legacy tasks.
func ResolveCompiledStockPoolMembers(
	plan *CompiledRenderPlanV2,
	jobID string,
	rawSnapshot interface{},
	candidates []StockPoolCandidateV2,
) ([]StockPoolCandidateV2, error) {
	if rawSnapshot == nil {
		return SelectCompiledStockPoolMembers(plan, jobID, candidates)
	}
	encoded, err := json.Marshal(rawSnapshot)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: encode stock_pool snapshot: %w", err)
	}
	var snapshot StockPoolSnapshotV1
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return nil, fmt.Errorf("compiled render plan v2: decode stock_pool snapshot: %w", err)
	}
	if plan == nil || plan.StockPool == nil || snapshot.Version != 1 || snapshot.JobID != jobID ||
		snapshot.TimelineSHA256 != plan.TimelineSHA256 || snapshot.DriveFolderID != plan.StockPool.DriveFolderID {
		return nil, fmt.Errorf("compiled render plan v2: stock_pool snapshot does not match job and plan")
	}
	available := make(map[string]StockPoolCandidateV2, len(candidates))
	for _, candidate := range candidates {
		driveID := strings.TrimSpace(candidate.DriveFileID)
		if !candidate.StockPoolMember || driveID == "" {
			continue
		}
		if previous, exists := available[driveID]; exists {
			if previous.SizeBytes != candidate.SizeBytes || previous.DurationMS != candidate.DurationMS ||
				stockPoolCandidateSHA(previous) != stockPoolCandidateSHA(candidate) {
				return nil, fmt.Errorf("compiled render plan v2: stock_pool member %q has conflicting identities", driveID)
			}
			if previous.AssetID <= candidate.AssetID {
				continue
			}
		}
		available[driveID] = candidate
	}
	for _, member := range snapshot.Members {
		driveID := strings.TrimSpace(member.DriveFileID)
		current, ok := available[driveID]
		if !ok || current.SizeBytes != member.SizeBytes || current.DurationMS != member.DurationMS || current.AssetID != member.AssetID ||
			(stockPoolCandidateSHA(member) != "" && !strings.EqualFold(stockPoolCandidateSHA(member), stockPoolCandidateSHA(current))) {
			return nil, fmt.Errorf("compiled render plan v2: stock_pool snapshot member %q is missing or changed", driveID)
		}
	}
	if err := ValidateCompiledStockPoolMembers(plan, snapshot.Members); err != nil {
		return nil, fmt.Errorf("compiled render plan v2: invalid stock_pool snapshot: %w", err)
	}
	return snapshot.Members, nil
}

// ExpandCompiledStockPoolSlots resolves editorial stock slots into explicit
// source windows using the already persisted member ordering. The selected
// pool is cycled when its total duration is shorter than the requested slots.
// It deliberately does not assert media certification; ingest/preparation
// must attach those facts before the plan becomes executable.
func ExpandCompiledStockPoolSlots(plan *CompiledRenderPlanV2, members []StockPoolCandidateV2) error {
	if plan == nil || plan.StockPool == nil {
		return fmt.Errorf("compiled render plan v2: stock_pool is required")
	}
	if err := ValidateCompiledStockPoolMembers(plan, members); err != nil {
		return err
	}
	ordered := append([]StockPoolCandidateV2(nil), members...)
	framesLeft := make([]int64, len(ordered))
	assetByID := make(map[string]AssetRefV2, len(plan.Assets)+len(ordered))
	for _, asset := range plan.Assets {
		assetByID[asset.AssetID] = asset
	}
	for i, member := range ordered {
		if member.DurationMS > math.MaxInt64/1000 {
			return fmt.Errorf("compiled render plan v2: stock_pool member %q duration overflows microseconds", member.DriveFileID)
		}
		frames, overflow := checkedStockPoolFrames(member.DurationMS, plan.Output.FPSNum, plan.Output.FPSDen)
		if overflow || frames <= 0 {
			return fmt.Errorf("compiled render plan v2: stock_pool member %q has invalid frame duration", member.DriveFileID)
		}
		framesLeft[i] = frames
		assetID := "stockpool:" + member.DriveFileID
		if existing, ok := assetByID[assetID]; ok && (existing.SHA256 != stockPoolCandidateSHA(member) || existing.SizeBytes != member.SizeBytes) {
			return fmt.Errorf("compiled render plan v2: stock_pool asset %q conflicts with its immutable identity", member.DriveFileID)
		}
		if _, ok := assetByID[assetID]; !ok {
			sha := stockPoolCandidateSHA(member)
			assetByID[assetID] = AssetRefV2{AssetID: assetID, AssetKey: member.DriveFileID, SourceKind: "drive", DriveFileID: member.DriveFileID, SHA256: sha, SizeBytes: member.SizeBytes, Kind: "video", MIME: "video/mp4", DurationUS: member.DurationMS * 1000, FrameCount: frames}
		}
	}
	assets := make([]AssetRefV2, 0, len(assetByID))
	for _, asset := range assetByID {
		assets = append(assets, asset)
	}
	plan.Assets = assets
	cursor := 0
	for trackIndex := range plan.VideoTracks {
		segments := make([]VideoSegmentV2, 0, len(plan.VideoTracks[trackIndex].Segments))
		for _, slot := range plan.VideoTracks[trackIndex].Segments {
			if !slot.StockPoolSlot {
				segments = append(segments, slot)
				continue
			}
			remaining, timelineFrame, part := slot.FrameCount, slot.TimelineStartFrame, 0
			for remaining > 0 {
				for cursor < len(ordered) && framesLeft[cursor] <= 0 {
					cursor++
				}
				if cursor == len(ordered) {
					cursor = 0
					for i, member := range ordered {
						framesLeft[i], _ = checkedStockPoolFrames(member.DurationMS, plan.Output.FPSNum, plan.Output.FPSDen)
					}
				}
				member := ordered[cursor]
				count := framesLeft[cursor]
				if count > remaining {
					count = remaining
				}
				if count <= 0 {
					return fmt.Errorf("compiled render plan v2: stock_pool member cycle made no progress")
				}
				if count > math.MaxInt64/int64(plan.Output.FPSDen)/1_000_000 {
					return fmt.Errorf("compiled render plan v2: stock_pool segment duration overflows microseconds")
				}
				part++
				durationUS := count * int64(plan.Output.FPSDen) * 1_000_000 / int64(plan.Output.FPSNum)
				assetID := "stockpool:" + member.DriveFileID
				segments = append(segments, VideoSegmentV2{SegmentID: fmt.Sprintf("%s-pool-%03d", slot.SegmentID, part), AssetID: assetID, SHA256: assetByID[assetID].SHA256, TimelineStartFrame: timelineFrame, FrameCount: count, SourceInUS: 0, SourceDurationUS: durationUS})
				framesLeft[cursor] -= count
				remaining -= count
				timelineFrame += count
				if framesLeft[cursor] == 0 {
					cursor++
				}
			}
		}
		plan.VideoTracks[trackIndex].Segments = segments
	}
	plan.StockPool = nil
	return nil
}

func stockPoolCandidateSHA(candidate StockPoolCandidateV2) string {
	for _, value := range []string{candidate.SourceSHA256, candidate.SHA256, candidate.AssetSHA256} {
		if value = strings.TrimSpace(value); value != "" {
			return strings.ToLower(value)
		}
	}
	return ""
}

func checkedStockPoolFrames(durationMS int64, fpsNum, fpsDen int) (int64, bool) {
	if durationMS <= 0 || fpsNum <= 0 || fpsDen <= 0 || int64(fpsDen) > math.MaxInt64/1000 || durationMS > math.MaxInt64/int64(fpsNum) {
		return 0, true
	}
	return durationMS * int64(fpsNum) / (1000 * int64(fpsDen)), false
}
