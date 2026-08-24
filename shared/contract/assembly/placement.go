package assembly

import (
	"fmt"
	"math"
	"sort"
)

const (
	cacheAssetWeight       int64  = 1_000_000
	freeSlotWeight         int64  = 10_000
	diskGiBWeight          int64  = 10
	networkMbpsWeight      int64  = 10
	loadPenaltyWeight      int64  = 100_000
	availabilityPenaltyDiv int64  = 10
	missingBytePenaltyDiv  uint64 = 1 << 30
)

// WorkerPlacementSnapshot is the scheduler's read-only input. Capacity and
// disk are admission facts, not hints: unknown values cannot make a worker
// eligible for a warm reservation that needs those resources.
type WorkerPlacementSnapshot struct {
	WorkerID              string   `json:"worker_id"`
	Available             bool     `json:"available"`
	CapacityAuthoritative bool     `json:"capacity_authoritative"`
	DiskAuthoritative     bool     `json:"disk_authoritative"`
	ActiveExecutionSlots  int      `json:"active_execution_slots"`
	MaxExecutionSlots     int      `json:"max_execution_slots"`
	FreeDiskBytes         uint64   `json:"free_disk_bytes"`
	EstimatedAvailableMS  int64    `json:"estimated_available_ms"`
	NetworkMbps           float64  `json:"network_mbps"`
	LoadRatio             float64  `json:"load_ratio"`
	Capabilities          []string `json:"capabilities"`
	CachedSHA256          []string `json:"cached_sha256"`
}

type PlacementRequest struct {
	RequiredCapabilities []string          `json:"required_capabilities"`
	AssetSHA256          []string          `json:"asset_sha256"`
	AssetSizes           map[string]uint64 `json:"asset_sizes,omitempty"`
	MinimumFreeDiskBytes uint64            `json:"minimum_free_disk_bytes"`
}

type PlacementDecision struct {
	WorkerID         string  `json:"worker_id"`
	Score            int64   `json:"score"`
	CachedAssets     int     `json:"cached_assets"`
	MissingAssets    int     `json:"missing_assets"`
	CachedBytes      uint64  `json:"cached_bytes,omitempty"`
	MissingBytes     uint64  `json:"missing_bytes,omitempty"`
	FreeSlots        int     `json:"free_slots"`
	FreeDiskBytes    uint64  `json:"free_disk_bytes"`
	LoadRatio        float64 `json:"load_ratio"`
	EstimatedStartMS int64   `json:"estimated_start_ms"`
}

// SelectPreferredWorker chooses a warm-placement target without mutating
// state. Cache locality is the dominant positive signal; capacity, disk,
// load and estimated availability keep the choice useful when cache state is
// similar. Ties are stable by worker_id, so retries produce the same result.
func SelectPreferredWorker(workers []WorkerPlacementSnapshot, request PlacementRequest) (PlacementDecision, error) {
	var candidates []PlacementDecision
	for _, worker := range workers {
		if !worker.Available || !worker.CapacityAuthoritative || worker.WorkerID == "" {
			continue
		}
		if worker.MaxExecutionSlots <= 0 || worker.ActiveExecutionSlots < 0 || worker.ActiveExecutionSlots >= worker.MaxExecutionSlots {
			continue
		}
		if request.MinimumFreeDiskBytes > 0 {
			if !worker.DiskAuthoritative || worker.FreeDiskBytes < request.MinimumFreeDiskBytes {
				continue
			}
		}
		if !hasCapabilities(worker.Capabilities, request.RequiredCapabilities) {
			continue
		}

		cached, missing, cachedBytes, missingBytes := assetLocality(worker.CachedSHA256, request)
		freeSlots := worker.MaxExecutionSlots - worker.ActiveExecutionSlots
		score := placementScore(worker, cached, missingBytes, freeSlots)
		candidates = append(candidates, PlacementDecision{
			WorkerID: worker.WorkerID, Score: score,
			CachedAssets: cached, MissingAssets: missing,
			CachedBytes: cachedBytes, MissingBytes: missingBytes,
			FreeSlots: freeSlots, FreeDiskBytes: worker.FreeDiskBytes,
			LoadRatio:        clampRatio(worker.LoadRatio),
			EstimatedStartMS: maxInt64(worker.EstimatedAvailableMS),
		})
	}
	if len(candidates) == 0 {
		return PlacementDecision{}, fmt.Errorf("assembly: no eligible worker for preparation")
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].WorkerID < candidates[j].WorkerID
	})
	return candidates[0], nil
}

func placementScore(worker WorkerPlacementSnapshot, cachedAssets int, missingBytes uint64, freeSlots int) int64 {
	// The cache term intentionally dominates: every verified local asset
	// removes work from the execution critical path. The remaining terms are
	// bounded tie-break signals and never override a materially warmer worker.
	score := int64(cachedAssets) * cacheAssetWeight
	score += int64(freeSlots) * freeSlotWeight
	score += int64(worker.FreeDiskBytes/(1<<30)) * diskGiBWeight
	score += roundedInt64(worker.NetworkMbps) * networkMbpsWeight
	score -= int64(math.Round(clampRatio(worker.LoadRatio) * float64(loadPenaltyWeight)))
	if missingBytes > 0 {
		score -= int64(minUint64(missingBytes/missingBytePenaltyDiv, uint64(math.MaxInt64/2)))
	}
	if worker.EstimatedAvailableMS > 0 {
		score -= worker.EstimatedAvailableMS / availabilityPenaltyDiv
	}
	return score
}

func assetLocality(cachedKeys []string, request PlacementRequest) (cached, missing int, cachedBytes, missingBytes uint64) {
	cachedSet := make(map[string]struct{}, len(cachedKeys))
	for _, key := range cachedKeys {
		if key != "" {
			cachedSet[key] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(request.AssetSHA256))
	for _, key := range request.AssetSHA256 {
		if key == "" {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		size := request.AssetSizes[key]
		if _, ok := cachedSet[key]; ok {
			cached++
			cachedBytes += size
		} else {
			missing++
			missingBytes += size
		}
	}
	return cached, missing, cachedBytes, missingBytes
}

func hasCapabilities(have, required []string) bool {
	set := make(map[string]struct{}, len(have))
	for _, capability := range have {
		set[capability] = struct{}{}
	}
	for _, capability := range required {
		if _, ok := set[capability]; !ok {
			return false
		}
	}
	return true
}

func clampRatio(value float64) float64 {
	if math.IsNaN(value) || value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func roundedInt64(value float64) int64 {
	if math.IsNaN(value) || value <= 0 {
		return 0
	}
	if value >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(math.Round(value))
}

func maxInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}
