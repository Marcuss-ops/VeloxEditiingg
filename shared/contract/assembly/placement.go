package assembly

import (
	"fmt"
	"sort"
)

// WorkerPlacementSnapshot is the scheduler's read-only input. Values must
// come from authoritative worker/lease telemetry; an unknown capacity is not
// treated as available.
type WorkerPlacementSnapshot struct {
	WorkerID              string   `json:"worker_id"`
	Available             bool     `json:"available"`
	CapacityAuthoritative bool     `json:"capacity_authoritative"`
	ActiveExecutionSlots  int      `json:"active_execution_slots"`
	MaxExecutionSlots     int      `json:"max_execution_slots"`
	FreeDiskBytes         uint64   `json:"free_disk_bytes"`
	EstimatedAvailableMS  int64    `json:"estimated_available_ms"`
	NetworkMbps           float64  `json:"network_mbps"`
	Capabilities          []string `json:"capabilities"`
	CachedSHA256          []string `json:"cached_sha256"`
}

type PlacementRequest struct {
	RequiredCapabilities []string `json:"required_capabilities"`
	AssetSHA256          []string `json:"asset_sha256"`
	MinimumFreeDiskBytes uint64   `json:"minimum_free_disk_bytes"`
}

type PlacementDecision struct {
	WorkerID         string `json:"worker_id"`
	Score            int64  `json:"score"`
	CachedAssets     int    `json:"cached_assets"`
	MissingAssets    int    `json:"missing_assets"`
	EstimatedStartMS int64  `json:"estimated_start_ms"`
}

// SelectPreferredWorker chooses a warm-placement target without mutating
// state. Ties are stable by worker_id, so retries produce the same result.
func SelectPreferredWorker(workers []WorkerPlacementSnapshot, request PlacementRequest) (PlacementDecision, error) {
	var candidates []PlacementDecision
	for _, worker := range workers {
		if !worker.Available || !worker.CapacityAuthoritative || worker.WorkerID == "" {
			continue
		}
		if worker.MaxExecutionSlots <= 0 || worker.ActiveExecutionSlots >= worker.MaxExecutionSlots {
			continue
		}
		if worker.FreeDiskBytes < request.MinimumFreeDiskBytes || !hasCapabilities(worker.Capabilities, request.RequiredCapabilities) {
			continue
		}
		cached := countIntersection(worker.CachedSHA256, request.AssetSHA256)
		missing := len(request.AssetSHA256) - cached
		freeSlots := worker.MaxExecutionSlots - worker.ActiveExecutionSlots
		// Cache locality dominates because it directly removes download work.
		// Availability and free disk are eligibility gates; load and network
		// break otherwise similar choices.
		score := int64(cached)*1000 + int64(freeSlots)*100 + int64(worker.FreeDiskBytes/(1024*1024*1024))
		score += int64(worker.NetworkMbps)
		score -= worker.EstimatedAvailableMS / 10
		candidates = append(candidates, PlacementDecision{WorkerID: worker.WorkerID, Score: score, CachedAssets: cached, MissingAssets: missing, EstimatedStartMS: worker.EstimatedAvailableMS})
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

func countIntersection(left, right []string) int {
	set := make(map[string]struct{}, len(left))
	for _, value := range left {
		set[value] = struct{}{}
	}
	seen := make(map[string]struct{}, len(right))
	count := 0
	for _, value := range right {
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		if _, ok := set[value]; ok {
			count++
		}
	}
	return count
}
