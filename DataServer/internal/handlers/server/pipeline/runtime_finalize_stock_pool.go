package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
)

// snapshotRuntimeStockPool chooses and records the exact Drive members used by
// a stock-pool plan before the task becomes claimable. The selected runtime
// bindings are stored with the task spec, so retries and worker changes reuse
// the same input set.
func snapshotRuntimeStockPool(
	plan *contract.CompiledRenderPlanV2,
	jobID string,
	bindings []map[string]interface{},
) ([]map[string]interface{}, map[string]interface{}, error) {
	encoded, err := json.Marshal(bindings)
	if err != nil {
		return nil, nil, fmt.Errorf("encode stock pool bindings: %w", err)
	}
	var candidates []contract.StockPoolCandidateV2
	if err := json.Unmarshal(encoded, &candidates); err != nil {
		return nil, nil, fmt.Errorf("decode stock pool bindings: %w", err)
	}
	memberBindings := make(map[string]map[string]interface{})
	memberMetadata := make(map[string]contract.StockPoolCandidateV2)
	for index, candidate := range candidates {
		if !candidate.StockPoolMember {
			continue
		}
		driveID := strings.TrimSpace(candidate.DriveFileID)
		if driveID == "" {
			continue
		}
		binding := bindings[index]
		if previous := memberBindings[driveID]; previous != nil {
			previousCandidate := memberMetadata[driveID]
			if previousCandidate.SizeBytes != candidate.SizeBytes || previousCandidate.DurationMS != candidate.DurationMS ||
				stockPoolBindingSHA(previous) != stockPoolBindingSHA(binding) {
				return nil, nil, fmt.Errorf("stock pool Drive member %q has conflicting identities", driveID)
			}
			previousAssetID := strings.TrimSpace(previousCandidate.AssetID)
			candidateAssetID := strings.TrimSpace(candidate.AssetID)
			if previousAssetID <= candidateAssetID {
				continue
			}
		}
		memberBindings[driveID] = binding
		memberMetadata[driveID] = candidate
	}
	selected, err := contract.SelectCompiledStockPoolMembers(plan, jobID, candidates)
	if err != nil {
		return nil, nil, err
	}
	selectedBindings := make([]map[string]interface{}, 0, len(selected))
	selectedMembers := make([]map[string]interface{}, 0, len(selected))
	for _, candidate := range selected {
		binding := memberBindings[candidate.DriveFileID]
		if binding == nil {
			return nil, nil, fmt.Errorf("selected stock pool member %q has no runtime binding", candidate.DriveFileID)
		}
		selectedBindings = append(selectedBindings, binding)
		selectedMembers = append(selectedMembers, map[string]interface{}{
			"asset_id": candidate.AssetID, "drive_file_id": candidate.DriveFileID,
			"source_sha256": stockPoolBindingSHA(binding),
			"size_bytes":    candidate.SizeBytes, "duration_ms": candidate.DurationMS,
			"stock_pool_member": true,
		})
	}
	finalBindings := make([]map[string]interface{}, 0, len(bindings)-len(memberBindings)+len(selectedBindings))
	for _, binding := range bindings {
		if stockPoolBindingMember(binding) {
			continue
		}
		finalBindings = append(finalBindings, binding)
	}
	finalBindings = append(finalBindings, selectedBindings...)
	snapshot := map[string]interface{}{
		"version": 1, "job_id": jobID, "timeline_sha256": plan.TimelineSHA256,
		"drive_folder_id": plan.StockPool.DriveFolderID, "members": selectedMembers,
	}
	return finalBindings, snapshot, nil
}

func stockPoolBindingMember(binding map[string]interface{}) bool {
	member, _ := binding["stock_pool_member"].(bool)
	return member
}

func stockPoolBindingSHA(binding map[string]interface{}) string {
	for _, key := range []string{"sha256", "asset_sha256"} {
		if value, ok := binding[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.ToLower(strings.TrimSpace(value))
		}
	}
	return ""
}
