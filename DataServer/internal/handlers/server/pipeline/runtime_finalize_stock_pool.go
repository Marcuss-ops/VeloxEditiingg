package pipeline

import (
	"encoding/json"
	"fmt"
	"math"
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

// prepareRuntimeStockPoolPlan persists the deterministic member selection but
// leaves editorial stock slots unresolved. Source certification must finish
// before ExpandCompiledStockPoolSlots chooses source offsets: without the
// certificate, those offsets cannot be aligned to actual keyframes.
func prepareRuntimeStockPoolPlan(
	planJSON string,
	jobID string,
	bindings []map[string]interface{},
) (string, []map[string]interface{}, map[string]interface{}, error) {
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return "", nil, nil, fmt.Errorf("decode compiled stock pool plan: %w", err)
	}
	if plan.StockPool == nil {
		return planJSON, bindings, nil, nil
	}
	selectedBindings, snapshot, err := snapshotRuntimeStockPool(&plan, jobID, bindings)
	if err != nil {
		return "", nil, nil, err
	}
	encodedMembers, err := json.Marshal(snapshot["members"])
	if err != nil {
		return "", nil, nil, fmt.Errorf("encode selected stock pool members: %w", err)
	}
	var members []contract.StockPoolCandidateV2
	if err := json.Unmarshal(encodedMembers, &members); err != nil {
		return "", nil, nil, fmt.Errorf("decode selected stock pool members: %w", err)
	}
	assetsByID := make(map[string]contract.AssetRefV2, len(plan.Assets)+len(members))
	for _, asset := range plan.Assets {
		assetsByID[asset.AssetID] = asset
	}
	for _, member := range members {
		assetID := "stockpool:" + member.DriveFileID
		if existing, ok := assetsByID[assetID]; ok {
			if (existing.SHA256 != "" && existing.SHA256 != stockPoolCandidateDigest(member)) ||
				(existing.SizeBytes > 0 && existing.SizeBytes != member.SizeBytes) {
				return "", nil, nil, fmt.Errorf("stock pool source %q conflicts with its selected immutable identity", member.DriveFileID)
			}
			continue
		}
		if member.DurationMS <= 0 || member.DurationMS > math.MaxInt64/1000 {
			return "", nil, nil, fmt.Errorf("stock pool source %q has invalid duration", member.DriveFileID)
		}
		assetsByID[assetID] = contract.AssetRefV2{
			AssetID: assetID, AssetKey: member.DriveFileID, SourceKind: "drive",
			DriveFileID: member.DriveFileID, SHA256: stockPoolCandidateDigest(member),
			SizeBytes: member.SizeBytes, Kind: "video", MIME: "video/mp4",
			DurationUS: member.DurationMS * 1000,
		}
	}
	plan.Assets = plan.Assets[:0]
	for _, asset := range assetsByID {
		plan.Assets = append(plan.Assets, asset)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return "", nil, nil, fmt.Errorf("canonicalize stock pool source plan: %w", err)
	}
	return string(canonical), selectedBindings, snapshot, nil
}

// expandRuntimeStockPoolPlanFromSnapshot materializes the persisted member
// selection only after its source certificates have been attached to the plan.
func expandRuntimeStockPoolPlanFromSnapshot(
	planJSON string,
	jobID string,
	rawSnapshot interface{},
	bindings []map[string]interface{},
) (string, error) {
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return "", fmt.Errorf("decode compiled stock pool plan: %w", err)
	}
	if plan.StockPool == nil {
		return planJSON, nil
	}
	encodedBindings, err := json.Marshal(bindings)
	if err != nil {
		return "", fmt.Errorf("encode selected stock pool bindings: %w", err)
	}
	var candidates []contract.StockPoolCandidateV2
	if err := json.Unmarshal(encodedBindings, &candidates); err != nil {
		return "", fmt.Errorf("decode selected stock pool bindings: %w", err)
	}
	members, err := contract.ResolveCompiledStockPoolMembers(&plan, jobID, rawSnapshot, candidates)
	if err != nil {
		return "", err
	}
	if err := contract.ExpandCompiledStockPoolSlots(&plan, members); err != nil {
		return "", fmt.Errorf("expand stock pool slots: %w", err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return "", fmt.Errorf("canonicalize expanded stock pool plan: %w", err)
	}
	return string(canonical), nil
}

func stockPoolBindingMember(binding map[string]interface{}) bool {
	member, _ := binding["stock_pool_member"].(bool)
	return member
}

func stockPoolBindingSHA(binding map[string]interface{}) string {
	for _, key := range []string{"source_sha256", "sha256", "asset_sha256"} {
		if value, ok := binding[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.ToLower(strings.TrimSpace(value))
		}
	}
	return ""
}

func stockPoolCandidateDigest(candidate contract.StockPoolCandidateV2) string {
	for _, digest := range []string{candidate.SourceSHA256, candidate.SHA256, candidate.AssetSHA256} {
		if digest = strings.ToLower(strings.TrimSpace(digest)); digest != "" {
			return digest
		}
	}
	return ""
}
