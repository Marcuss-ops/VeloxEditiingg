package assets

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"velox-shared/contract"
)

// PlanVideoSourceCertificationTasks fans out one artifact-free verification
// task per missing immutable source identity. Window count and timeline
// placement do not participate in the task identity.
func (s *AssetService) PlanVideoSourceCertificationTasks(planJSON string, runtimeAssets []map[string]interface{}) ([]contract.VideoSourceCertificationTaskV1, error) {
	if s == nil || s.videoWindowVerifier == nil {
		return nil, fmt.Errorf("video source certification planning is unavailable")
	}
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return nil, fmt.Errorf("decode compiled plan for source certification: %w", err)
	}
	profile := s.videoWindowVerifier.profile
	revision := s.videoWindowVerifier.preparationRevision()
	bindings := stockRuntimeBindings(runtimeAssets)
	byIdentity := make(map[string]*contract.VideoSourceCertificationTaskV1)
	for _, asset := range plan.Assets {
		if !strings.EqualFold(strings.TrimSpace(asset.Kind), "video") {
			continue
		}
		binding := stockRuntimeBindingForAsset(asset, bindings)
		if !isDriveStock(asset) && !isDriveRuntimeBinding(binding) {
			continue
		}
		sha := strings.ToLower(strings.TrimSpace(asset.SHA256))
		runtimeSHA := strings.ToLower(bindingString(binding, "source_sha256", "sha256", "asset_sha256"))
		if sha != "" && runtimeSHA != "" && sha != runtimeSHA {
			return nil, fmt.Errorf("runtime source digest conflicts with compiled asset %s", asset.AssetID)
		}
		if sha == "" {
			sha = runtimeSHA
		}
		size := asset.SizeBytes
		runtimeSize := bindingInt64(binding, "size_bytes")
		if size > 0 && runtimeSize > 0 && size != runtimeSize {
			return nil, fmt.Errorf("runtime source size conflicts with compiled asset %s", asset.AssetID)
		}
		if size <= 0 {
			size = runtimeSize
		}
		if size <= 0 {
			return nil, fmt.Errorf("video source %s needs a positive immutable size before certification", asset.AssetID)
		}
		if asset.SourceCertificate != nil {
			if err := asset.SourceCertificate.Validate(); err != nil || asset.SourceCertificate.SHA256 != sha || asset.SourceCertificate.SizeBytes != size || asset.SourceCertificate.PreparationRevision != revision || asset.SourceCertificate.ProfileID != profile.ProfileID {
				return nil, fmt.Errorf("compiled asset %s carries invalid source certificate evidence", asset.AssetID)
			}
			continue
		}
		source := asset
		source.Kind, source.SHA256, source.SizeBytes = "video", sha, size
		driveID := strings.TrimSpace(source.DriveFileID)
		if driveID == "" {
			driveID = bindingString(binding, "drive_file_id")
		}
		if driveID != "" {
			source.SourceKind, source.DriveFileID = "drive", driveID
		}
		request := contract.VideoSourceCertificationV1{
			Version: contract.VideoSourceCertificationVersion, Source: source,
			OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
		}
		identity, err := contract.VideoSourceCertificationIdentity(request)
		if err != nil {
			return nil, fmt.Errorf("video source %s certification identity: %w", asset.AssetID, err)
		}
		task := byIdentity[identity]
		if task == nil {
			task = &contract.VideoSourceCertificationTaskV1{Identity: identity, Request: request}
			byIdentity[identity] = task
		}
		found := false
		for _, id := range task.AssetIDs {
			found = found || id == asset.AssetID
		}
		if !found {
			task.AssetIDs = append(task.AssetIDs, asset.AssetID)
		}
	}
	identities := make([]string, 0, len(byIdentity))
	for identity := range byIdentity {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	out := make([]contract.VideoSourceCertificationTaskV1, 0, len(identities))
	for _, identity := range identities {
		task := *byIdentity[identity]
		sort.Strings(task.AssetIDs)
		out = append(out, task)
	}
	return out, nil
}
