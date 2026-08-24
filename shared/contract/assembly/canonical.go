package assembly

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// CanonicalJSON returns the byte-stable JSON representation exchanged by
// PipelineGen, Velox and RenderingGen. Asset declarations are set-like and
// therefore sorted by asset_id; timeline order remains semantic and is kept
// unchanged. The input is never mutated.
func (j CanonicalAssemblyContractV1) CanonicalJSON() ([]byte, error) {
	if err := j.Validate(); err != nil {
		return nil, err
	}
	canonical := j
	canonical.Assets = append([]AssetRequirement(nil), j.Assets...)
	sort.SliceStable(canonical.Assets, func(i, k int) bool {
		return assetRequirementSortKey(canonical.Assets[i]) < assetRequirementSortKey(canonical.Assets[k])
	})
	if canonical.Assets == nil {
		canonical.Assets = []AssetRequirement{}
	}
	if canonical.Timeline == nil {
		canonical.Timeline = []TimelineItem{}
	}
	return json.Marshal(&canonical)
}

// ContractSHA256 returns SHA-256(CanonicalJSON()) as lowercase hexadecimal.
func (j CanonicalAssemblyContractV1) ContractSHA256() (string, error) {
	data, err := j.CanonicalJSON()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// ComputePreparationHash identifies the physical preparation inputs shared by
// all three producers. Runtime-only assets are excluded because they do not
// exist when the early prefetch request is sent. Timeline mapping, asset
// hashes, profile, job and revision are included so a changed editorial
// preparation cannot be mistaken for an already-prepared one.
func (j CanonicalAssemblyContractV1) ComputePreparationHash() string {
	return preparationHash(j.JobID, j.TimelineRevision, j.Output.ProfileID, j.Assets, j.Timeline)
}

func preparationHash(jobID string, revision uint64, profile string, assets []AssetRequirement, timeline []TimelineItem) string {
	known := make([]struct {
		ID   string `json:"asset_id"`
		Kind string `json:"kind"`
		SHA  string `json:"sha256"`
		Size int64  `json:"size_bytes,omitempty"`
	}, 0, len(assets))
	for _, asset := range assets {
		if asset.Availability != AvailabilityKnown {
			continue
		}
		known = append(known, struct {
			ID   string `json:"asset_id"`
			Kind string `json:"kind"`
			SHA  string `json:"sha256"`
			Size int64  `json:"size_bytes,omitempty"`
		}{asset.AssetID, string(asset.Kind), asset.SHA256, asset.SizeBytes})
	}
	sort.SliceStable(known, func(i, k int) bool {
		if known[i].ID != known[k].ID {
			return known[i].ID < known[k].ID
		}
		return known[i].SHA < known[k].SHA
	})
	mapping := append([]TimelineItem(nil), timeline...)
	payload := struct {
		ContractVersion  string         `json:"contract_version"`
		JobID            string         `json:"job_id"`
		TimelineRevision uint64         `json:"timeline_revision"`
		ExpectedProfile  string         `json:"expected_profile"`
		Assets           interface{}    `json:"assets"`
		Timeline         []TimelineItem `json:"timeline"`
	}{ContractVersion, jobID, revision, profile, known, mapping}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func assetRequirementSortKey(asset AssetRequirement) string {
	data, err := json.Marshal(asset)
	if err != nil {
		return fmt.Sprintf("%s\x00%s", asset.AssetID, asset.SHA256)
	}
	return asset.AssetID + "\x00" + string(data)
}

// DecodeCanonicalAssemblyContractV1 decodes and validates one canonical
// contract at the component boundary. Invalid versions, references or
// integrity metadata never reach scheduling or rendering.
func DecodeCanonicalAssemblyContractV1(data []byte) (CanonicalAssemblyContractV1, error) {
	var contract CanonicalAssemblyContractV1
	if err := json.Unmarshal(data, &contract); err != nil {
		return CanonicalAssemblyContractV1{}, fmt.Errorf("assembly: decode canonical contract: %w", err)
	}
	if err := contract.Validate(); err != nil {
		return CanonicalAssemblyContractV1{}, err
	}
	return contract, nil
}
