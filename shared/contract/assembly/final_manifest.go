package assembly

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const FinalAssemblyManifestVersionV1 = ContractVersion

// FinalAssemblyManifest is the control-plane read model after late-bound
// artifacts (for example Chronon-generated scenes) are published. It is
// revisioned independently from the early preparation request.
type FinalAssemblyManifest struct {
	ContractVersion  string              `json:"contract_version"`
	JobID            string              `json:"job_id"`
	Revision         uint64              `json:"revision"`
	PreparationHash  string              `json:"preparation_hash"`
	TimelineRevision uint64              `json:"timeline_revision"`
	TimelineHash     string              `json:"timeline_hash"`
	ExpectedProfile  string              `json:"expected_profile"`
	Artifacts        []PublishedArtifact `json:"artifacts"`
	LastDeltaHash    string              `json:"last_delta_hash,omitempty"`
}

// FinalManifestDelta is an atomic incremental update. UpsertedArtifacts may
// contain source-independent artifacts produced by Chronon or RenderingGen;
// invalidated IDs are removed from the final read model. The base revision
// prevents an out-of-order producer from overwriting a newer manifest.
type FinalManifestDelta struct {
	ContractVersion        string              `json:"contract_version"`
	JobID                  string              `json:"job_id"`
	BaseRevision           uint64              `json:"base_revision"`
	Revision               uint64              `json:"revision"`
	PreparationHash        string              `json:"preparation_hash"`
	TimelineRevision       uint64              `json:"timeline_revision,omitempty"`
	TimelineHash           string              `json:"timeline_hash,omitempty"`
	ExpectedProfile        string              `json:"expected_profile,omitempty"`
	UpsertedArtifacts      []PublishedArtifact `json:"upserted_artifacts,omitempty"`
	InvalidatedArtifactIDs []string            `json:"invalidated_artifact_ids,omitempty"`
}

func (a PublishedArtifact) Validate() error {
	if strings.TrimSpace(a.JobID) == "" || strings.TrimSpace(a.AssetID) == "" {
		return fmt.Errorf("assembly: published artifact requires job_id and asset_id")
	}
	if strings.TrimSpace(a.StorageURL) == "" {
		return fmt.Errorf("assembly: published artifact %q requires storage_url", a.AssetID)
	}
	if !validSHA256(a.SHA256) {
		return fmt.Errorf("assembly: published artifact %q requires a valid sha256", a.AssetID)
	}
	if a.TimelineRevision == 0 {
		return fmt.Errorf("assembly: published artifact %q requires timeline_revision", a.AssetID)
	}
	if a.SizeBytes <= 0 {
		return fmt.Errorf("assembly: published artifact %q requires positive size_bytes", a.AssetID)
	}
	if a.ProfileID != "" && !validEarlyAssemblyProfileID(a.ProfileID) {
		return fmt.Errorf("assembly: published artifact %q has unknown profile %q", a.AssetID, a.ProfileID)
	}
	if a.Producer != "" && !a.Producer.Valid() {
		return fmt.Errorf("assembly: published artifact %q has unknown producer %q", a.AssetID, a.Producer)
	}
	return nil
}

func (m FinalAssemblyManifest) Validate() error {
	if m.ContractVersion != ContractVersion {
		return fmt.Errorf("assembly: unsupported final manifest contract_version %q", m.ContractVersion)
	}
	if strings.TrimSpace(m.JobID) == "" || m.Revision == 0 {
		return fmt.Errorf("assembly: final manifest requires job_id and positive revision")
	}
	if m.TimelineRevision == 0 || strings.TrimSpace(m.TimelineHash) == "" {
		return fmt.Errorf("assembly: final manifest requires timeline identity")
	}
	if !validSHA256(m.PreparationHash) {
		return fmt.Errorf("assembly: final manifest requires a valid preparation_hash")
	}
	if strings.TrimSpace(m.ExpectedProfile) == "" || !validEarlyAssemblyProfileID(m.ExpectedProfile) {
		return fmt.Errorf("assembly: final manifest requires a registered expected_profile")
	}
	seen := make(map[string]struct{}, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if artifact.JobID != m.JobID {
			return fmt.Errorf("assembly: artifact %q belongs to job %q, want %q", artifact.AssetID, artifact.JobID, m.JobID)
		}
		if artifact.TimelineRevision != m.TimelineRevision {
			return fmt.Errorf("assembly: artifact %q timeline revision does not match manifest", artifact.AssetID)
		}
		if _, exists := seen[artifact.AssetID]; exists {
			return fmt.Errorf("assembly: duplicate final artifact %q", artifact.AssetID)
		}
		seen[artifact.AssetID] = struct{}{}
	}
	if m.LastDeltaHash != "" && !validSHA256(m.LastDeltaHash) {
		return fmt.Errorf("assembly: invalid last_delta_hash")
	}
	return nil
}

func (d FinalManifestDelta) Validate() error {
	if d.ContractVersion != ContractVersion {
		return fmt.Errorf("assembly: unsupported final manifest delta contract_version %q", d.ContractVersion)
	}
	if strings.TrimSpace(d.JobID) == "" || d.Revision == 0 {
		return fmt.Errorf("assembly: final manifest delta requires job_id and positive revision")
	}
	if !validSHA256(d.PreparationHash) {
		return fmt.Errorf("assembly: final manifest delta requires a valid preparation_hash")
	}
	if d.Revision <= d.BaseRevision {
		return fmt.Errorf("assembly: final manifest delta revision must exceed base_revision")
	}
	if d.TimelineRevision == 0 && d.TimelineHash != "" || d.TimelineRevision != 0 && d.TimelineHash == "" {
		return fmt.Errorf("assembly: final manifest delta timeline identity is incomplete")
	}
	seen := make(map[string]struct{}, len(d.UpsertedArtifacts)+len(d.InvalidatedArtifactIDs))
	for _, artifact := range d.UpsertedArtifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if artifact.JobID != d.JobID {
			return fmt.Errorf("assembly: delta artifact %q belongs to job %q, want %q", artifact.AssetID, artifact.JobID, d.JobID)
		}
		if _, exists := seen[artifact.AssetID]; exists {
			return fmt.Errorf("assembly: duplicate/conflicting artifact %q in delta", artifact.AssetID)
		}
		seen[artifact.AssetID] = struct{}{}
	}
	for _, id := range d.InvalidatedArtifactIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			return fmt.Errorf("assembly: final manifest delta contains empty invalidated artifact_id")
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("assembly: artifact %q cannot be upserted and invalidated in one delta", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// DeltaSHA256 is the deterministic identity used for replay detection.
func (d FinalManifestDelta) DeltaSHA256() (string, error) {
	if err := d.Validate(); err != nil {
		return "", err
	}
	artifacts := append([]PublishedArtifact(nil), d.UpsertedArtifacts...)
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].AssetID < artifacts[j].AssetID })
	invalidated := append([]string(nil), d.InvalidatedArtifactIDs...)
	sort.Strings(invalidated)
	canonical := struct {
		ContractVersion  string              `json:"contract_version"`
		JobID            string              `json:"job_id"`
		BaseRevision     uint64              `json:"base_revision"`
		Revision         uint64              `json:"revision"`
		PreparationHash  string              `json:"preparation_hash"`
		TimelineRevision uint64              `json:"timeline_revision,omitempty"`
		TimelineHash     string              `json:"timeline_hash,omitempty"`
		ExpectedProfile  string              `json:"expected_profile,omitempty"`
		Artifacts        []PublishedArtifact `json:"upserted_artifacts,omitempty"`
		Invalidated      []string            `json:"invalidated_artifact_ids,omitempty"`
	}{d.ContractVersion, d.JobID, d.BaseRevision, d.Revision, d.PreparationHash, d.TimelineRevision, d.TimelineHash, d.ExpectedProfile, artifacts, invalidated}
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// ApplyDelta atomically applies a valid delta to the previous final manifest.
// The returned RevisionReplay disposition is a no-op: callers must not
// resolve/download its artifacts again. Same revision with a different hash,
// stale base revision, or changed preparation identity is rejected.
func (m FinalAssemblyManifest) ApplyDelta(d FinalManifestDelta) (FinalAssemblyManifest, RevisionDisposition, error) {
	if err := m.Validate(); err != nil {
		return FinalAssemblyManifest{}, "", err
	}
	if err := d.Validate(); err != nil {
		return FinalAssemblyManifest{}, "", err
	}
	if m.JobID != d.JobID {
		return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: final manifest job_id changed from %q to %q", m.JobID, d.JobID)
	}
	if d.PreparationHash != m.PreparationHash {
		return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: final manifest preparation_hash changed")
	}
	if d.TimelineRevision != 0 && d.TimelineRevision < m.TimelineRevision {
		return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: final manifest timeline revision regressed")
	}
	expectedTimelineRevision := m.TimelineRevision
	if d.TimelineRevision != 0 {
		expectedTimelineRevision = d.TimelineRevision
	}
	for _, artifact := range d.UpsertedArtifacts {
		if artifact.TimelineRevision != expectedTimelineRevision {
			return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: artifact %q timeline revision does not match manifest", artifact.AssetID)
		}
	}
	deltaHash, err := d.DeltaSHA256()
	if err != nil {
		return FinalAssemblyManifest{}, "", err
	}
	if d.Revision == m.Revision && m.LastDeltaHash == deltaHash {
		if d.BaseRevision != m.Revision-1 {
			return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: replay base_revision=%d does not match prior revision=%d", d.BaseRevision, m.Revision-1)
		}
		return m, RevisionReplay, nil
	}
	if d.Revision <= m.Revision {
		return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: final manifest revision %d already applied with a different delta", d.Revision)
	}
	if d.BaseRevision != m.Revision {
		return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: final manifest base_revision=%d does not match current revision=%d", d.BaseRevision, m.Revision)
	}

	if d.ExpectedProfile != "" && d.ExpectedProfile != m.ExpectedProfile {
		return FinalAssemblyManifest{}, "", fmt.Errorf("assembly: final manifest profile changed from %q to %q", m.ExpectedProfile, d.ExpectedProfile)
	}

	byID := make(map[string]PublishedArtifact, len(m.Artifacts)+len(d.UpsertedArtifacts))
	for _, artifact := range m.Artifacts {
		byID[artifact.AssetID] = artifact
	}
	for _, id := range d.InvalidatedArtifactIDs {
		delete(byID, id)
	}
	for _, artifact := range d.UpsertedArtifacts {
		byID[artifact.AssetID] = artifact
	}
	artifacts := make([]PublishedArtifact, 0, len(byID))
	for _, artifact := range byID {
		artifacts = append(artifacts, artifact)
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].AssetID < artifacts[j].AssetID })

	next := m
	next.Revision = d.Revision
	next.Artifacts = artifacts
	next.LastDeltaHash = deltaHash
	if d.PreparationHash != "" {
		next.PreparationHash = d.PreparationHash
	}
	if d.TimelineRevision != 0 {
		next.TimelineRevision = d.TimelineRevision
		next.TimelineHash = d.TimelineHash
	}
	if d.ExpectedProfile != "" {
		next.ExpectedProfile = d.ExpectedProfile
	}
	if err := next.Validate(); err != nil {
		return FinalAssemblyManifest{}, "", err
	}
	return next, RevisionApplied, nil
}
