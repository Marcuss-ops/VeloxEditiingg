package assembly

import (
	"fmt"
	"strings"

	videoContract "velox-shared/contract"
)

const (
	CanonicalAssemblyProfileIDV1     = videoContract.CanonicalVideoProfileIDV1
	CanonicalAssemblyProfileLegacyV1 = "velox-h264-copy-v1"
)

func validEarlyAssemblyProfileID(profileID string) bool {
	return profileID == CanonicalAssemblyProfileIDV1 || profileID == CanonicalAssemblyProfileLegacyV1
}

// EarlyAssemblyManifest is the small, immutable handoff sent before the
// final timeline exists. It is intentionally limited to bytes that can be
// prefetched now, their verified identity, the canonical assembly profile and
// the revision used for idempotent replay.
type EarlyAssemblyManifest struct {
	ContractVersion  string                                 `json:"contract_version"`
	JobID            string                                 `json:"job_id"`
	Revision         uint64                                 `json:"revision,omitempty"`
	TimelineRevision uint64                                 `json:"timeline_revision,omitempty"` // legacy alias
	PreparationHash  string                                 `json:"preparation_hash"`
	Dispatch         DispatchPolicy                         `json:"dispatch"`
	ExpectedProfile  string                                 `json:"expected_profile"`
	Profile          *videoContract.CanonicalVideoProfileV1 `json:"profile,omitempty"`
	Timeline         []TimelineItem                         `json:"timeline,omitempty"`
	Assets           []AssetRequirement                     `json:"assets"`
}

// PreparationManifest is retained as a source-compatible name for existing
// Velox callers. New producers should use EarlyAssemblyManifest.
type PreparationManifest = EarlyAssemblyManifest

type PreparationReservation struct {
	JobID           string           `json:"job_id"`
	WorkerID        string           `json:"worker_id"`
	PreparationID   string           `json:"preparation_id"`
	PreparationHash string           `json:"preparation_hash"`
	State           PreparationState `json:"state"`
	ExpiresAt       string           `json:"expires_at"`
}

// EffectiveRevision returns the canonical revision. TimelineRevision is
// accepted only as a backwards-compatible input alias.
func (m EarlyAssemblyManifest) EffectiveRevision() (uint64, error) {
	if m.Revision != 0 && m.TimelineRevision != 0 && m.Revision != m.TimelineRevision {
		return 0, fmt.Errorf("assembly: revision and timeline_revision disagree")
	}
	if m.Revision != 0 {
		return m.Revision, nil
	}
	return m.TimelineRevision, nil
}

func (m EarlyAssemblyManifest) Validate() error {
	if m.ContractVersion != ContractVersion {
		return fmt.Errorf("assembly: unsupported early manifest contract_version %q", m.ContractVersion)
	}
	if strings.TrimSpace(m.JobID) == "" {
		return fmt.Errorf("assembly: early manifest job_id is required")
	}
	revision, err := m.EffectiveRevision()
	if err != nil {
		return err
	}
	if revision == 0 {
		return fmt.Errorf("assembly: early manifest revision is required")
	}
	if !m.Dispatch.Valid() || m.Dispatch.Mode != ModeEager || !m.Dispatch.StartPrepare {
		return fmt.Errorf("assembly: early manifest requires eager dispatch")
	}
	if strings.TrimSpace(m.ExpectedProfile) == "" {
		return fmt.Errorf("assembly: expected_profile is required")
	}
	if !validEarlyAssemblyProfileID(m.ExpectedProfile) {
		return fmt.Errorf("assembly: expected_profile %q is not a registered canonical profile", m.ExpectedProfile)
	}
	if m.Profile != nil {
		if err := m.Profile.Validate(); err != nil {
			return fmt.Errorf("assembly: invalid canonical profile: %w", err)
		}
		if m.Profile.ProfileID != m.ExpectedProfile {
			return fmt.Errorf("assembly: expected_profile %q does not match profile.profile_id %q", m.ExpectedProfile, m.Profile.ProfileID)
		}
	}
	if len(m.Assets) == 0 {
		return fmt.Errorf("assembly: early manifest requires at least one asset")
	}
	for i, asset := range m.Assets {
		if asset.Availability != AvailabilityKnown {
			return fmt.Errorf("assembly: early manifest asset %d must have known availability", i)
		}
		if asset.SizeBytes <= 0 {
			return fmt.Errorf("assembly: early manifest asset %q requires positive size_bytes", asset.AssetID)
		}
	}

	// Validate the complete asset/timeline invariants once, outside the asset
	// loop. This avoids accepting a malformed later asset merely because an
	// earlier iteration passed.
	revisionHash := fmt.Sprintf("early:%d", revision)
	if err := (CanonicalAssemblyContractV1{
		ContractVersion:  ContractVersion,
		JobID:            m.JobID,
		TimelineRevision: revision,
		TimelineHash:     revisionHash,
		Dispatch:         m.Dispatch,
		Output:           Output{ProfileID: m.ExpectedProfile},
		Assets:           m.Assets,
		Timeline:         m.Timeline,
	}).Validate(); err != nil {
		return err
	}
	want := m.ComputePreparationHash()
	if strings.TrimSpace(m.PreparationHash) == "" {
		return fmt.Errorf("assembly: preparation_hash is required")
	}
	if m.PreparationHash != want {
		return fmt.Errorf("assembly: preparation_hash does not match manifest contents")
	}
	return nil
}

// ComputePreparationHash is stable across producers: asset order does not
// affect it, while byte identity, profile, job, revision and the known
// timeline mapping do.
func (m EarlyAssemblyManifest) ComputePreparationHash() string {
	revision, _ := m.EffectiveRevision()
	return preparationHash(m.JobID, revision, m.ExpectedProfile, m.Assets, m.Timeline)
}

// RevisionIdentity is the idempotency identity for one early manifest
// revision. Replaying this exact tuple is a no-op; the same revision with a
// different preparation hash is a conflict and must never overwrite state.
func (m EarlyAssemblyManifest) RevisionIdentity() string {
	revision, _ := m.EffectiveRevision()
	return fmt.Sprintf("%s:%d:%s", strings.TrimSpace(m.JobID), revision, strings.TrimSpace(m.PreparationHash))
}

type RevisionDisposition string

const (
	RevisionApplied RevisionDisposition = "applied"
	RevisionReplay  RevisionDisposition = "replay_idempotent"
	RevisionStale   RevisionDisposition = "stale_ignored"
)

// ReconcileRevision compares an incoming manifest with the last accepted
// revision. It is pure, so a persistence layer can call it before an atomic
// insert/update. Same revision + same hash is an idempotent replay; a lower
// revision is stale; same revision + different hash is rejected.
func (m EarlyAssemblyManifest) ReconcileRevision(previous *EarlyAssemblyManifest) (RevisionDisposition, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	if previous == nil {
		return RevisionApplied, nil
	}
	if err := previous.Validate(); err != nil {
		return "", fmt.Errorf("assembly: previous early manifest invalid: %w", err)
	}
	if previous.JobID != m.JobID {
		return "", fmt.Errorf("assembly: early manifest job_id changed from %q to %q", previous.JobID, m.JobID)
	}
	currentRevision, _ := m.EffectiveRevision()
	previousRevision, _ := previous.EffectiveRevision()
	switch {
	case currentRevision < previousRevision:
		return RevisionStale, nil
	case currentRevision == previousRevision && m.PreparationHash == previous.PreparationHash:
		return RevisionReplay, nil
	case currentRevision == previousRevision:
		return "", fmt.Errorf("assembly: revision %d already exists with a different preparation_hash", currentRevision)
	default:
		return RevisionApplied, nil
	}
}

// CanonicalContract converts the early manifest into the shared assembly
// contract while preserving the revision and preparation hash. It does not
// invent a final timeline or mark the job executable.
func (m EarlyAssemblyManifest) CanonicalContract() (CanonicalAssemblyContractV1, error) {
	if err := m.Validate(); err != nil {
		return CanonicalAssemblyContractV1{}, err
	}
	revision, _ := m.EffectiveRevision()
	return CanonicalAssemblyContractV1{
		ContractVersion:  ContractVersion,
		JobID:            m.JobID,
		TimelineRevision: revision,
		TimelineHash:     m.PreparationHash,
		PreparationHash:  m.PreparationHash,
		Dispatch:         m.Dispatch,
		Assets:           append([]AssetRequirement(nil), m.Assets...),
		Timeline:         append([]TimelineItem(nil), m.Timeline...),
		Output:           Output{ProfileID: m.ExpectedProfile},
		State:            StatePreparing,
	}, nil
}

func (k AssetKind) String() string { return string(k) }
