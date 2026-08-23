package assembly

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// PreparationManifest is the small early handoff sent before the final
// timeline exists. It contains only assets that can be prefetched now.
type PreparationManifest struct {
	ContractVersion  string             `json:"contract_version"`
	JobID            string             `json:"job_id"`
	TimelineRevision uint64             `json:"timeline_revision"`
	PreparationHash  string             `json:"preparation_hash"`
	Dispatch         DispatchPolicy     `json:"dispatch"`
	ExpectedProfile  string             `json:"expected_profile"`
	Assets           []AssetRequirement `json:"assets"`
}

type PreparationState string

const (
	PreparationAssigned    PreparationState = "assigned_for_prefetch"
	PreparationPrefetching PreparationState = "prefetching"
	PreparationPrepared    PreparationState = "prepared"
	PreparationWaiting     PreparationState = "waiting_final_manifest"
	PreparationExpired     PreparationState = "expired"
	PreparationFailed      PreparationState = "failed"
)

type PreparationReservation struct {
	JobID           string           `json:"job_id"`
	WorkerID        string           `json:"worker_id"`
	PreparationID   string           `json:"preparation_id"`
	PreparationHash string           `json:"preparation_hash"`
	State           PreparationState `json:"state"`
	ExpiresAt       string           `json:"expires_at"`
}

func (m PreparationManifest) Validate() error {
	if m.ContractVersion != ContractVersion {
		return fmt.Errorf("assembly: unsupported preparation contract_version %q", m.ContractVersion)
	}
	if strings.TrimSpace(m.JobID) == "" || m.TimelineRevision == 0 {
		return fmt.Errorf("assembly: preparation job_id and timeline_revision are required")
	}
	if !m.Dispatch.Valid() || m.Dispatch.Mode != ModeEager || !m.Dispatch.StartPrepare {
		return fmt.Errorf("assembly: preparation requires eager dispatch")
	}
	if strings.TrimSpace(m.ExpectedProfile) == "" {
		return fmt.Errorf("assembly: expected_profile is required")
	}
	if len(m.Assets) == 0 {
		return fmt.Errorf("assembly: preparation requires at least one asset")
	}
	for i, asset := range m.Assets {
		if asset.Availability != AvailabilityKnown {
			return fmt.Errorf("assembly: preparation asset %d must have known availability", i)
		}
		if err := (AssemblyJobV1{ContractVersion: ContractVersion, JobID: m.JobID, TimelineRevision: m.TimelineRevision, TimelineHash: "preparation", Dispatch: m.Dispatch, Output: Output{ProfileID: m.ExpectedProfile}, Assets: []AssetRequirement{asset}}).Validate(); err != nil {
			return err
		}
	}
	want := m.ComputePreparationHash()
	if m.PreparationHash != "" && m.PreparationHash != want {
		return fmt.Errorf("assembly: preparation_hash does not match manifest contents")
	}
	return nil
}

// ComputePreparationHash is stable across producers: asset order does not
// affect it, while byte identity, profile, job and revision do.
func (m PreparationManifest) ComputePreparationHash() string {
	parts := make([]string, 0, len(m.Assets))
	for _, asset := range m.Assets {
		parts = append(parts, asset.AssetID+"\x00"+asset.SHA256+"\x00"+asset.Kind.String())
	}
	sort.Strings(parts)
	h := sha256.New()
	for _, part := range []string{ContractVersion, m.JobID, fmt.Sprint(m.TimelineRevision), m.ExpectedProfile} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	for _, part := range parts {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func (k AssetKind) String() string { return string(k) }
