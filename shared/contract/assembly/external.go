package assembly

import (
	"fmt"
	"strings"
)

// ExternalAssemblyRequest is the small public intake shape. The handler
// normalizes it once into AssemblyJobV1 before it reaches control-plane
// persistence; renderer payloads never contain this object.
type ExternalAssemblyRequest struct {
	SendToVelox      bool               `json:"send_to_velox"`
	TimelineRevision uint64             `json:"timeline_revision,omitempty"`
	TimelineHash     string             `json:"timeline_hash,omitempty"`
	Assets           []AssetRequirement `json:"assets,omitempty"`
	Timeline         []TimelineItem     `json:"timeline,omitempty"`
	OutputProfile    string             `json:"output_profile,omitempty"`
	// EarlyManifest is the preferred producer-facing handoff. Legacy fields
	// remain accepted for compatibility, but cannot be combined with it.
	EarlyManifest *EarlyAssemblyManifest `json:"early_manifest,omitempty"`
}

// Normalize converts the public convenience shape into the canonical typed
// assembly job. A disabled request intentionally produces no job so callers
// can treat send_to_velox=false as a no-op without manufacturing invalid
// control-plane state.
func (r ExternalAssemblyRequest) Normalize(jobID string) (*AssemblyJobV1, error) {
	if r.EarlyManifest != nil {
		if r.TimelineRevision != 0 || r.TimelineHash != "" || len(r.Assets) != 0 || len(r.Timeline) != 0 || r.OutputProfile != "" {
			return nil, fmt.Errorf("assembly: early_manifest cannot be combined with legacy assembly fields")
		}
		manifest := *r.EarlyManifest
		if strings.TrimSpace(manifest.JobID) == "" {
			manifest.JobID = jobID
		}
		if strings.TrimSpace(manifest.JobID) != strings.TrimSpace(jobID) {
			return nil, fmt.Errorf("assembly: early_manifest.job_id must match idempotency key")
		}
		canonical, err := manifest.CanonicalContract()
		if err != nil {
			return nil, err
		}
		return &canonical, nil
	}
	if !r.SendToVelox {
		return nil, nil
	}
	if r.TimelineRevision == 0 {
		return nil, fmt.Errorf("assembly: timeline_revision is required when send_to_velox is true")
	}
	job := &AssemblyJobV1{
		ContractVersion:  ContractVersion,
		JobID:            jobID,
		TimelineRevision: r.TimelineRevision,
		TimelineHash:     r.TimelineHash,
		Dispatch:         NormalizeDispatch(ExternalDispatch{SendToVelox: true}),
		Assets:           r.Assets,
		Timeline:         r.Timeline,
		Output:           Output{ProfileID: r.OutputProfile},
		State:            StatePreparing,
	}
	if err := job.Validate(); err != nil {
		return nil, err
	}
	return job, nil
}
