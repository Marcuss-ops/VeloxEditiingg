package assembly

import "fmt"

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
}

// Normalize converts the public convenience shape into the canonical typed
// assembly job. A disabled request intentionally produces no job so callers
// can treat send_to_velox=false as a no-op without manufacturing invalid
// control-plane state.
func (r ExternalAssemblyRequest) Normalize(jobID string) (*AssemblyJobV1, error) {
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
