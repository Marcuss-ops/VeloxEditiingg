package remoteengine

import (
	"encoding/json"
	"time"
)

// remoteJobResponse is the remote engine's raw job response wrapper.
// The remote engine returns {"job": { ... }} at the top level.
type remoteJobResponse struct {
	Job remoteJobEnvelope `json:"job"`
}

type remoteJobEnvelope struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Status    string                 `json:"status"`
	Progress  int                    `json:"progress"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
	Result    map[string]interface{} `json:"result,omitempty"`
	Error     string                 `json:"error,omitempty"`
	CreatedAt string                 `json:"created_at,omitempty"`
	UpdatedAt string                 `json:"updated_at,omitempty"`
}

// parseRemoteJobResponse parses the raw remote engine job response into a
// PipelineStatusResponse. It handles the {"job": {...}} wrapper and maps
// the envelope fields to the public response shape.
func parseRemoteJobResponse(respBody []byte) (*PipelineStatusResponse, error) {
	var wrapper remoteJobResponse
	if err := json.Unmarshal(respBody, &wrapper); err != nil {
		return nil, ClassifyDecodeError(err, string(respBody))
	}

	j := wrapper.Job
	status := j.Status
	ok := status == "completed" || status == "running" || status == "queued"

	var createdAt, updatedAt time.Time
	if j.CreatedAt != "" {
		createdAt, _ = time.Parse(time.RFC3339, j.CreatedAt)
	}
	if j.UpdatedAt != "" {
		updatedAt, _ = time.Parse(time.RFC3339, j.UpdatedAt)
	}

	// Use Result (output fields) falling back to Payload (input params) if Result is nil.
	resultData := j.Result
	if resultData == nil {
		resultData = j.Payload
	}

	return &PipelineStatusResponse{
		OK:        ok,
		TraceID:   j.ID,
		Status:    j.Status,
		Progress:  float64(j.Progress),
		Result:    resultData,
		Error:     j.Error,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}
