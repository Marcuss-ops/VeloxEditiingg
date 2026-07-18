package pipelineruns

import "time"

// PipelineRun is the durable aggregate that tracks the lifecycle of a
// client-initiated generation pipeline.
//
// It is created before any remote call is made and exposes a single,
// versioned status to API clients. It does not replace the internal
// state machines of jobs, tasks, artifacts or deliveries; it projects
// an aggregated view of them.
type PipelineRun struct {
	ID                    string    `json:"id"`
	RequestID             string    `json:"request_id"`
	IdempotencyKey        string    `json:"idempotency_key"`
	UserID                string    `json:"user_id"`
	CampaignID            string    `json:"campaign_id"`
	CampaignItemID        string    `json:"campaign_item_id"`
	Status                Status    `json:"status"`
	CurrentStage          string    `json:"current_stage"`
	RemoteProvider        string    `json:"remote_provider"`
	RemoteJobID           string    `json:"remote_job_id"`
	ForwardingID          string    `json:"forwarding_id"`
	VeloxJobID            string    `json:"velox_job_id"`
	ArtifactID            string    `json:"artifact_id"`
	DeliveryID            string    `json:"delivery_id"`
	RequestedPayloadJSON  string    `json:"requested_payload_json"`
	NormalizedPayloadJSON string    `json:"normalized_payload_json"`
	ResultJSON            string    `json:"result_json"`
	ErrorCode             string    `json:"error_code"`
	ErrorMessage          string    `json:"error_message"`
	FailedStage           string    `json:"failed_stage"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
	CompletedAt           time.Time `json:"completed_at,omitempty"`
}

// Status is the aggregated pipeline_run status exposed to clients.
// It is intentionally not the same as the internal job/task/artifact/delivery
// states.
type Status string

const (
	StatusAccepted           Status = "ACCEPTED"
	StatusRemoteSubmitting   Status = "REMOTE_SUBMITTING"
	StatusRemoteQueued       Status = "REMOTE_QUEUED"
	StatusRemoteRunning      Status = "REMOTE_RUNNING"
	StatusRemoteCompleted    Status = "REMOTE_COMPLETED"
	StatusForwarding         Status = "FORWARDING"
	StatusWorkerQueued       Status = "WORKER_QUEUED"
	StatusRendering          Status = "RENDERING"
	StatusArtifactProcessing Status = "ARTIFACT_PROCESSING"
	StatusArtifactReady      Status = "ARTIFACT_READY"
	StatusDeliveryPending    Status = "DELIVERY_PENDING"
	StatusDelivering         Status = "DELIVERING"
	StatusScheduled          Status = "SCHEDULED"
	StatusPublished          Status = "PUBLISHED"
	StatusCompleted          Status = "COMPLETED"
	StatusFailed             Status = "FAILED"
	StatusCancelled          Status = "CANCELLED"
)

// Terminal returns true if the status represents a final state.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}
