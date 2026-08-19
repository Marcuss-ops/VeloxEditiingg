package instaedit

import (
	"encoding/json"
	"time"
)

// CreateJobCmd is the typed input for Service.CreateJob.
type CreateJobCmd struct {
	WorkspaceID     int64
	ProjectID       string
	ContractVersion string
	IdempotencyKey  string
	RenderSpec      json.RawMessage
	Destinations    []CreateDestinationCmd
	PublishAt       string
	Target          *PublicationTargetCmd
	Publications    json.RawMessage
	RenderOnly      bool
}

// PublicationTargetCmd is payload metadata describing the logical social
// target selected in InstaEdit Social. Velox still delivers through opaque
// destination IDs; these fields are carried for auditability and UI/job
// inspection, never used as credentials or as a replacement for destination
// resolution.
type PublicationTargetCmd struct {
	Type        string   `json:"type"`
	ChannelID   string   `json:"channel_id,omitempty"`
	ChannelName string   `json:"channel_name,omitempty"`
	GroupID     int64    `json:"group_id,omitempty"`
	GroupName   string   `json:"group_name,omitempty"`
	ChannelIDs  []string `json:"channel_ids,omitempty"`
}

// CreateDestinationCmd is a single destination inside CreateJobCmd.
type CreateDestinationCmd struct {
	ExternalDestinationID string
	PublicationID         string
	VariantID             string
	Metadata              json.RawMessage
}

// createJobRequest is the HTTP request body for POST /jobs.
type createJobRequest struct {
	ContractVersion string                `json:"contract_version"`
	IdempotencyKey  string                `json:"idempotency_key"`
	ProjectID       string                `json:"project_id"`
	RenderSpec      json.RawMessage       `json:"render_spec"`
	DeliveryPlan    deliveryPlanReq       `json:"delivery_plan"`
	PublishAt       string                `json:"publish_at,omitempty"`
	Target          *PublicationTargetCmd `json:"target,omitempty"`
	Publications    json.RawMessage       `json:"publications,omitempty"`
	RenderOnly      bool                  `json:"render_only,omitempty"`

	// Canonical velox.job.v1 fields are accepted at the transport boundary
	// so the InstaEdit client can use the provider-neutral job contract.
	// The editor service currently derives execution from render_spec;
	// these fields remain available for the canonical validator/registry.
	JobType         string          `json:"job_type,omitempty"`
	TemplateID      string          `json:"template_id,omitempty"`
	TemplateVersion int             `json:"template_version,omitempty"`
	VideoName       string          `json:"video_name,omitempty"`
	Spec            json.RawMessage `json:"spec,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
}

// deliveryPlanReq is the HTTP wrapper for delivery destinations.
type deliveryPlanReq struct {
	Destinations []deliveryDestinationReq `json:"destinations"`
}

// deliveryDestinationReq is the HTTP wrapper for a single destination.
type deliveryDestinationReq struct {
	ExternalDestinationID string          `json:"external_destination_id"`
	PublicationID         string          `json:"publication_id,omitempty"`
	Metadata              json.RawMessage `json:"metadata"`
}

// jobResponse is the InstaEdit view of a Job.
type jobResponse struct {
	ID                string    `json:"id"`
	Duplicate         bool      `json:"duplicate,omitempty"`
	WorkspaceID       int64     `json:"workspace_id"`
	ProjectID         string    `json:"project_id,omitempty"`
	RenderStatus      string    `json:"render_status"`
	PublicationStatus string    `json:"publication_status"`
	OverallStatus     string    `json:"overall_status"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// deliveryResponse is the InstaEdit view of a job delivery.
type deliveryResponse struct {
	PublicationID         string `json:"publication_id,omitempty"`
	ExternalDestinationID string `json:"external_destination_id"`
	SocialDeliveryID      string `json:"social_delivery_id"`
	Status                string `json:"status"`
	Phase                 string `json:"phase,omitempty"`
	Attempt               int    `json:"attempt,omitempty"`
	NextRetryAt           string `json:"next_retry_at,omitempty"`
	LastErrorCode         string `json:"last_error_code,omitempty"`
	LastErrorMessage      string `json:"last_error_message,omitempty"`
	RetryFrom             string `json:"retry_from,omitempty"`
	PlatformMediaID       string `json:"platform_media_id,omitempty"`
	PlatformURL           string `json:"platform_url,omitempty"`
}

// jobDetailResponse combines a job with its deliveries.
type jobDetailResponse struct {
	Job        jobResponse        `json:"job"`
	Deliveries []deliveryResponse `json:"deliveries"`
}

// workerResponse is the InstaEdit view of a worker snapshot.
type workerResponse struct {
	ID          string `json:"id"`
	WorkspaceID int64  `json:"workspace_id"`
	Status      string `json:"status"`
	CPU         int    `json:"cpu,omitempty"`
	RAMMB       int    `json:"ram_mb,omitempty"`
	GPU         string `json:"gpu,omitempty"`
	DiskGB      int    `json:"disk_gb,omitempty"`
}

// assetResponse is the InstaEdit view of an workspace asset.
type assetResponse struct {
	ID          string `json:"id"`
	WorkspaceID int64  `json:"workspace_id"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	MimeType    string `json:"mime_type"`
	DownloadURL string `json:"download_url,omitempty"`
}

// listJobsResponse is the payload for GET /jobs.
type listJobsResponse struct {
	Jobs []jobResponse `json:"jobs"`
}

// listWorkersResponse is the payload for GET /workers.
type listWorkersResponse struct {
	Workers []workerResponse `json:"workers"`
}

// listDeliveriesResponse is the payload for GET /jobs/:id/deliveries.
type listDeliveriesResponse struct {
	Deliveries []deliveryResponse `json:"deliveries"`
}
