package instaedit

import (
	"encoding/json"
	"time"
)

// CreateJobCmd is the typed input for Service.CreateJob.
type CreateJobCmd struct {
	WorkspaceID  int64
	ProjectID    string
	RenderSpec   json.RawMessage
	Destinations []CreateDestinationCmd
}

// CreateDestinationCmd is a single destination inside CreateJobCmd.
type CreateDestinationCmd struct {
	ExternalDestinationID string
	Metadata              json.RawMessage
}

// createJobRequest is the HTTP request body for POST /jobs.
type createJobRequest struct {
	ProjectID    string          `json:"project_id"`
	RenderSpec   json.RawMessage `json:"render_spec"`
	DeliveryPlan deliveryPlanReq `json:"delivery_plan"`
}

// deliveryPlanReq is the HTTP wrapper for delivery destinations.
type deliveryPlanReq struct {
	Destinations []deliveryDestinationReq `json:"destinations"`
}

// deliveryDestinationReq is the HTTP wrapper for a single destination.
type deliveryDestinationReq struct {
	ExternalDestinationID string          `json:"external_destination_id"`
	Metadata              json.RawMessage `json:"metadata"`
}

// jobResponse is the InstaEdit view of a Job.
type jobResponse struct {
	ID           string    `json:"id"`
	WorkspaceID  int64     `json:"workspace_id"`
	ProjectID    string    `json:"project_id,omitempty"`
	RenderStatus string    `json:"render_status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// deliveryResponse is the InstaEdit view of a job delivery.
type deliveryResponse struct {
	ExternalDestinationID string `json:"external_destination_id"`
	SocialDeliveryID      string `json:"social_delivery_id"`
	Status                string `json:"status"`
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
