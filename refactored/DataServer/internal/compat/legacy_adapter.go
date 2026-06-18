// Package compat provides adapters that derive legacy job fields from the new
// domain model (artifacts + job_deliveries). These adapters are used by
// endpoints that still expose the old flat fields, but never accept them
// in writes. New code should use the CleanJobView or *Job struct directly.
package compat

import (
	"context"

	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// LegacyJobView assembles the legacy flat fields from the canonical sources.
type LegacyJobView struct {
	JobID               string `json:"job_id"`
	Status              string `json:"status"`

	// Derived from artifacts (primary READY artifact)
	VideoUploaded       bool   `json:"video_uploaded,omitempty"`
	MasterVideoPath     string `json:"master_video_path,omitempty"`
	VideoSHA256         string `json:"video_sha256,omitempty"`
	PrimaryArtifactID   string `json:"artifact_id,omitempty"`
	OutputVideoID       string `json:"output_video_id,omitempty"`
	ArtifactStatus      string `json:"artifact_status,omitempty"`

	// Derived from job_deliveries (succeeded Drive delivery)
	DriveURL            string `json:"drive_url,omitempty"`
	DriveFolderID       string `json:"drive_folder_id,omitempty"`

	// Derived from job_deliveries (succeeded YouTube delivery)
	YouTubeURL          string `json:"youtube_url,omitempty"`
	YouTubeVideoID      string `json:"youtube_video_id,omitempty"`
	YouTubeChannelID    string `json:"youtube_channel_id,omitempty"`
	YouTubeChannelName  string `json:"youtube_channel_name,omitempty"`
}

// AssembleLegacyJobView builds a LegacyJobView from a job, its artifacts,
// and its job_deliveries. Returns nil if the job is nil.
func AssembleLegacyJobView(ctx context.Context, dbStore *store.SQLiteStore, job *queue.Job) (*LegacyJobView, error) {
	if job == nil {
		return nil, nil
	}
	if dbStore == nil {
		return &LegacyJobView{JobID: job.JobID, Status: mapCanonicalStatus(job.Status)}, nil
	}

	view := &LegacyJobView{
		JobID:  job.JobID,
		Status: mapCanonicalStatus(job.Status),
	}

	// Load artifacts for this job
	artifacts, err := dbStore.GetArtifactsByJob(job.JobID, 5)
	if err != nil {
		// Non-fatal: return view with just job fields
		return view, nil
	}

	// Find primary READY artifact (highest priority: video type)
	var primaryArtifact *store.Artifact
	for i, a := range artifacts {
		if a.Status == "READY" {
			if a.Type == "video" || (primaryArtifact == nil) {
				primaryArtifact = &artifacts[i]
			}
		}
	}

	if primaryArtifact != nil {
		view.VideoUploaded = true
		view.MasterVideoPath = primaryArtifact.LocalPath
		if primaryArtifact.LocalPath == "" {
			view.MasterVideoPath = primaryArtifact.StorageKey
		}
		view.VideoSHA256 = primaryArtifact.SHA256
		view.PrimaryArtifactID = primaryArtifact.ID
		view.ArtifactStatus = primaryArtifact.Status

		// Load job_deliveries for this artifact
		deliveries, err := dbStore.ListJobDeliveriesByJob(job.JobID)
		if err == nil {
			for _, d := range deliveries {
				if d.ArtifactID != primaryArtifact.ID {
					continue
				}
				// Resolve destination to find the provider type
				dest, destErr := dbStore.GetDeliveryDestination(ctx, d.DestinationID)
				if destErr != nil {
					continue
				}
				if d.Status != "SUCCEEDED" && d.RemoteURL == "" {
					continue
				}
				switch dest.Provider {
				case "drive", "gdrive":
					if d.RemoteURL != "" {
						view.DriveURL = d.RemoteURL
					}
					if dest.FolderID != "" {
						view.DriveFolderID = dest.FolderID
					}
				case "youtube":
					view.YouTubeURL = d.RemoteURL
					view.YouTubeVideoID = d.RemoteID
					if dest.ChannelID != "" {
						view.YouTubeChannelID = dest.ChannelID
					}
					if dest.Name != "" {
						view.YouTubeChannelName = dest.Name
					}
				}
			}
		}
	}

	return view, nil
}

// mapCanonicalStatus translates queue statuses to the legacy COMPLETED/ERROR/etc.
func mapCanonicalStatus(s queue.JobStatus) string {
	switch s {
	case queue.StatusPending:
		return "PENDING"
	case queue.StatusRunning, queue.StatusLeased:
		return "PROCESSING"
	case queue.StatusSucceeded:
		return "COMPLETED"
	case queue.StatusFailed:
		return "FAILED"
	case queue.StatusRetryWait:
		return "RETRY_WAIT"
	case queue.StatusCancelled:
		return "CANCELLED"
	default:
		return string(s)
	}
}

// CleanResultJSON builds a result_json map with only canonical fields
// (no operational fields like worker_id, lease_id, etc.).
// Artifact paths are NOT included — they belong on the artifacts table.
func CleanResultJSON(job *queue.Job, primaryArtifactID string) map[string]interface{} {
	if job == nil {
		return nil
	}
	result := map[string]interface{}{
		"primary_artifact_id": primaryArtifactID,
	}
	return result
}
