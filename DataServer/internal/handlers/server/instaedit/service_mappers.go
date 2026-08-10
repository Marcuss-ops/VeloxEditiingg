package instaedit

import (
	"fmt"
	"strings"
	"time"

	deliverydomain "velox-server/internal/deliveries"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// Mapping helpers keep storage rows and domain records behind the InstaEdit response views.

func mapJob(row map[string]any, workspaceID int64) jobResponse {
	j := jobResponse{
		ID:           asString(row["job_id"]),
		WorkspaceID:  workspaceID,
		ProjectID:    asString(row["project_id"]),
		RenderStatus: asString(row["status"]),
		CreatedAt:    parseTime(row["created_at"]),
		UpdatedAt:    parseTime(row["updated_at"]),
	}
	if j.RenderStatus == "" {
		j.RenderStatus = "PENDING"
	}
	j.PublicationStatus = "pending"
	j.OverallStatus = strings.ToLower(j.RenderStatus)
	return j
}

func mapJobWithDeliveries(row map[string]any, workspaceID int64, deliveries []deliveryResponse) jobResponse {
	j := mapJob(row, workspaceID)
	if len(deliveries) == 0 {
		return j
	}
	allSucceeded := true
	anyFailed := false
	anyActive := false
	for _, d := range deliveries {
		// The response DTO remains string-based at the HTTP boundary. Parse
		// it into the delivery domain type before applying publication rules;
		// legacy PUBLISHED/COMPLETED aliases are accepted only here.
		deliveryStatus := deliverydomain.DeliveryStatus(strings.ToUpper(d.Status))
		switch deliveryStatus {
		case deliverydomain.DeliverySucceeded, deliverydomain.DeliveryStatus("PUBLISHED"), deliverydomain.DeliveryStatus("COMPLETED"):
		default:
			allSucceeded = false
		}
		switch deliveryStatus {
		case deliverydomain.DeliveryFailed, deliverydomain.DeliveryBlockedAuth, deliverydomain.DeliveryCancelled:
			anyFailed = true
		case deliverydomain.DeliveryRunning, deliverydomain.DeliveryRetryWait, deliverydomain.DeliveryStatus("CLAIMED"), deliverydomain.DeliveryPending:
			anyActive = true
		}
	}
	switch {
	case allSucceeded:
		j.PublicationStatus = "published"
	case anyFailed:
		j.PublicationStatus = "failed"
	case anyActive:
		j.PublicationStatus = "publishing"
	default:
		j.PublicationStatus = "pending"
	}
	renderStatus := jobs.JobStatus(strings.ToUpper(j.RenderStatus))
	// COMPLETED is a legacy aggregate response value, not a JobStatus.
	// Keep accepting it at this HTTP projection boundary without making it
	// part of the canonical job state machine.
	if renderStatus == jobs.StatusSucceeded || strings.ToUpper(j.RenderStatus) == "COMPLETED" {
		j.OverallStatus = j.PublicationStatus
	} else {
		j.OverallStatus = strings.ToLower(j.RenderStatus)
	}
	return j
}

func mapWorker(row map[string]any, workspaceID int64) workerResponse {
	// Status prefers the canonical DERIVED connection_status key when the
	// reader surfaces it (registry-backed readers); legacy store rows carry
	// the retired free-form `status` key, which remains the fallback so
	// already-persisted snapshots keep rendering until re-upserted.
	status := asString(row["connection_status"])
	if status == "" {
		status = asString(row["status"])
	}
	w := workerResponse{
		ID:          asString(row["worker_id"]),
		WorkspaceID: workspaceID,
		Status:      status,
	}
	if w.ID == "" {
		w.ID = asString(row["id"])
	}
	if metrics, ok := row["metrics"].(map[string]any); ok {
		if cpu, ok := metrics["cpu_count"].(int64); ok {
			w.CPU = int(cpu)
		} else if cpu, ok := metrics["cpu_count"].(float64); ok {
			w.CPU = int(cpu)
		}
		if ram, ok := metrics["ram_bytes"].(int64); ok {
			w.RAMMB = int(ram / 1024 / 1024)
		} else if ram, ok := metrics["ram_bytes"].(float64); ok {
			w.RAMMB = int(ram / 1024 / 1024)
		}
		if disk, ok := metrics["disk_free_bytes"].(int64); ok {
			w.DiskGB = int(disk / 1024 / 1024 / 1024)
		} else if disk, ok := metrics["disk_free_bytes"].(float64); ok {
			w.DiskGB = int(disk / 1024 / 1024 / 1024)
		}
		w.GPU = asString(metrics["gpu"])
	}
	return w
}

func mapAsset(a *store.AssetRecord, workspaceID int64) assetResponse {
	return assetResponse{
		ID:          a.AssetID,
		WorkspaceID: workspaceID,
		SHA256:      a.SHA256,
		SizeBytes:   a.SizeBytes,
		MimeType:    a.MimeType,
		DownloadURL: "",
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	if s, ok := v.([]byte); ok {
		return string(s)
	}
	return fmt.Sprintf("%v", v)
}

func parseTime(v any) time.Time {
	s := asString(v)
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}
