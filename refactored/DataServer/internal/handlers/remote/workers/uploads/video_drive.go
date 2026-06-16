package uploads

import (
	"context"
	"log"
	"strings"
	"time"

	driveapi "velox-server/internal/integrations/drive"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
)

func maybeAutoUploadDrive(fileQ *queue.FileQueue, driveService *driveapi.Service, dataDir string, jobID string, uploadInfo map[string]interface{}, videoPath string) {
	if fileQ == nil || driveService == nil || strings.TrimSpace(jobID) == "" || strings.TrimSpace(videoPath) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	job, err := fileQ.GetJobAsMap(ctx, jobID)
	if err != nil || job == nil {
		log.Printf("[DRIVE] Auto-upload skipped for %s: job not found (%v)", jobID, err)
		return
	}

	if shouldSkipDriveUpload(job) {
		return
	}

	folderRef := firstNonEmptyString(
		asString(uploadInfo["drive_output_folder"]),
		asString(job["drive_output_folder"]),
		asStringFromSlot(job, "drive_output_folder"),
	)
	if folderRef == "" {
		return
	}

	rootFolderID := enqueue.ResolveDriveOutputFolderReference(dataDir, folderRef)
	if rootFolderID == "" {
		log.Printf("[DRIVE] Auto-upload skipped for %s: could not resolve folder %q", jobID, folderRef)
		return
	}

	videoName := firstNonEmptyString(
		asString(uploadInfo["video_name"]),
		asString(uploadInfo["title"]),
		asString(job["video_name"]),
		asString(job["title"]),
		jobID,
	)
	subfolderName := sanitizeDriveFolderName(videoName)
	if subfolderName == "" {
		subfolderName = jobID
	}

	_ = fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
		"drive_upload_status": "scheduled",
	})

	go func() {
		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer uploadCancel()

		uploadResult, uploadErr := driveService.UploadVideo(uploadCtx, videoPath, subfolderName, rootFolderID)
		if uploadErr != nil {
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"drive_upload_status": "failed",
				"last_drive_upload_result": map[string]interface{}{
					"success":     false,
					"error":       uploadErr.Error(),
					"folder_ref":  folderRef,
					"folder_id":   rootFolderID,
					"uploaded_at": time.Now().UTC().Format(time.RFC3339),
				},
			})
			log.Printf("[DRIVE] Auto-upload failed for %s: %v", jobID, uploadErr)
			return
		}

		update := map[string]interface{}{
			"drive_upload_status": "completed",
			"drive_url":           uploadResult.WebViewLink,
			"drive_folder_url":    uploadResult.FolderLink,
			"drive_folder_id":     rootFolderID,
			"last_drive_upload_result": map[string]interface{}{
				"success":     true,
				"drive_url":   uploadResult.WebViewLink,
				"folder_url":  uploadResult.FolderLink,
				"folder_id":   rootFolderID,
				"subfolder":   subfolderName,
				"uploaded_at": time.Now().UTC().Format(time.RFC3339),
			},
		}
		if err := fileQ.UpdateJobFields(uploadCtx, jobID, update); err != nil {
			log.Printf("[DRIVE] Auto-upload persisted with warning for %s: %v", jobID, err)
		}
		log.Printf("[DRIVE] Auto-upload completed for %s -> %s", jobID, uploadResult.WebViewLink)
	}()
}

func shouldSkipDriveUpload(job map[string]interface{}) bool {
	if job == nil {
		return true
	}
	if strings.TrimSpace(asString(job["drive_url"])) != "" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["drive_upload_status"])), "scheduled") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["drive_upload_status"])), "uploading") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["drive_upload_status"])), "completed") {
		return true
	}
	if result, ok := job["last_drive_upload_result"].(map[string]interface{}); ok {
		if ok, _ := result["success"].(bool); ok {
			return true
		}
	}
	return false
}

func sanitizeDriveFolderName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == ' ':
			b.WriteRune('_')
		default:
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "drive_upload"
	}
	return out
}
