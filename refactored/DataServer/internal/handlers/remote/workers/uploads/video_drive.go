package uploads

import (
	"context"
	"log"
	"time"

	"velox-server/internal/store"
)

// maybeAutoUploadDrive triggers a Drive upload if a resolved delivery_target of type "drive" exists.
// Uses the pre-resolved config from the delivery_target (PR4: target resolved at enqueue).
func maybeAutoUploadDrive(fileQ interface{ UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}) error }, driveService DriveAutoUploader, dataDir string, jobID string, uploadInfo map[string]interface{}, videoPath string, targets []store.DeliveryTarget) {
	if driveService == nil || jobID == "" || videoPath == "" {
		return
	}

	// Find the drive delivery target (PR4: resolved at enqueue time)
	var driveTarget *store.DeliveryTarget
	for i, t := range targets {
		if t.TargetType == "drive" && (t.Status == "pending" || t.Status == "scheduled") {
			driveTarget = &targets[i]
			break
		}
	}
	if driveTarget == nil {
		return
	}

	// Parse the pre-resolved config
	cfg, err := store.ParseTargetConfig(driveTarget.Config)
	if err != nil || cfg.FolderID == "" {
		log.Printf("[DRIVE] Auto-upload skipped for %s: invalid target config (%v)", jobID, err)
		return
	}

	videoName := firstNonEmptyString(
		asString(uploadInfo["video_name"]),
		asString(uploadInfo["title"]),
		cfg.VideoName,
		jobID,
	)
	subfolderName := sanitizeDriveFolderName(videoName)
	if subfolderName == "" {
		subfolderName = jobID
	}

	// Schedule delivery
	_ = fileQ.UpdateJobFields(context.Background(), jobID, map[string]interface{}{
		"drive_upload_status": "scheduled",
	})

	go func() {
		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer uploadCancel()

		dbStore := getDBStore(fileQ)
		attemptNumber := driveTarget.AttemptCount + 1
		now := time.Now().UTC().Format(time.RFC3339)

		// Record delivery attempt
		var attemptID int64
		if dbStore != nil {
			attemptID, _ = dbStore.InsertDeliveryAttempt(&store.DeliveryAttempt{
				DeliveryTargetID: driveTarget.ID,
				AttemptNumber:    attemptNumber,
				Status:           "uploading",
				StartedAt:        now,
				WorkerID:         "auto_drive",
			})
		}

		uploadResult, uploadErr := driveService.UploadVideo(uploadCtx, videoPath, subfolderName, cfg.FolderID)

		var status string
		var result store.DeliveryTargetResult
		if uploadErr != nil {
			status = "failed"
			result = store.DeliveryTargetResult{
				Success: false,
				Error:   uploadErr.Error(),
			}
			log.Printf("[DRIVE] Auto-upload failed for %s: %v", jobID, uploadErr)
		} else {
			status = "completed"
			result = store.DeliveryTargetResult{
				Success:     true,
				WebViewLink: uploadResult.WebViewLink,
				FolderLink:  uploadResult.FolderLink,
			}
		}

		resultJSON := store.MustTargetResultJSON(&result)

		// Update delivery target status
		if dbStore != nil {
			_ = dbStore.UpdateDeliveryTargetResult(driveTarget.ID, status, resultJSON)
		}

		// Update delivery attempt
		if dbStore != nil && attemptID > 0 {
			attemptStatus := "completed"
			errMsg := ""
			if uploadErr != nil {
				attemptStatus = "failed"
				errMsg = uploadErr.Error()
			}
			_ = dbStore.UpdateDeliveryAttempt(int(attemptID), attemptStatus, resultJSON, errMsg)
		}

		// DEPRECATED: Update job fields for backward compat
		update := map[string]interface{}{
			"drive_upload_status": status,
		}
		if uploadErr == nil {
			update["drive_url"] = uploadResult.WebViewLink
			update["drive_folder_url"] = uploadResult.FolderLink
			update["drive_folder_id"] = cfg.FolderID
		}
		_ = fileQ.UpdateJobFields(uploadCtx, jobID, update)

		log.Printf("[DRIVE] Auto-upload %s for %s -> %v", status, jobID, result.WebViewLink)
	}()
}
