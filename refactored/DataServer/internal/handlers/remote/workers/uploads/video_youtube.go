package uploads

import (
	"context"
	"log"
	"os"
	"time"

	ytservice "velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// maybeAutoUploadYouTube triggers a YouTube upload if a resolved delivery_target of type "youtube" exists.
// Uses the pre-resolved config from the delivery_target (PR4: target resolved at enqueue).
// Falls back to resolving the channel at upload time if channel_id is not in the config.
func maybeAutoUploadYouTube(fileQ interface{ UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}) error }, youtubeService YouTubeAutoUploader, jobID string, uploadInfo map[string]interface{}, videoPath string, targets []store.DeliveryTarget) {
	if youtubeService == nil || jobID == "" {
		return
	}

	// Find the youtube delivery target (PR4: resolved at enqueue time)
	var ytTarget *store.DeliveryTarget
	for i, t := range targets {
		if t.TargetType == "youtube" && (t.Status == "pending" || t.Status == "scheduled") {
			ytTarget = &targets[i]
			break
		}
	}
	if ytTarget == nil {
		return
	}

	// Parse the pre-resolved config (may have channel_id if resolved at enqueue)
	cfg, err := store.ParseTargetConfig(ytTarget.Config)
	if err != nil {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: invalid target config (%v)", jobID, err)
		return
	}

	// Resolve video path
	if videoPath == "" {
		videoPath = firstNonEmptyString(
			asString(uploadInfo["master_video_path"]),
			asString(uploadInfo["result_path_worker"]),
			asString(uploadInfo["result_path"]),
		)
	}
	if videoPath == "" {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: missing video path", jobID)
		return
	}
	if _, statErr := os.Stat(videoPath); statErr != nil {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: video not found at %s (%v)", jobID, videoPath, statErr)
		return
	}

	// Title
	title := firstNonEmptyString(
		asString(uploadInfo["video_name"]),
		asString(uploadInfo["title"]),
		cfg.Title,
		jobID,
	)

	// Schedule delivery
	_ = fileQ.UpdateJobFields(context.Background(), jobID, map[string]interface{}{
		"youtube_upload_status": "scheduled",
	})

	go func() {
		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer uploadCancel()

		dbStore := getDBStore(fileQ)
		attemptNumber := ytTarget.AttemptCount + 1
		now := time.Now().UTC().Format(time.RFC3339)

		// Record delivery attempt
		var attemptID int64
		if dbStore != nil {
			attemptID, _ = dbStore.InsertDeliveryAttempt(&store.DeliveryAttempt{
				DeliveryTargetID: ytTarget.ID,
				AttemptNumber:    attemptNumber,
				Status:           "uploading",
				StartedAt:        now,
				WorkerID:         "auto_youtube",
			})
		}

		// Resolve channel (may already be in config, otherwise resolve at upload time)
		var channelID, channelName string
		if cfg.ChannelID != "" {
			channelID = cfg.ChannelID
			channelName = cfg.ChannelName
		} else {
			// Fallback: resolve channel at upload time (for targets created without pre-resolution)
			groupName := cfg.GroupName
			language := cfg.Language
			if groupName == "" {
				// Try to extract from original upload info
				groupName = firstNonEmptyString(
					asString(uploadInfo["youtube_group"]),
				)
			}
			if language == "" {
				language = "en"
			}
			if groupName != "" {
				channel, chErr := youtubeService.ResolveChannelByLanguage(groupName, language)
				if chErr != nil {
					log.Printf("[UPLOAD] YouTube auto-upload failed for %s: resolve channel: %v", jobID, chErr)
					failDelivery(dbStore, ytTarget.ID, attemptID, chErr.Error(), "needs_reauth")
					_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
						"youtube_upload_status": "needs_reauth",
					})
					return
				}
				channelID = channel.ID
				channelName = channel.Name
			}
		}

		if channelID == "" {
			log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: no channel resolved", jobID)
			return
		}

		// Health check
		health, healthErr := youtubeService.HealthCheck(uploadCtx, channelID)
		if healthErr != nil {
			health = map[string]interface{}{
				"ok":    false,
				"error": healthErr.Error(),
			}
		}
		if ok, _ := health["ok"].(bool); !ok {
			errMsg, _ := health["error"].(string)
			if errMsg == "" {
				errMsg = "YouTube channel authentication is not ready"
			}
			failDelivery(dbStore, ytTarget.ID, attemptID, errMsg, "needs_reauth")
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"youtube_upload_status": "needs_reauth",
			})
			log.Printf("[UPLOAD] YouTube auto-upload deferred for %s: %s", jobID, errMsg)
			return
		}

		// Upload
		tags := cfg.Tags
		privacy := cfg.Privacy
		if privacy == "" {
			privacy = "private"
		}
		description := cfg.Description

		result, uploadErr := youtubeService.UploadVideo(uploadCtx, channelID, videoPath, ytservice.UploadConfig{
			Title:         title,
			Description:   description,
			Tags:          tags,
			PrivacyStatus: privacy,
			ChannelID:     channelID,
			ChannelName:   channelName,
		})

		var status string
		var targetResult store.DeliveryTargetResult
		if uploadErr != nil {
			status = "failed"
			targetResult = store.DeliveryTargetResult{
				Success: false,
				Error:   uploadErr.Error(),
			}
			log.Printf("[UPLOAD] YouTube auto-upload failed for %s: %v", jobID, uploadErr)
		} else {
			status = "completed"
			targetResult = store.DeliveryTargetResult{
				Success: true,
				URL:     result.YouTubeURL,
				VideoID: result.VideoID,
			}
		}

		resultJSON := store.MustTargetResultJSON(&targetResult)

		// Update delivery target
		if dbStore != nil {
			_ = dbStore.UpdateDeliveryTargetResult(ytTarget.ID, status, resultJSON)
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
			"youtube_upload_status": status,
		}
		if uploadErr == nil {
			update["youtube_url"] = result.YouTubeURL
			update["youtube_video_id"] = result.VideoID
			update["youtube_channel_id"] = channelID
			update["youtube_channel_name"] = channelName
		}
		_ = fileQ.UpdateJobFields(uploadCtx, jobID, update)

		log.Printf("[UPLOAD] YouTube auto-upload %s for %s -> %v", status, jobID, targetResult.URL)
	}()
}

// failDelivery records a failed delivery attempt and updates the target status.
func failDelivery(dbStore *store.SQLiteStore, targetID int, attemptID int64, errMsg, status string) {
	if dbStore == nil {
		return
	}
	result := store.MustTargetResultJSON(&store.DeliveryTargetResult{
		Success: false,
		Error:   errMsg,
	})
	_ = dbStore.UpdateDeliveryTargetResult(targetID, status, result)
	if attemptID > 0 {
		_ = dbStore.UpdateDeliveryAttempt(int(attemptID), "failed", result, errMsg)
	}
}
