package uploads

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	ytservice "velox-server/internal/integrations/youtube"
	"velox-server/internal/queue"
)

// maybeAutoUploadYouTube checks job metadata and if conditions are met,
// triggers a YouTube auto-upload for the completed video.
func maybeAutoUploadYouTube(fileQ *queue.FileQueue, youtubeService *ytservice.Service, jobID string, uploadInfo map[string]interface{}, videoPath string) {
	if fileQ == nil || youtubeService == nil || strings.TrimSpace(jobID) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := fileQ.GetJobAsMap(ctx, jobID)
	if err != nil || job == nil {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: job not found (%v)", jobID, err)
		return
	}

	if shouldSkipYouTubeUpload(job) {
		return
	}

	groupName := firstNonEmptyString(
		asString(uploadInfo["youtube_group"]),
		asString(job["youtube_group"]),
		asStringFromSlot(job, "youtube_group"),
	)
	if groupName == "" {
		return
	}

	language := firstNonEmptyString(
		asString(uploadInfo["audio_language_for_srt"]),
		asString(uploadInfo["target_language"]),
		asString(uploadInfo["language"]),
		asString(uploadInfo["voice_language"]),
		asString(uploadInfo["lang"]),
		asString(job["audio_language_for_srt"]),
		asString(job["target_language"]),
		asString(job["language"]),
		asString(job["voice_language"]),
		asString(job["lang"]),
		asStringFromSlot(job, "audio_language_for_srt"),
		asStringFromSlot(job, "target_language"),
		asStringFromSlot(job, "language"),
		asStringFromSlot(job, "voice_language"),
		asStringFromSlot(job, "lang"),
	)
	if language == "" {
		language = "en"
	}

	if strings.TrimSpace(videoPath) == "" {
		videoPath = firstNonEmptyString(
			asString(uploadInfo["master_video_path"]),
			asString(uploadInfo["result_path_worker"]),
			asString(uploadInfo["result_path"]),
			asString(job["master_video_path"]),
			asString(job["result_path_worker"]),
			asString(job["result_path"]),
		)
	}
	if strings.TrimSpace(videoPath) == "" {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: missing video path", jobID)
		return
	}
	if _, statErr := os.Stat(videoPath); statErr != nil {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: video not found at %s (%v)", jobID, videoPath, statErr)
		return
	}

	title := firstNonEmptyString(
		asString(uploadInfo["video_name"]),
		asString(uploadInfo["title"]),
		asString(job["video_name"]),
		asString(job["title"]),
	)
	if title == "" {
		title = jobID
	}

	description := firstNonEmptyString(
		asString(uploadInfo["script_text"]),
		asString(uploadInfo["source_text"]),
		asString(job["script_text"]),
		asString(job["source_text"]),
	)

	privacy := firstNonEmptyString(
		asString(uploadInfo["privacy_status"]),
		asString(uploadInfo["privacy"]),
		asString(job["privacy_status"]),
		asString(job["privacy"]),
	)
	if privacy == "" {
		privacy = "private"
	}

	tags := mergeStringSlices(
		asStringSlice(uploadInfo["tags"]),
		asStringSlice(job["tags"]),
		asStringSliceFromSlot(job, "tags"),
	)

	jobRunID := firstNonEmptyString(
		asString(uploadInfo["job_run_id"]),
		asString(uploadInfo["run_id"]),
		asString(job["job_run_id"]),
		asString(job["run_id"]),
	)

	now := time.Now().UTC().Format(time.RFC3339)
	_ = fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
		"youtube_upload_status": "scheduled",
		"youtube_upload_at":     now,
	})

	go func() {
		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer uploadCancel()

		channel, chErr := youtubeService.ResolveChannelByLanguage(groupName, language)
		if chErr != nil {
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"youtube_upload_status": "failed",
				"last_youtube_upload_result": map[string]interface{}{
					"success":     false,
					"error":       chErr.Error(),
					"group":       groupName,
					"language":    language,
					"uploaded_at": time.Now().UTC().Format(time.RFC3339),
				},
			})
			log.Printf("[UPLOAD] YouTube auto-upload failed for %s: resolve channel: %v", jobID, chErr)
			return
		}

		health, healthErr := youtubeService.HealthCheck(uploadCtx, channel.ID)
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
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"youtube_upload_status": "needs_reauth",
				"last_youtube_upload_result": map[string]interface{}{
					"success":      false,
					"error":        errMsg,
					"group":        groupName,
					"language":     language,
					"channel_id":   channel.ID,
					"channel_name": channel.Name,
					"job_run_id":   jobRunID,
					"uploaded_at":  time.Now().UTC().Format(time.RFC3339),
				},
			})
			log.Printf("[UPLOAD] YouTube auto-upload deferred for %s: %s", jobID, errMsg)
			return
		}

		result, uploadErr := youtubeService.UploadVideo(uploadCtx, channel.ID, videoPath, ytservice.UploadConfig{
			Title:         title,
			Description:   description,
			Tags:          tags,
			PrivacyStatus: privacy,
			ChannelID:     channel.ID,
			ChannelName:   channel.Name,
		})
		if uploadErr != nil {
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"youtube_upload_status": "failed",
				"last_youtube_upload_result": map[string]interface{}{
					"success":      false,
					"error":        uploadErr.Error(),
					"group":        groupName,
					"language":     language,
					"channel_id":   channel.ID,
					"channel_name": channel.Name,
					"job_run_id":   jobRunID,
					"uploaded_at":  time.Now().UTC().Format(time.RFC3339),
				},
			})
			log.Printf("[UPLOAD] YouTube auto-upload failed for %s: %v", jobID, uploadErr)
			return
		}

		update := map[string]interface{}{
			"youtube_upload_status": "completed",
			"youtube_url":           result.YouTubeURL,
			"youtube_video_id":      result.VideoID,
			"youtube_channel_id":    channel.ID,
			"youtube_channel_name":  channel.Name,
			"youtube_channel_language": channel.Language,
			"last_youtube_upload_result": map[string]interface{}{
				"success":      true,
				"youtube_url":  result.YouTubeURL,
				"video_id":     result.VideoID,
				"channel_id":   channel.ID,
				"channel_name": channel.Name,
				"group":        groupName,
				"language":     language,
				"job_run_id":   jobRunID,
				"uploaded_at":  time.Now().UTC().Format(time.RFC3339),
				"privacy":      privacy,
			},
		}
		if err := fileQ.UpdateJobFields(uploadCtx, jobID, update); err != nil {
			log.Printf("[UPLOAD] YouTube auto-upload persisted with warning for %s: %v", jobID, err)
		}
		log.Printf("[UPLOAD] YouTube auto-upload completed for %s -> %s", jobID, result.YouTubeURL)
	}()
}

func shouldSkipYouTubeUpload(job map[string]interface{}) bool {
	if job == nil {
		return true
	}
	if strings.TrimSpace(asString(job["youtube_url"])) != "" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["youtube_upload_status"])), "scheduled") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["youtube_upload_status"])), "uploading") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["youtube_upload_status"])), "completed") {
		return true
	}
	if result, ok := job["last_youtube_upload_result"].(map[string]interface{}); ok {
		if success, _ := result["success"].(bool); success {
			return true
		}
	}
	return false
}
