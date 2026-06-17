package enqueue

import (
	"log"

	"velox-server/internal/store"
)

// CreateDeliveryTargetsForJob creates delivery_target rows for a newly enqueued job.
// Inspects the normalized payload for drive_output_folder and youtube_group,
// and creates resolved delivery targets for each.
// Called after the job is persisted to the queue.
func CreateDeliveryTargetsForJob(dbStore *store.SQLiteStore, jobID string, normalized map[string]interface{}) {
	if dbStore == nil || jobID == "" || normalized == nil {
		return
	}

	// ── Drive target ──
	if driveFolderID := firstString(normalized, "drive_output_folder"); driveFolderID != "" {
		cfg := store.DeliveryTargetConfig{
			FolderID:  driveFolderID,
			VideoName: firstString(normalized, "video_name", "title"),
			ProjectID: firstString(normalized, "project_id"),
		}
		target := &store.DeliveryTarget{
			JobID:      jobID,
			TargetType: "drive",
			Status:     "pending",
			Config:     store.MustTargetConfigJSON(&cfg),
		}
		if _, err := dbStore.InsertDeliveryTarget(target); err != nil {
			log.Printf("[ENQUEUE] Failed to create drive delivery target for %s: %v", jobID, err)
		} else {
			log.Printf("[ENQUEUE] Created drive delivery target for %s (folder=%s)", jobID, driveFolderID)
		}
	}

	// ── YouTube target ──
	if ytGroup := firstString(normalized, "youtube_group", "channel_id"); ytGroup != "" {
		language := firstString(normalized, "audio_language_for_srt", "language")
		if language == "" {
			language = "en"
		}
		cfg := store.DeliveryTargetConfig{
			GroupName:   ytGroup,
			Language:    language,
			Title:       firstString(normalized, "video_name", "title"),
			Description: firstString(normalized, "script_text", "source_text"),
			Privacy:     firstString(normalized, "privacy_status", "privacy"),
			JobRunID:    firstString(normalized, "job_run_id", "run_id"),
		}
		if cfg.Privacy == "" {
			cfg.Privacy = "private"
		}
		// If tags are present in the payload
		if tags, ok := normalized["tags"]; ok {
			switch v := tags.(type) {
			case []string:
				cfg.Tags = v
			case []interface{}:
				for _, t := range v {
					if s, ok := t.(string); ok {
						cfg.Tags = append(cfg.Tags, s)
					}
				}
			}
		}

		target := &store.DeliveryTarget{
			JobID:      jobID,
			TargetType: "youtube",
			Status:     "pending",
			Config:     store.MustTargetConfigJSON(&cfg),
		}
		if _, err := dbStore.InsertDeliveryTarget(target); err != nil {
			log.Printf("[ENQUEUE] Failed to create youtube delivery target for %s: %v", jobID, err)
		} else {
			log.Printf("[ENQUEUE] Created youtube delivery target for %s (group=%s)", jobID, ytGroup)
		}
	}
}

func firstString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
