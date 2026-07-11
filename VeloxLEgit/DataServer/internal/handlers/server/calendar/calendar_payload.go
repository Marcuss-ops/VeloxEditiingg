package calendar

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

func buildCalendarJobPayload(event *store.CalendarEvent, jobRunID string) map[string]interface{} {
	clipPaths := func(clips []store.VideoClip) []string {
		out := make([]string, 0, len(clips))
		for _, clip := range clips {
			path := calendarClipPath(clip)
			if path != "" {
				out = append(out, path)
			}
		}
		return out
	}

	voiceovers := make([]string, 0, len(event.VoiceoverPaths))
	for _, s := range event.VoiceoverPaths {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			voiceovers = append(voiceovers, trimmed)
		}
	}

	if strings.TrimSpace(jobRunID) == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	createdAt := time.Now().UTC().Format(time.RFC3339)
	// refactor/payload-v2-single-shape: drop the `parameters` sub-map
	// mirror that previously duplicated every field here. Single
	// canonical map at top level only — readers that still expect the
	// legacy mirror (e.g. older calendar handlers) now consult the
	// top-level keys directly.
	payload := map[string]interface{}{
		"job_id":               event.JobID,
		"job_run_id":           jobRunID,
		"job_type":             "process_video",
		"priority":             1,
		"created_at":           createdAt,
		"timeout_secs":         1800,
		"video_name":           event.Title,
		"project_id":           event.ID,
		"youtube_group":        event.YouTubeGroup,
		"status":               "PENDING",
		"submitted_via":        "calendar",
		"source":               event.Source,
		"external_id":          event.ExternalID,
		"calendar_event_id":    event.ID,
		"calendar_date":        event.Date,
		"calendar_event_month": event.Month,
		"calendar_event_year":  event.Year,
		"category":             event.Category,
		"titles":               event.Titles,
		"youtube_links":        event.YouTubeLinks,
		"script_text":          event.ScriptText,
		"start_clip_paths":     clipPaths(event.InitialClips),
		"middle_clip_paths":    clipPaths(event.IntermediateClips),
		"end_clip_paths":       clipPaths(event.FinalClips),
		"stock_clip_paths":     clipPaths(event.StockFootage),
		"voiceover_paths":      voiceovers,
	}
	return payload
}

func existingJobRunID(job *jobs.QueueItem) string {
	if job == nil || job.Payload == nil {
		return ""
	}
	if v, ok := job.Payload["job_run_id"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func calendarClipPath(clip store.VideoClip) string {
	for _, candidate := range []string{clip.Path, clip.URL, clip.WebView} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	if strings.TrimSpace(clip.DriveID) != "" {
		return "/api/drive/media/" + strings.TrimSpace(clip.DriveID)
	}
	return strings.TrimSpace(clip.Name)
}

func generateETag(events []*store.CalendarEvent, minimal bool) string {
	h := sha256.New()
	for _, e := range events {
		fmt.Fprintf(h, "%s-%d-%d-%d-%d-%s-%s-%s-%s", e.ID, e.Date, e.Month, e.Year, len(e.Title), e.Status, e.JobID, e.JobStatus, e.UpdatedAt.UTC().Format(time.RFC3339))
	}
	hash := hex.EncodeToString(h.Sum(nil))[:16]
	return fmt.Sprintf("W/\"cal-%s-%d\"", hash, len(events))
}
