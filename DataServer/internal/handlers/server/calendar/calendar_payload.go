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
		"status":               "PENDING",
		"submitted_via":        "calendar",
		"source":               event.Source,
		"render_only":          true,
		"external_id":          event.ExternalID,
		"calendar_event_id":    event.ID,
		"calendar_date":        event.Date,
		"calendar_event_month": event.Month,
		"calendar_event_year":  event.Year,
		"category":             event.Category,
		"titles":               event.Titles,
		"script_text":          event.ScriptText,
		"scenes":               calendarScenes(event),
		"audio_tracks":         calendarAudioTracks(event),
	}
	return payload
}

// calendarScenes is the canonical calendar-to-render projection. Each scene
// owns its clip, optional stock fallback, voiceover, and duration; no
// top-level arrays or positional voiceover correlation cross the job boundary.
func calendarScenes(event *store.CalendarEvent) []map[string]interface{} {
	if event == nil {
		return nil
	}
	clips := make([]struct {
		kind string
		clip store.VideoClip
	}, 0, len(event.InitialClips)+len(event.IntermediateClips)+len(event.FinalClips))
	for _, group := range []struct {
		kind  string
		items []store.VideoClip
	}{
		{kind: "intro", items: event.InitialClips},
		{kind: "clip", items: event.IntermediateClips},
		{kind: "outro", items: event.FinalClips},
	} {
		for _, clip := range group.items {
			if calendarClipPath(clip) != "" {
				clips = append(clips, struct {
					kind string
					clip store.VideoClip
				}{kind: group.kind, clip: clip})
			}
		}
	}
	for _, clip := range event.StockFootage {
		if calendarClipPath(clip) != "" {
			clips = append(clips, struct {
				kind string
				clip store.VideoClip
			}{kind: "stock", clip: clip})
		}
	}
	out := make([]map[string]interface{}, 0, len(clips))
	for i, entry := range clips {
		duration := entry.clip.Duration
		if duration <= 0 {
			// A canonical scene must carry a declared duration. Do not
			// invent timing for calendar assets that have not been probed
			// or explicitly measured yet.
			continue
		}
		scene := map[string]interface{}{
			"scene_id":         fmt.Sprintf("calendar-scene-%d", i),
			"index":            i,
			"kind":             entry.kind,
			"text":             event.Title,
			"duration_seconds": float64(duration),
			"clip": map[string]interface{}{
				"url":         calendarClipPath(entry.clip),
				"duration_ms": duration * 1000,
			},
		}
		out = append(out, scene)
	}
	return out
}

func calendarAudioTracks(event *store.CalendarEvent) []map[string]interface{} {
	if event == nil || len(event.VoiceoverPaths) == 0 {
		return nil
	}
	tracks := make([]map[string]interface{}, 0, len(event.VoiceoverPaths))
	for _, path := range event.VoiceoverPaths {
		if path = strings.TrimSpace(path); path != "" {
			tracks = append(tracks, map[string]interface{}{
				"source_url": path,
				"role":       "voiceover",
			})
		}
	}
	return tracks
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
