package script

import (
	"strings"

	"velox-shared/paths"
	"velox-shared/payload"
)

func normalizeStringList(source map[string]interface{}, keys ...string) []string {
	return payload.NormalizeStringList(source, keys...)
}

func firstNonEmptyString(source map[string]interface{}, keys ...string) string {
	return payload.FirstString(source, keys...)
}

func floatFromPayload(source map[string]interface{}, fallback float64, keys ...string) float64 {
	return payload.FloatParam(source, fallback, keys...)
}

func intFromPayload(source map[string]interface{}, fallback int, key string) int {
	return payload.IntParam(source, fallback, key)
}

func ensureInt(value interface{}, fallback int) int {
	return payload.EnsureInt(value, fallback)
}

func normalizedDuration(value interface{}) float64 {
	return payload.NormalizedDuration(value)
}

func ensureRFC3339(value, fallback string) string {
	return payload.EnsureRFC3339(value, fallback)
}

func sanitizeVideoName(value string) string {
	return paths.SanitizeVideoName(value)
}

func buildScriptText(payload map[string]interface{}) string {
	var parts []string
	if s := firstNonEmptyString(payload, "topic", "title"); s != "" {
		parts = append(parts, s)
	}
	if s := firstNonEmptyString(payload, "source_text"); s != "" {
		parts = append(parts, s)
	}
	if len(parts) == 0 {
		parts = append(parts, "script with images")
	}
	return strings.Join(parts, " - ")
}

func isLikelyMediaSource(value string) bool {
	return payload.IsLikelyMediaSource(value)
}

func dedupeStrings(values []string) []string {
	return payload.DedupeStrings(values)
}

func mustJSON(v interface{}) string {
	return payload.MustJSON(v)
}

func renderJobResponse(job map[string]interface{}, full bool) map[string]interface{} {
	if job == nil {
		return map[string]interface{}{"ok": false}
	}
	response := map[string]interface{}{
		"ok":                  true,
		"job_id":              firstString(job, "job_id"),
		"script_id":           firstString(job, "job_id", "script_id"),
		"status":              firstString(job, "status"),
		"video_name":          firstString(job, "video_name", "title"),
		"job_run_id":          firstString(job, "job_run_id", "run_id"),
		"run_id":              firstString(job, "run_id", "job_run_id"),
		"created_at":          job["created_at"],
		"updated_at":          job["updated_at"],
		"started_at":          job["started_at"],
		"completed_at":        job["completed_at"],
		"output_path":         firstString(job, "output_path"),
		"drive_output_folder": firstString(job, "drive_output_folder"),
		"scene_count":         job["scene_count"],
		"voiceover_count":     job["voiceover_count"],
		"video_mode":          firstString(job, "video_mode"),
	}
	if errMsg := firstString(job, "error", "last_error", "error_message"); errMsg != "" {
		response["error"] = errMsg
	}
	if result := job["result"]; result != nil {
		response["result"] = result
	}
	if full {
		response["job"] = job
		response["request"] = job["request"]
	}
	return response
}

func firstString(source map[string]interface{}, keys ...string) string {
	return payload.FirstString(source, keys...)
}
