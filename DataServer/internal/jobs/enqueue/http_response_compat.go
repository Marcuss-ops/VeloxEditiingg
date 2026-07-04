// Package enqueue — http_response_compat.go isolates the HTTP-edge job-
// response formatter from the canonical enqueue pipeline.
//
// ────────────────────────────────────────────────────────────────────────
// WHY THIS FILE IS OUTSIDE THE CANONICAL-PURITY GATE
// ────────────────────────────────────────────────────────────────────────
// scripts/ci/check-payload-canonical-form.sh (Step 8/8) grep-fails any
// MASTER-write path that emits a forbidden legacy alias
// (`"id":|`"run_id":|`"title":|`"voiceover_path":|`"audio_path":`) or a
// `"parameters": map[...]` sub-map mirror.
//
// This file is INTENTIONALLY EXCLUDED from that gate because the
// formatter deliberately dual-writes the legacy aliases alongside their
// canonical counterparts, to keep HTTP clients that pre-date PR15.6
// functional against the canonical-only persisted rows.
//
// History: PR15.6 (Velox Maintainer) renamed the helper from
//
//	RenderJobResponse         → RenderHTTPBoundaryJobResponse
//
// to make the intent explicit: this is the SOLE canonical-to-alias
// adapter in the codebase. Internal callers (script handler,
// creatorflow, pipeline) all consume canonical keys already. ONLY this
// helper tolerates dual-write reads.
//
// Future contributors: if you find yourself adding another file or
// function with a similarly-shaped legacy-key dual-write, please:
//
//  1. Do NOT add it to the gate allowlist inline.
//  2. Move/copy the pattern HERE and update this docstring + the gate.
//  3. Cite the back-compat reason explicitly in the commit message.
//
// ────────────────────────────────────────────────────────────────────────
package enqueue

import (
	"os"

	"velox-shared/payload"
)

// RenderHTTPBoundaryJobResponse builds the HTTP-edge JSON response map for
// a job record, READing via legacy-alias-tolerant fallbacks so old SQLite
// rows that still carry `id`/`run_id`/`title`/`voiceover_path`/`audio_path`
// (written before PR15.6) continue to render correctly.
//
// PR15.6: renamed from RenderJobResponse. The function is the sole canonical-
// to-alias adapter at the HTTP boundary; internal callers (script handler,
// creatorflow, pipeline) all consume canonical keys already. ONLY this
// helper tolerates dual-write reads.
//
// The legacy alias probe chain tolerates rows written before PR15.6 that
// still carry `id` (HTTP01 subtest basic_legacy_alias_fallback) — `id` is
// consulted LAST so canonical `job_id` wins when present.
func RenderHTTPBoundaryJobResponse(job map[string]interface{}, full bool) map[string]interface{} {
	if job == nil {
		return map[string]interface{}{"ok": false}
	}
	response := map[string]interface{}{
		"ok":                  true,
		"job_id":              payload.FirstString(job, "job_id"),
		"script_id":           payload.FirstString(job, "job_id", "script_id", "id"),
		"status":              payload.FirstString(job, "status"),
		"video_name":          payload.FirstString(job, "video_name", "title"),
		"job_run_id":          payload.FirstString(job, "job_run_id", "run_id"),
		"run_id":              payload.FirstString(job, "run_id", "job_run_id"),
		"created_at":          job["created_at"],
		"updated_at":          job["updated_at"],
		"started_at":          job["started_at"],
		"completed_at":        job["completed_at"],
		"output_path":         payload.FirstString(job, "output_path"),
		"drive_output_folder": ResolveDriveOutputFolderReference(os.Getenv("VELOX_DATA_DIR"), payload.FirstString(job, "drive_output_folder")),
		"scene_count":         job["scene_count"],
		"voiceover_count":     job["voiceover_count"],
		"video_mode":          payload.FirstString(job, "video_mode"),
	}
	if errMsg := payload.FirstString(job, "error", "last_error", "error_message"); errMsg != "" {
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
