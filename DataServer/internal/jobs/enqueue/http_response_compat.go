// Package enqueue — legacy HTTP response formatter.
//
// RenderHTTPBoundaryJobResponse builds the script-handler response map
// from a canonical (flat, job_id/job_run_id/video_name) row map.
// The PR15.6 legacy alias dual-write (script_id, run_id, title) has
// been removed: the handler response surface carries only canonical keys.
//
// Drive folder resolution is delegated through the optional resolver
// interface; a nil resolver preserves the raw value.
package enqueue

import (
	"context"
	"fmt"
)

func RenderHTTPBoundaryJobResponse(ctx context.Context, job map[string]interface{}, full bool, resolver DriveFolderResolver) (map[string]interface{}, error) {
	if job == nil {
		return map[string]interface{}{"ok": false}, nil
	}
	driveOutput, err := ResolveDriveOutputFolderReference(ctx, fmt.Sprint(job["drive_output_folder"]), resolver)
	if err != nil {
		return nil, fmt.Errorf("resolve response drive output folder: %w", err)
	}
	response := map[string]interface{}{
		"ok":                  true,
		"job_id":              job["job_id"],
		"status":              job["status"],
		"video_name":          job["video_name"],
		"job_run_id":          job["job_run_id"],
		"created_at":          job["created_at"],
		"updated_at":          job["updated_at"],
		"started_at":          job["started_at"],
		"completed_at":        job["completed_at"],
		"output_path":         job["output_path"],
		"drive_output_folder": driveOutput,
		"scene_count":         job["scene_count"],
		"voiceover_count":     job["voiceover_count"],
		"video_mode":          job["video_mode"],
	}
	if errMsg, ok := job["error"].(string); ok && errMsg != "" {
		response["error"] = errMsg
	}
	if result := job["result"]; result != nil {
		response["result"] = result
	}
	if full {
		response["job"] = job
		response["request"] = job["request"]
	}
	return response, nil
}
