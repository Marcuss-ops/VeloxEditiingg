// Package queue assembler — assembles JobView from canonical tables.
//
// JobViewAssembler is the only sanctioned way to read legacy flat fields
// (master_video_path, drive_url, youtube_video_id, video_uploaded). It
// performs a single SQL JOIN across jobs + artifacts + job_deliveries +
// delivery_attempts and projects the result back to a JSON-compatible
// JobView. The legacy fields are NOT persisted on the jobs row anymore.
//
// The interface keeps the SQL behind a method, so unit tests can supply
// a stub assembler and verify callers do not reach into Job flat fields
// that have been removed from the domain.
package queue

import (
	"context"

	"velox-server/internal/store"
)

// JobViewAssembler assembles a JobView from canonical tables.
//
// Implementations MUST:
//   * consult the database as the source of truth
//   * use only one round-trip per Build (joining is fine)
//   * never look at the legacy flat fields on the Job struct
type JobViewAssembler interface {
	// Build returns all fields needed by the legacy HTTP API for one job.
	// Returns (nil, error) on transient DB failure, (nil, nil) when the
	// job does not exist.
	Build(ctx context.Context, jobID string) (*JobView, error)
}

// SQLJobViewAssembler is the production implementation backed by *store.SQLiteStore.
//
// Per architecture doc: "video_uploaded = esiste almeno un artifact READY",
// "master_video_path = storage_key dell'artifact primario", "drive_url =
// URL dell'ultima delivery Drive riuscita", "youtube_video_id = remote_id
// dell'ultima delivery YouTube riuscita".
type SQLJobViewAssembler struct {
	dbStore *store.SQLiteStore
}

// NewSQLJobViewAssembler wires the assembler against a SQLite handle.
func NewSQLJobViewAssembler(dbStore *store.SQLiteStore) *SQLJobViewAssembler {
	return &SQLJobViewAssembler{dbStore: dbStore}
}

// Build runs the assembled JOIN and projects the result into a JobView.
//
// SQL strategy:
//   * LEFT JOIN artifacts WHERE status='READY' for video_uploaded + primary
//     artifact (lowest id wins for determinism).
//   * LEFT JOIN job_deliveries → delivery_attempts for drive_url + youtube_video_id.
//   * DriveURL: the most recent SUCCESSFUL delivery_attempt where provider='drive'.
//   * YouTubeVideoID: the most recent SUCCESSFUL delivery_attempt where provider='youtube'.
//
// The implementation here is intentionally a single SELECT. The schema must
// provide the joinee columns; if 022_split_deliveries.sql has been applied,
// job_deliveries and delivery_attempts are queryable end-to-end.
func (a *SQLJobViewAssembler) Build(ctx context.Context, jobID string) (*JobView, error) {
	if a == nil || a.dbStore == nil {
		return nil, nil
	}

	m, err := a.dbStore.AssembleJobView(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, nil
	}

	view := &JobView{
		JobID:    asStringFromMap(m, "job_id"),
		Type:     asStringFromMap(m, "job_type"),
		Status:   JobStatus(asStringFromMap(m, "status")),
		Revision: asInt64FromMap(m, "revision"),
		VideoName: asStringFromMap(m, "video_name"),
		ProjectID: asStringFromMap(m, "project_id"),
	}
	if v := asStringFromMap(m, "primary_artifact_id"); v != "" {
		view.MasterVideoPath = v
	}
	if b := asBoolFromMap(m, "video_uploaded"); b {
		view.VideoUploaded = true
	}
	if v := asStringFromMap(m, "drive_url"); v != "" {
		view.DriveURL = v
	}
	if v := asStringFromMap(m, "youtube_video_id"); v != "" {
		view.YouTubeVideoID = v
	}
	return view, nil
}

// helpers (inlined because store_assembly.go may not exist yet under the new
// names — they are cheap enough to re-declare here without owning the package
// contract).
func asStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func asInt64FromMap(m map[string]interface{}, key string) int64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		case float64:
			return int64(n)
		}
	}
	return 0
}

func asBoolFromMap(m map[string]interface{}, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}
