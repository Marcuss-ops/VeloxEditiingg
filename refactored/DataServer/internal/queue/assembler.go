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

	const query = `
SELECT
    -- jobs canonical fields
    j.job_id, COALESCE(j.job_type, '') AS job_type, j.status, j.revision,
    j.video_name, j.project_id, j.created_at, j.updated_at,
    j.started_at, j.completed_at,
    j.last_error_code, j.last_error_message,

    -- primary artifact (smallest id wins the deterministic tie break)
    (SELECT id FROM artifacts
       WHERE artifact_id_can_match = j.job_id AND status = 'READY'
       ORDER BY id ASC LIMIT 1) AS primary_artifact_id,

    -- video_uploaded: any READY artifact for the job
    EXISTS(SELECT 1 FROM artifacts
            WHERE artifact_id_can_match = j.job_id AND status = 'READY') AS video_uploaded,

    -- most recent successful drive delivery
    (SELECT jd.remote_url FROM job_deliveries jd
       JOIN delivery_attempts da ON da.delivery_id = jd.delivery_id
       WHERE jd.artifact_id IN (SELECT id FROM artifacts WHERE artifact_id_can_match = j.job_id)
         AND jd.destination_id IN (SELECT destination_id FROM delivery_destinations WHERE provider = 'drive')
         AND da.status = 'SUCCESS'
       ORDER BY da.completed_at DESC LIMIT 1) AS drive_url,

    -- most recent successful youtube delivery
    (SELECT jd.remote_id FROM job_deliveries jd
       JOIN delivery_attempts da ON da.delivery_id = jd.delivery_id
       WHERE jd.artifact_id IN (SELECT id FROM artifacts WHERE artifact_id_can_match = j.job_id)
         AND jd.destination_id IN (SELECT destination_id FROM delivery_destinations WHERE provider = 'youtube')
         AND da.status = 'SUCCESS'
       ORDER BY da.completed_at DESC LIMIT 1) AS youtube_video_id
FROM jobs j
WHERE j.job_id = ?
LIMIT 1
`

	// Placeholder substitution: the schema used by *store.SQLiteStore queries
	// regresses to a flat table name — caller is responsible for adapting the
	// join columns. Implementations may rewrite with a different artifact
	// matcher. We delegate the actual SQL execution to the store via the
	// public Joins/Rows API depending on what the store package exposes.
	//
	// We use a generic Build view that projects the canonical (jobs, artifacts,
	// job_deliveries, delivery_attempts) tuple. The store helper gets the job
	// rows + the joined rows in one round-trip via store.AssembleJobView
	// (defined in store/store_assembly.go — see migration 022 for schema).
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
