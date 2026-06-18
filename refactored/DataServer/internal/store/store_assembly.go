// Package store / store_assembly.go
//
// Methods backing internal/queue.JobViewAssembler (PR1b) and the
// orchestrator completion path (PR1c). Flat fields from the legacy jobs
// table (master_video_path, drive_url, youtube_video_id, video_uploaded)
// are still surfaced through HTTP endpoints via the join between
// (jobs, artifacts, job_deliveries, delivery_attempts, delivery_destinations)
// so the assembler projects back to the JobView shape expected by handlers.
//
// The sole legal writer of jobs.status='SUCCEEDED' is
// FinalizationRepository.FinalizeVerified.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// AssembleJobView returns the legacy-flat-fields projection of a job for
// the HTTP API. Implementations must read the underlying tables as the
// source of truth — NEVER read master_video_path/drive_url/etc. from the
// jobs raw_json blob.
//
// One SELECT per Build call; the join is correlated subqueries so the
// planner can short-circuit and the result rows map 1:1 to JobView.
// Returns (nil, nil) when the job does not exist.
func (s *SQLiteStore) AssembleJobView(ctx context.Context, jobID string) (map[string]interface{}, error) {
	if jobID == "" {
		return nil, fmt.Errorf("store: AssembleJobView missing jobID")
	}

	const jobQuery = `
SELECT
  j.job_id, COALESCE(j.job_type, '') AS job_type, j.status, COALESCE(j.revision, 0) AS revision,
  j.video_name, j.project_id, j.created_at, j.updated_at, j.started_at, j.completed_at,
  j.last_error, j.error_message,
  (SELECT id FROM artifacts
     WHERE job_id = j.job_id AND status = 'READY'
     ORDER BY id ASC LIMIT 1) AS primary_artifact_id,
  EXISTS(SELECT 1 FROM artifacts
          WHERE job_id = j.job_id AND status = 'READY') AS video_uploaded,
  (SELECT jd.remote_url
     FROM job_deliveries jd
     JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id AND dd.provider = 'drive'
     WHERE jd.artifact_id IN (SELECT id FROM artifacts WHERE job_id = j.job_id AND status = 'READY')
       AND jd.remote_url != ''
     ORDER BY jd.updated_at DESC LIMIT 1) AS drive_url,
  (SELECT jd.remote_id
     FROM job_deliveries jd
     JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id AND dd.provider = 'youtube'
     WHERE jd.artifact_id IN (SELECT id FROM artifacts WHERE job_id = j.job_id AND status = 'READY')
       AND jd.remote_id != ''
     ORDER BY jd.updated_at DESC LIMIT 1) AS youtube_video_id
FROM jobs j
WHERE j.job_id = ?
LIMIT 1
`
	row := s.db.QueryRowContext(ctx, jobQuery, jobID)
	out := map[string]interface{}{}
	var (
		jobIDOut, status, jobType, videoName, projectID sql.NullString
		createdAt, updatedAt, startedAt, completedAt    sql.NullString
		lastError, errorMessage                        sql.NullString
		revision                                      sql.NullInt64
		primaryArtifactID, driveURL, youtubeVideoID    sql.NullString
		videoUploaded                                  sql.NullInt64
	)
	if err := row.Scan(
		&jobIDOut, &jobType, &status, &revision,
		&videoName, &projectID, &createdAt, &updatedAt, &startedAt, &completedAt,
		&lastError, &errorMessage,
		&primaryArtifactID, &videoUploaded,
		&driveURL, &youtubeVideoID,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("store: AssembleJobView scan: %w", err)
	}

	setStr := func(k string, v sql.NullString) {
		if v.Valid {
			out[k] = v.String
		}
	}
	setStr("job_id", jobIDOut)
	setStr("job_type", jobType)
	setStr("status", status)
	setStr("video_name", videoName)
	setStr("project_id", projectID)
	setStr("created_at", createdAt)
	setStr("updated_at", updatedAt)
	setStr("started_at", startedAt)
	setStr("completed_at", completedAt)
	setStr("last_error", lastError)
	setStr("error_message", errorMessage)
	setStr("primary_artifact_id", primaryArtifactID)
	setStr("drive_url", driveURL)
	setStr("youtube_video_id", youtubeVideoID)
	if revision.Valid {
		out["revision"] = revision.Int64
	}
	if videoUploaded.Valid && videoUploaded.Int64 != 0 {
		out["video_uploaded"] = true
	}
	if primaryArtifactID.Valid && primaryArtifactID.String != "" {
		storageKey, storageURL, sha256, mime, _ := s.loadArtifactStorageFields(ctx, primaryArtifactID.String)
		out["master_video_path"] = selectStorageValue(storageKey, storageURL)
		out["artifact_id"] = primaryArtifactID.String
		if sha256 != "" {
			out["output_sha256"] = sha256
		}
		if mime != "" {
			out["mime_type"] = mime
		}
	}
	return out, nil
}

// loadArtifactStorageFields returns (storage_key, storage_url, sha256, mime_type, has_row).
func (s *SQLiteStore) loadArtifactStorageFields(ctx context.Context, artifactID string) (string, string, string, string, bool) {
	row := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(sha256,''), COALESCE(mime_type,'')
		 FROM artifacts WHERE id = ?`, artifactID)
	var sk, su, sh, mt string
	if err := row.Scan(&sk, &su, &sh, &mt); err != nil {
		return "", "", "", "", false
	}
	return sk, su, sh, mt, true
}

// selectStorageValue picks the value the JobView will surface as
// master_video_path.
//
// Policy: storage_url is preferred — it is the resolvable URL the worker
// recorded when the artifact was finalized, and it is what the HTTP API
// historically surfaced as master_video_path. storage_key is a
// relative / orchestrator-internal path that the worker uses locally but
// is not directly fetchable by clients, so it is used only as a fallback
// when the master has not yet been wired to write storage_url (e.g.
// legacy backfills without a fully landed 022 migration).
func selectStorageValue(storageKey, storageURL string) string {
	if strings.TrimSpace(storageURL) != "" {
		return storageURL
	}
	return storageKey
}


