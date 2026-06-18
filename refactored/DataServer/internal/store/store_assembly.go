// Package store / store_assembly.go
//
// Methods backing internal/queue.JobViewAssembler (PR1b) and the
// orchestrator completion path (PR1c). The legacy flat fields
// (master_video_path, drive_url, youtube_video_id, video_uploaded) are
// still surfaced through HTTP endpoints, so the assembler runs the join
// between (jobs, artifacts, job_deliveries, delivery_attempts, delivery_destinations)
// and projects back to the JobView shape expected by handlers.
//
// CompleteJobTx is the atomic "jobs SUCCEEDED + close attempt + outbox
// event" transition (PR 8 + PR 9: outbox_events replaces the legacy
// orchestrator_outbox). The same transaction captures all three changes
// so a process crash after the status flip but before the attempt-close
// leaves no rows half-promoted.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/outbox"
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

// ── PR1c — atomic CompleteJobTx ──────────────────────────────────────────────

// CompleteJobTx atomically marks a job SUCCEEDED, closes the latest
// job_attempts row, and emits a JOB_SUCCEEDED outbox event in a single
// BEGIN IMMEDIATE transaction.
//
// PR 9 cutover: writes to outbox_events (replacing orchestrator_outbox).
// Aggregate_id = job_id so the JobSucceededHandler can recover the
// owning workflow step via workflow_steps.job_id.
//
// Idempotent: re-running on a job already in COMPLETED/SUCCEEDED returns
// nil error and skips the outbox emit (the event is also idempotent
// because the dispatcher marks PROCESSED only once per event_id).
//
// attemptID = 0 closes "any current attempt" (uses latest by started_at);
// attemptID > 0 targets the specific attempt row.
//
// Tx boundary: jobs UPDATE + job_attempts UPDATE + outbox INSERT all
// happen inside the same *sql.Tx and a single Commit. A crash before
// commit rolls back ALL three changes — no orphan event, no half-promoted
// job. (PR 9 cutover tightened this from the previous best-effort
// post-commit emit, which could lose events if the process died
// between commit and emit.)
func (s *SQLiteStore) CompleteJobTx(ctx context.Context, jobID string, attemptID int64, outboxPayload string) error {
	if jobID == "" {
		return fmt.Errorf("store: CompleteJobTx: missing jobID")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("store: CompleteJobTx: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Flip jobs.status COMPLETED + stamp completed_at. The no-op guard
	//    uses UPPER(status) NOT IN ('SUCCEEDED', 'COMPLETED') so repeated
	//    calls do not bump revision on already-completed jobs.
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'COMPLETED', completed_at = ?, updated_at = ?
		 WHERE job_id = ? AND UPPER(COALESCE(status, '')) NOT IN ('SUCCEEDED', 'COMPLETED')`,
		now, now, jobID,
	)
	if err != nil {
		return fmt.Errorf("store: CompleteJobTx: jobs UPDATE: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Already completed — skip both attempt close and outbox emit.
		// Commit needed to release the tx's locks either way.
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: CompleteJobTx: idempotent commit: %w", err)
		}
		return nil
	}

	// 2. Close job_attempts (latest by created_at if attemptID=0 else specific).
	if attemptID > 0 {
		_, err = tx.ExecContext(ctx,
			`UPDATE job_attempts
			 SET status = 'succeeded', finished_at = ?
			 WHERE id = ? AND status NOT IN ('succeeded', 'failed', 'expired')`,
			now, attemptID,
		)
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE job_attempts
			 SET status = 'succeeded', finished_at = ?
			 WHERE job_id = ? AND status NOT IN ('succeeded', 'failed', 'expired')
			 ORDER BY id DESC LIMIT 1`,
			now, jobID,
		)
	}
	if err != nil {
		return fmt.Errorf("store: CompleteJobTx: job_attempts UPDATE: %w", err)
	}

	// 3. Enqueue JOB_SUCCEEDED inside the tx so commit is the single
	//    atomicity boundary. emitOutbox forwards `tx` as the Executor —
	//    the INSERT joins the same write tx as the UPDATEs above.
	//    aggregate_id=job_id is what makes the handler's
	//    GetStepByJobID lookup possible.
	if outboxPayload == "" {
		outboxPayload = "{}"
	}
	if err := s.emitOutbox(ctx, tx, outbox.InsertParams{
		AggregateType: "job",
		AggregateID:   jobID,
		EventType:     "JOB_SUCCEEDED",
		Payload:       []byte(outboxPayload),
	}); err != nil {
		return fmt.Errorf("store: CompleteJobTx: outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: CompleteJobTx: commit: %w", err)
	}
	return nil
}
