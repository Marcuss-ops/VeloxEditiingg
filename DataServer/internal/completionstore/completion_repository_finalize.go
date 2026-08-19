package completionstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (r *sqliteCompletionTx) MarkCompletionCommitted(ctx context.Context, commitID, now string) error {
	res, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET status='COMMITTED',committed_at=?,updated_at=? WHERE commit_id=? AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`, now, now, commitID)
	if err != nil {
		return fmt.Errorf("store: mark completion committed: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("store: mark completion committed rows: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: commit=%s committed rows=%d", ErrCompletionTransitionConflict, commitID, n)
	}
	return nil
}

func (r *sqliteCompletionTx) MarkCompletionTaskAttemptSucceeded(ctx context.Context, attemptID, workerID, leaseID, now string) error {
	// A valid completion commit may resurrect a FAILED attempt. The
	// attempt_commits row is created only by DeclareOutputs, which runs
	// strictly AFTER a successful render; a commit that reaches this point
	// has ready >= required and therefore proves the render + upload
	// succeeded. The worker's durable outbox can race a transient
	// upload/declare failure (e.g. master restart) as TaskResult "failed"
	// BEFORE the resume loop re-drives the commit — that false FAILED must
	// not block the commit. CANCELLED and TIMED_OUT stay terminal
	// (operator/master decisions, never resurrected); SUCCEEDED is the
	// idempotent-replay no-op. error_code/error_message are cleared so a
	// resurrected attempt never carries a stale failure reason.
	res, err := r.tx.ExecContext(ctx, `UPDATE task_attempts SET status='SUCCEEDED',completed_at=COALESCE(completed_at,?),report_version=report_version+1,updated_at=?,error_code='',error_message='' WHERE id=? AND worker_id=? AND lease_id=? AND status NOT IN ('SUCCEEDED','CANCELLED','TIMED_OUT')`, now, now, attemptID, workerID, leaseID)
	if err != nil {
		return fmt.Errorf("store: mark completion attempt succeeded: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("store: mark completion attempt succeeded rows: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: attempt=%s succeeded rows=%d", ErrCompletionTransitionConflict, attemptID, n)
	}
	return nil
}

func (r *sqliteCompletionTx) MarkCompletionTaskSucceeded(ctx context.Context, taskID, attemptID, workerID, leaseID, now string) error {
	// FAILED is allowed alongside RUNNING/LEASED: the same resume-loop race
	// that can leave the attempt FAILED also left the task FAILED (the
	// ingest's task CAS does not clear attempt_id/worker_id/lease_id, so the
	// fence still matches). The valid commit resurrects the task together
	// with its attempt in one transaction.
	res, err := r.tx.ExecContext(ctx, `UPDATE tasks SET status='SUCCEEDED',completed_at=?,updated_at=?,winning_attempt_id=?,winning_attempt_committed_at=?,winning_attempt_terminal_pending=0,revision=revision+1 WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=? AND status IN ('RUNNING','LEASED','FAILED')`, now, now, attemptID, now, taskID, attemptID, workerID, leaseID)
	if err != nil {
		return fmt.Errorf("store: mark completion task succeeded: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("store: mark completion task succeeded rows: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: task=%s succeeded rows=%d", ErrCompletionTransitionConflict, taskID, n)
	}
	return nil
}

func (r *sqliteCompletionTx) MarkCompletionJobSucceededIfTasksDone(ctx context.Context, jobID, now string) error {
	// The persisted request contract is authoritative. render_only=true is
	// the only explicit no-artifact path; every other job must already be at
	// AWAITING_ARTIFACT and must prove at least one durable READY artifact.
	var status, requestJSON string
	if err := r.tx.QueryRowContext(ctx,
		`SELECT COALESCE(status,''), COALESCE(request_json,'{}') FROM jobs WHERE job_id=?`, jobID).
		Scan(&status, &requestJSON); err != nil {
		return fmt.Errorf("store: read completion job contract: %w", err)
	}
	if status == "SUCCEEDED" {
		// Completion retries are idempotent after the original terminal
		// writer committed. Do not turn a harmless replay into a conflict.
		return nil
	}
	if strings.TrimSpace(requestJSON) == "" {
		requestJSON = `{}`
	}
	var contract map[string]interface{}
	if err := json.Unmarshal([]byte(requestJSON), &contract); err != nil {
		return fmt.Errorf("%w: invalid request_json for job %s: %v", ErrCompletionTransitionConflict, jobID, err)
	}
	renderOnly, _ := contract["render_only"].(bool)
	artifactContract := !renderOnly
	// An artifact-contract job may reach the commit while still RUNNING: in
	// the completion-protocol flow the worker publishes its outputs (declare
	// → upload → complete) BEFORE the TaskResult whose ingest would normally
	// roll the job RUNNING→AWAITING_ARTIFACT. The commit therefore performs
	// that promotion itself (see below) inside the SAME transaction that
	// writes SUCCEEDED — the intermediate state is never skipped, but it is
	// finalizer-owned in this flow instead of ingest-owned. A job in any
	// other state (PENDING/LEASED/CANCELLED) is still rejected; FAILED is
	// admitted (see the gate below) so a false "failed" TaskResult raced
	// ahead of the resume-loop commit cannot strand a valid commit.
	if artifactContract && status != "AWAITING_ARTIFACT" && status != "RUNNING" && status != "FAILED" {
		return fmt.Errorf("%w: completion job %s must be AWAITING_ARTIFACT (or RUNNING, or FAILED) before SUCCEEDED (status=%s)", ErrCompletionTransitionConflict, jobID, status)
	}
	if !artifactContract && status != "RUNNING" && status != "AWAITING_ARTIFACT" && status != "FAILED" {
		return fmt.Errorf("%w: render-only job %s cannot complete from status=%s", ErrCompletionTransitionConflict, jobID, status)
	}

	// A READY row is not sufficient by itself: it must carry a verified
	// timestamp, a canonical lowercase 64-hex SHA-256, and durable storage.
	// For artifact jobs, require at least one such row and reject every
	// declared/associated artifact that is not equally publishable.
	artifactGate := "1=1"
	if artifactContract {
		artifactGate = `
			-- Every artifact-bound completion must have a durable commit
			-- contract; an unrelated READY artifact can never satisfy it.
			EXISTS (
				SELECT 1 FROM attempt_commits ac
				WHERE ac.job_id=? AND ac.required_output_count>0
			)
			AND NOT EXISTS (
				SELECT 1 FROM attempt_commits ac
				WHERE ac.job_id=? AND ac.required_output_count>0
				  AND (
					ac.required_output_count <> (
						SELECT COUNT(*) FROM task_output_declarations d
						WHERE d.commit_id=ac.commit_id
					)
					OR EXISTS (
						SELECT 1
						FROM task_output_declarations d
						LEFT JOIN artifacts a ON a.id=d.artifact_id
						LEFT JOIN artifact_uploads u ON u.upload_id=d.upload_id
						WHERE d.commit_id=ac.commit_id
						  AND (
							d.artifact_id IS NULL
							OR a.job_id<>ac.job_id
							OR a.status!='READY'
							OR COALESCE(a.verified_at,'')=''
							OR length(COALESCE(a.sha256,''))<>64
							OR COALESCE(a.sha256,'') GLOB '*[^0-9a-f]*'
							OR (COALESCE(a.storage_key,'')='' AND COALESCE(a.local_path,'')='')
							OR lower(COALESCE(d.expected_sha256,''))<>lower(COALESCE(a.sha256,''))
							OR lower(COALESCE(u.received_sha256,''))<>lower(COALESCE(a.sha256,''))
							OR COALESCE(u.received_size_bytes,-1)<>COALESCE(a.size_bytes,-2)
							OR d.expected_size_bytes<>COALESCE(a.size_bytes,-1)
						  )
					)
				  )
			)
			AND NOT EXISTS (
				SELECT 1 FROM attempt_commits ac
				WHERE ac.job_id=? AND ac.required_output_count>ac.ready_output_count
			)`
	}

	// Finalizer-owned RUNNING→AWAITING_ARTIFACT promotion (see contract gate
	// above). Gated on every sibling task being SUCCEEDED plus the same
	// artifact contract, so a job whose artifact set is missing/invalid is
	// NOT promoted — it stays RUNNING and the commit fails closed.
	if artifactContract && status == "RUNNING" {
		// Six placeholders: updated_at, job_id, tasks.job_id, plus the
		// three attempt_commits gates inside artifactGate (all jobID).
		promoteArgs := []interface{}{now, jobID, jobID, jobID, jobID, jobID}
		res, promoteErr := r.tx.ExecContext(ctx, `
			UPDATE jobs
			SET status='AWAITING_ARTIFACT', updated_at=?, revision=revision+1
			WHERE job_id=?
			  AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.job_id=? AND t.status!='SUCCEEDED')
			  AND `+artifactGate+`
			  AND status='RUNNING'`, promoteArgs...)
		if promoteErr != nil {
			return fmt.Errorf("store: promote completion job RUNNING→AWAITING_ARTIFACT: %w", promoteErr)
		}
		if n, rowsErr := res.RowsAffected(); rowsErr != nil {
			return fmt.Errorf("store: promote completion job rows affected: %w", rowsErr)
		} else if n != 1 {
			return fmt.Errorf("%w: completion job %s did not satisfy artifact/status gate for AWAITING_ARTIFACT promotion (status=%s)", ErrCompletionTransitionConflict, jobID, status)
		}
		status = "AWAITING_ARTIFACT"
	}

	query := `
		UPDATE jobs
		SET status='SUCCEEDED', completed_at=?, updated_at=?, revision=revision+1
		WHERE job_id=?
		  AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.job_id=? AND t.status!='SUCCEEDED')
		  AND ` + artifactGate + `
		  AND status=?`
	args := []interface{}{now, now, jobID, jobID}
	if artifactContract {
		args = append(args, jobID, jobID, jobID)
	}
	args = append(args, status)
	res, err := r.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("store: mark completion job succeeded: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("store: mark completion job succeeded rows affected: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: completion job %s did not satisfy artifact/status gate (status=%s)", ErrCompletionTransitionConflict, jobID, status)
	}
	return nil
}

func (r *sqliteCompletionTx) InsertCompletionDeliveries(ctx context.Context, jobID, now string) error {
	rows, err := r.tx.QueryContext(ctx, `
		SELECT a.id, p.destination_id, COALESCE(p.publication_id, '')
		FROM artifacts a CROSS JOIN job_delivery_plans p
		WHERE a.job_id=? AND p.job_id=? AND a.status='READY' AND p.enabled=1
		  AND (
			(json_extract(p.metadata_json, '$.output_variant_id') IS NOT NULL
			 AND a.output_kind = json_extract(p.metadata_json, '$.output_variant_id'))
			OR
			(json_extract(p.metadata_json, '$.output_artifact_role') IS NOT NULL
			 AND a.output_kind = json_extract(p.metadata_json, '$.output_artifact_role'))
			OR
			(json_extract(p.metadata_json, '$.output_variant_id') IS NULL
			 AND json_extract(p.metadata_json, '$.output_artifact_role') IS NULL
			AND (a.output_kind IN ('final_video','video') OR (a.output_kind='' AND a.type IN ('video','final_video')))))
		  )`, jobID, jobID)
	if err != nil {
		return fmt.Errorf("store: completion delivery query: %w", err)
	}
	defer rows.Close()
	type key struct{ a, p, d string }
	seen := map[key]bool{}
	for rows.Next() {
		var a, d, publicationID string
		if err := rows.Scan(&a, &d, &publicationID); err != nil {
			return fmt.Errorf("store: completion delivery scan: %w", err)
		}
		if a == "" || d == "" || seen[key{a, publicationID, d}] {
			continue
		}
		seen[key{a, publicationID, d}] = true
		deliveryKey := a + "_" + publicationID + "_" + d
		if _, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO job_deliveries (delivery_id,artifact_id,publication_id,destination_id,status,idempotency_key,created_at,updated_at) VALUES (?,?,?,?,'PENDING',?,?,?)`, `jbd_comp_`+deliveryKey, a, publicationID, d, deliveryKey, now, now); err != nil {
			return fmt.Errorf("store: completion delivery insert: %w", err)
		}
	}
	return rows.Err()
}

func (r *sqliteCompletionTx) InsertCompletionOutbox(ctx context.Context, eventID, aggregateType, aggregateID, eventType, payloadJSON, now string) error {
	_, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO outbox_events (event_id,aggregate_type,aggregate_id,event_type,payload_json,status,available_at,attempt_count,created_at) VALUES (?,?,?,?,?,'PENDING',?,0,?)`, eventID, aggregateType, aggregateID, eventType, payloadJSON, now, now)
	if err != nil {
		return fmt.Errorf("store: completion outbox insert: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) FindCompletionAttempt(ctx context.Context, commitID string) (*CompletionAttemptRow, error) {
	var x CompletionAttemptRow
	err := r.tx.QueryRowContext(ctx, `SELECT commit_id,task_id,attempt_id,job_id,worker_id,lease_id,status,required_output_count,ready_output_count,COALESCE(commit_deadline_at,'') FROM attempt_commits WHERE commit_id=?`, commitID).Scan(&x.CommitID, &x.TaskID, &x.AttemptID, &x.JobID, &x.WorkerID, &x.LeaseID, &x.Status, &x.RequiredOutputCount, &x.ReadyOutputCount, &x.CommitDeadlineAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: commit_id=%s", ErrCompletionAttemptNotFound, commitID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: find completion attempt: %w", err)
	}
	return &x, nil
}

func (r *sqliteCompletionTx) GetCompletionResult(ctx context.Context, commitID string) (*CompletionCommitResult, error) {
	var out CompletionCommitResult
	var committed sql.NullString
	var task, job sql.NullString
	err := r.tx.QueryRowContext(ctx, `SELECT ac.commit_id,ac.task_id,ac.attempt_id,ac.job_id,COALESCE(t.status,''),COALESCE(j.status,''),ac.committed_at FROM attempt_commits ac LEFT JOIN tasks t ON t.task_id=ac.task_id LEFT JOIN jobs j ON j.job_id=ac.job_id WHERE ac.commit_id=?`, commitID).Scan(&out.CommitID, &out.TaskID, &out.AttemptID, &out.JobID, &task, &job, &committed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: commit_id=%s", ErrCompletionAttemptNotFound, commitID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: completion result: %w", err)
	}
	out.TaskStatus = task.String
	out.JobStatus = job.String
	if committed.Valid && committed.String != "" {
		if t, e := time.Parse(time.RFC3339Nano, committed.String); e == nil {
			out.CommittedAt = &t
		}
	}
	rows, err := r.tx.QueryContext(ctx, `SELECT a.id FROM artifacts a JOIN task_output_declarations d ON d.artifact_id=a.id WHERE d.commit_id=? AND a.status='READY'`, commitID)
	if err != nil {
		return nil, fmt.Errorf("store: completion result artifacts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if e := rows.Scan(&id); e != nil {
			return nil, e
		}
		out.ArtifactIDs = append(out.ArtifactIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &out, nil
}
