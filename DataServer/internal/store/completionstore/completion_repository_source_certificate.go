package completionstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"velox-server/internal/repository"
	"velox-shared/contract"
)

func (r *sqliteCompletionTx) CommitSourceCertificate(ctx context.Context, p repository.SourceCertificateCommitParams) (string, error) {
	f := p.Fence
	commitID := "sourcecert_" + f.TaskID + "_" + f.AttemptID
	var priorStatus, priorWorker, priorLease string
	var priorRevision int
	if err := r.tx.QueryRowContext(ctx, `SELECT status,worker_id,lease_id,task_revision FROM attempt_commits WHERE commit_id=?`, commitID).Scan(&priorStatus, &priorWorker, &priorLease, &priorRevision); err == nil {
		if priorStatus == "COMMITTED" && priorWorker == f.WorkerID && priorLease == f.LeaseID && (priorRevision == f.Revision || priorRevision+1 == f.Revision) {
			return commitID, nil
		}
		return "", fmt.Errorf("%w: prior source-certificate commit fence/status mismatch", ErrCompletionTransitionConflict)
	} else if err != sql.ErrNoRows {
		return "", err
	}
	var taskExecutor, taskJob, taskAttempt, taskWorker, taskLease, taskStatus string
	var taskRevision, terminalPending int
	err := r.tx.QueryRowContext(ctx, `SELECT executor_id,job_id,COALESCE(attempt_id,''),COALESCE(worker_id,''),COALESCE(lease_id,''),status,revision,COALESCE(winning_attempt_terminal_pending,0) FROM tasks WHERE task_id=?`, f.TaskID).Scan(&taskExecutor, &taskJob, &taskAttempt, &taskWorker, &taskLease, &taskStatus, &taskRevision, &terminalPending)
	if err != nil {
		return "", fmt.Errorf("store: source-certificate task lookup: %w", err)
	}
	if taskExecutor != contract.VideoSourceCertificationExecutorID || taskJob != p.JobID || taskAttempt != f.AttemptID || taskWorker != f.WorkerID || taskLease != f.LeaseID || taskRevision != f.Revision || (taskStatus != "RUNNING" && taskStatus != "FAILED") || terminalPending != 1 {
		return "", fmt.Errorf("%w: source-certificate task fence or executor mismatch", ErrCompletionTransitionConflict)
	}
	var specJSON string
	if err := r.tx.QueryRowContext(ctx, `SELECT COALESCE(payload_json,'{}') FROM task_specs WHERE task_id=?`, f.TaskID).Scan(&specJSON); err != nil {
		return "", fmt.Errorf("store: source-certificate canonical task spec: %w", err)
	}
	var canonicalSpec struct {
		Request contract.VideoSourceCertificationV1 `json:"video_source_certification"`
	}
	if err := json.Unmarshal([]byte(specJSON), &canonicalSpec); err != nil || canonicalSpec.Request.Validate() != nil {
		return "", fmt.Errorf("store: invalid canonical source-certificate task spec")
	}
	var attemptStatus string
	if err := r.tx.QueryRowContext(ctx, `SELECT status FROM task_attempts WHERE id=? AND task_id=? AND worker_id=? AND lease_id=?`, f.AttemptID, f.TaskID, f.WorkerID, f.LeaseID).Scan(&attemptStatus); err != nil {
		return "", fmt.Errorf("store: source-certificate attempt lookup: %w", err)
	}
	if attemptStatus != "SUCCEEDED" {
		return "", fmt.Errorf("%w: source-certificate attempt status=%s", ErrCompletionTransitionConflict, attemptStatus)
	}
	var source contract.VideoSourceCertificateV1
	if err := json.Unmarshal([]byte(p.CertificateJSON), &source); err != nil {
		return "", fmt.Errorf("store: decode source-certificate result: %w", err)
	}
	if err := source.Validate(); err != nil || source.SizeBytes != canonicalSpec.Request.Source.SizeBytes || source.ProfileID != canonicalSpec.Request.OutputProfileID || source.PreparationRevision != canonicalSpec.Request.OutputProfileRevision || (canonicalSpec.Request.Source.SHA256 != "" && source.SHA256 != canonicalSpec.Request.Source.SHA256) {
		return "", fmt.Errorf("%w: source certificate does not match fenced request", ErrCompletionTransitionConflict)
	}
	if err := r.PersistVideoSourceCertificate(ctx, p.CertificateJSON, p.Now); err != nil {
		return "", err
	}
	if _, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO attempt_commits (commit_id,task_id,attempt_id,job_id,worker_id,lease_id,task_revision,status,required_output_count,ready_output_count,commit_token_hash,commit_deadline_at,last_progress_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,'VERIFYING',0,0,?,?,?,?,?)`, commitID, f.TaskID, f.AttemptID, p.JobID, f.WorkerID, f.LeaseID, f.Revision, p.TokenHash, p.Now, p.Now, p.Now, p.Now, p.Now); err != nil {
		return "", fmt.Errorf("store: source-certificate commit record: %w", err)
	}
	if err := r.MarkCompletionCommitted(ctx, commitID, p.Now); err != nil {
		return "", err
	}
	if err := r.MarkCompletionTaskSucceeded(ctx, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, p.Now); err != nil {
		return "", err
	}
	if err := r.InsertCompletionOutbox(ctx, "ce_"+commitID, "task", f.TaskID, "commit_protocol.committed", `{"kind":"source_certificate","commit_id":"`+commitID+`","attempt_id":"`+f.AttemptID+`","job_id":"`+p.JobID+`"}`, p.Now); err != nil {
		return "", err
	}
	return commitID, nil
}
