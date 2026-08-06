package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/sqliteerr"
)

// CompletionStore is the persistence boundary for the artifact completion
// protocol. Application packages submit typed operations; this package owns
// the SQLite connection, transaction lifecycle, SQL, and row projections.
type CompletionStore interface {
	Run(ctx context.Context, fn func(CompletionTx) error) error
	ListCompletionUploadBindings(ctx context.Context, commitID string) ([]CompletionUploadBinding, error)
	GetCompletionUploadBinding(ctx context.Context, uploadID string) (*CompletionUploadBinding, error)
	BindCompletionUpload(ctx context.Context, declarationID, uploadID, artifactID string) error
	GetCompletionCommitTokenHash(ctx context.Context, commitID string) (string, error)
	ScanCompletionCandidates(ctx context.Context, now, deadlineCutoff, progressCutoff, outboxCutoff string, limit int) ([]CompletionReconcileCandidate, int64, error)
}

// CompletionTx is a transaction-bound, typed repository surface. The caller
// never receives *sql.Tx and cannot accidentally commit only part of the
// completion protocol.
type CompletionTx interface {
	ReadCompletionFence(ctx context.Context, fence CompletionFence, allowMissing bool) (*CompletionAttemptState, error)
	InsertCompletionAttempt(ctx context.Context, p CompletionDeclareParams) (string, error)
	InsertCompletionDeclaration(ctx context.Context, p CompletionDeclarationParams) error
	GetCompletionDeclarationID(ctx context.Context, commitID, outputKind, logicalName string) (string, error)
	GetCompletionUploadState(ctx context.Context, uploadID string) (*CompletionUploadState, error)
	CompleteCompletionUpload(ctx context.Context, verdict CompletionArtifactVerdict, uploadID, serverSHA, now string) error
	StampCompletionArtifact(ctx context.Context, artifactID, storageKey, sha string, size int64) error
	UpdateCompletionProgress(ctx context.Context, commitID, now, deadline string) (int64, error)
	UpdateCompletionUploadedBytes(ctx context.Context, fence CompletionFence, uploadID string, uploadedBytes int64, now string) error
	UpdateCompletionReadyCount(ctx context.Context, fence CompletionFence, now string) error
	ExpireCompletionAttempt(ctx context.Context, fence CompletionFence, now string) error
	ExpireCompletionAttemptByID(ctx context.Context, commitID, now string) error
	MarkCompletionCommitted(ctx context.Context, commitID, now string) error
	MarkCompletionTaskAttemptSucceeded(ctx context.Context, attemptID, workerID, leaseID, now string) error
	MarkCompletionTaskSucceeded(ctx context.Context, taskID, attemptID, workerID, leaseID, now string) error
	MarkCompletionJobSucceededIfTasksDone(ctx context.Context, jobID, now string) error
	InsertCompletionDeliveries(ctx context.Context, jobID, now string) error
	InsertCompletionOutbox(ctx context.Context, eventID, aggregateType, aggregateID, eventType, payloadJSON, now string) error
	FindCompletionAttempt(ctx context.Context, commitID string) (*CompletionAttemptRow, error)
	GetCompletionResult(ctx context.Context, commitID string) (*CompletionCommitResult, error)
}

type CompletionFence struct {
	TaskID, AttemptID, WorkerID, LeaseID string
	Revision                             int
}

type CompletionAttemptState struct {
	CommitID     string
	Status       string
	TaskRevision int
}

type CompletionAttemptRow struct {
	CommitID, TaskID, AttemptID, JobID, WorkerID, LeaseID, Status, CommitDeadlineAt string
	RequiredOutputCount, ReadyOutputCount                                           int
}

type CompletionDeclareParams struct {
	CommitID, TaskID, AttemptID, JobID, WorkerID, LeaseID string
	Revision, RequiredOutputCount                         int
	TokenHash, Deadline, Now                              string
}

type CompletionDeclarationParams struct {
	DeclarationID, CommitID, TaskID, AttemptID string
	OutputKind, LogicalName, MimeType          string
	SizeBytes                                  int64
	SHA256, WorkerSpoolKey, Now                string
}

type CompletionUploadState struct {
	UploadID, ExpectedSHA256, ReceivedSHA256, Status string
	ArtifactID, TemporaryStorageKey, MimeType        string
	SizeBytes                                        int64
}

type CompletionArtifactVerdict int

const (
	CompletionKeepVerifying CompletionArtifactVerdict = iota
	CompletionReady
)

type CompletionCommitResult struct {
	CommitID, TaskID, AttemptID, JobID, TaskStatus, JobStatus string
	ArtifactIDs                                               []string
	CommittedAt                                               *time.Time
}

type CompletionUploadBinding struct {
	DeclarationID, CommitID, UploadID, ArtifactID string
	TaskID, AttemptID, WorkerID, LeaseID          string
	Revision                                      int
	OutputKind, LogicalName                       string
}

type CompletionReconcileCandidate struct {
	CommitID, Case string
}

var (
	ErrCompletionAttemptNotFound    = errors.New("store: completion attempt not found")
	ErrCompletionTransitionConflict = errors.New("store: completion transition conflict")
	ErrCompletionCanonicalConflict  = errors.New("store: completion canonical artifact conflict")
	ErrCompletionBindingConflict    = errors.New("store: completion upload binding conflict")
)

type SQLiteCompletionStore struct{ db *sql.DB }

func NewSQLiteCompletionStore(db *sql.DB) *SQLiteCompletionStore {
	if db == nil {
		panic("store: NewSQLiteCompletionStore requires a non-nil database")
	}
	return &SQLiteCompletionStore{db: db}
}

func (s *SQLiteCompletionStore) Run(ctx context.Context, fn func(CompletionTx) error) error {
	if fn == nil {
		return fmt.Errorf("store: completion transaction callback is nil")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("store: completion begin: %w", err)
	}
	ct := &sqliteCompletionTx{tx: tx}
	if err := fn(ct); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: completion commit: %w", err)
	}
	return nil
}

type sqliteCompletionTx struct{ tx *sql.Tx }

func (r *sqliteCompletionTx) ReadCompletionFence(ctx context.Context, f CompletionFence, allowMissing bool) (*CompletionAttemptState, error) {
	var commitID, status, worker, lease string
	var rev int
	err := r.tx.QueryRowContext(ctx, `SELECT commit_id,status,worker_id,lease_id,task_revision FROM attempt_commits WHERE task_id=? AND attempt_id=?`, f.TaskID, f.AttemptID).Scan(&commitID, &status, &worker, &lease, &rev)
	if errors.Is(err, sql.ErrNoRows) {
		if allowMissing {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: task_id=%s attempt_id=%s", ErrCompletionAttemptNotFound, f.TaskID, f.AttemptID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: completion fence read: %w", err)
	}
	if worker != f.WorkerID || lease != f.LeaseID || rev != f.Revision {
		return nil, fmt.Errorf("%w: stored worker/lease/revision mismatch", ErrCompletionTransitionConflict)
	}
	return &CompletionAttemptState{CommitID: commitID, Status: status, TaskRevision: rev}, nil
}

func (r *sqliteCompletionTx) InsertCompletionAttempt(ctx context.Context, p CompletionDeclareParams) (string, error) {
	res, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO attempt_commits (
		commit_id,task_id,attempt_id,job_id,worker_id,lease_id,task_revision,status,
		required_output_count,commit_token_hash,commit_deadline_at,last_progress_at,created_at,updated_at
	) VALUES (?,?,?,?,?,?,?,'DECLARED',?,?,?,?,?,?)`,
		p.CommitID, p.TaskID, p.AttemptID, p.JobID, p.WorkerID, p.LeaseID, p.Revision,
		p.RequiredOutputCount, p.TokenHash, p.Deadline, p.Now, p.Now, p.Now)
	if err != nil {
		return "", fmt.Errorf("store: insert completion attempt: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 1 {
		return p.CommitID, nil
	}
	var canonical string
	if err := r.tx.QueryRowContext(ctx, `SELECT commit_id FROM attempt_commits WHERE task_id=? AND attempt_id=?`, p.TaskID, p.AttemptID).Scan(&canonical); err != nil {
		return "", fmt.Errorf("store: resolve completion attempt race: %w", err)
	}
	return canonical, nil
}

func (r *sqliteCompletionTx) InsertCompletionDeclaration(ctx context.Context, p CompletionDeclarationParams) error {
	_, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_output_declarations (
		declaration_id,commit_id,task_id,attempt_id,output_kind,logical_name,mime_type,
		expected_size_bytes,expected_sha256,worker_spool_key,status,created_at,updated_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,'DECLARED',?,?)`,
		p.DeclarationID, p.CommitID, p.TaskID, p.AttemptID, p.OutputKind, p.LogicalName, p.MimeType,
		p.SizeBytes, p.SHA256, p.WorkerSpoolKey, p.Now, p.Now)
	if err != nil {
		return fmt.Errorf("store: insert completion declaration: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) GetCompletionDeclarationID(ctx context.Context, commitID, outputKind, logicalName string) (string, error) {
	var id string
	err := r.tx.QueryRowContext(ctx, `SELECT declaration_id FROM task_output_declarations WHERE commit_id=? AND output_kind=? AND logical_name=?`, commitID, outputKind, logicalName).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("store: resolve completion declaration: %w", err)
	}
	return id, nil
}

func (r *sqliteCompletionTx) GetCompletionUploadState(ctx context.Context, uploadID string) (*CompletionUploadState, error) {
	var expected, received, mimeType sql.NullString
	var status, artifactID, storageKey string
	var size sql.NullInt64
	err := r.tx.QueryRowContext(ctx, `SELECT au.expected_sha256,au.received_sha256,au.status,au.artifact_id,au.temporary_storage_key,COALESCE(d.mime_type,a.type,''),COALESCE(au.received_size_bytes,au.expected_size_bytes,a.size_bytes,0) FROM artifact_uploads au JOIN artifacts a ON a.id=au.artifact_id LEFT JOIN task_output_declarations d ON d.upload_id=au.upload_id WHERE au.upload_id=?`, uploadID).Scan(&expected, &received, &status, &artifactID, &storageKey, &mimeType, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrCompletionAttemptNotFound, uploadID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: read completion upload: %w", err)
	}
	return &CompletionUploadState{UploadID: uploadID, ExpectedSHA256: expected.String, ReceivedSHA256: received.String, Status: status, ArtifactID: artifactID, TemporaryStorageKey: storageKey, MimeType: mimeType.String, SizeBytes: size.Int64}, nil
}

func (r *sqliteCompletionTx) CompleteCompletionUpload(ctx context.Context, verdict CompletionArtifactVerdict, uploadID, serverSHA, now string) error {
	if verdict == CompletionReady {
		if _, err := r.tx.ExecContext(ctx, `UPDATE artifact_uploads SET status='COMPLETED',completed_at=?,received_sha256=? WHERE upload_id=? AND status IN ('CREATED','UPLOADING','RECEIVED')`, now, serverSHA, uploadID); err != nil {
			return fmt.Errorf("store: completion upload ready CAS: %w", err)
		}
		if _, err := r.tx.ExecContext(ctx, `UPDATE artifacts SET status='READY',verified_at=?,output_kind=COALESCE((SELECT output_kind FROM task_output_declarations WHERE artifact_id=artifacts.id LIMIT 1),output_kind) WHERE id=(SELECT artifact_id FROM artifact_uploads WHERE upload_id=?) AND status IN ('STAGING','VERIFYING')`, now, uploadID); err != nil {
			return fmt.Errorf("store: completion artifact ready CAS: %w", err)
		}
		return nil
	}
	if verdict == CompletionKeepVerifying {
		if _, err := r.tx.ExecContext(ctx, `UPDATE artifact_uploads SET status='COMPLETED',completed_at=?,received_sha256=COALESCE(received_sha256,?) WHERE upload_id=? AND status IN ('CREATED','UPLOADING','RECEIVED')`, now, serverSHA, uploadID); err != nil {
			return fmt.Errorf("store: completion upload verifying CAS: %w", err)
		}
		if _, err := r.tx.ExecContext(ctx, `UPDATE artifacts SET status='VERIFYING',verified_at=? WHERE id=(SELECT artifact_id FROM artifact_uploads WHERE upload_id=?) AND status IN ('STAGING','VERIFYING')`, now, uploadID); err != nil {
			return fmt.Errorf("store: completion artifact verifying CAS: %w", err)
		}
		return nil
	}
	return fmt.Errorf("store: unknown completion artifact verdict %d", verdict)
}

func (r *sqliteCompletionTx) StampCompletionArtifact(ctx context.Context, artifactID, storageKey, sha string, size int64) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE artifacts SET storage_provider='local',storage_key=?,sha256=?,size_bytes=? WHERE id=?`, storageKey, sha, size, artifactID)
	if err != nil {
		if sqliteerr.IsUniqueConstraint(err) {
			return fmt.Errorf("%w: %v", ErrCompletionCanonicalConflict, err)
		}
		return fmt.Errorf("store: stamp completion artifact: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) UpdateCompletionProgress(ctx context.Context, commitID, now, deadline string) (int64, error) {
	res, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET last_progress_at=?,commit_deadline_at=?,updated_at=? WHERE commit_id=? AND status IN ('DECLARED','UPLOADING')`, now, deadline, now, commitID)
	if err != nil {
		return 0, fmt.Errorf("store: completion progress: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (r *sqliteCompletionTx) UpdateCompletionUploadedBytes(ctx context.Context, f CompletionFence, uploadID string, uploadedBytes int64, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE task_output_declarations SET uploaded_bytes=MAX(uploaded_bytes,?),updated_at=MAX(updated_at,?) WHERE commit_id IN (SELECT commit_id FROM attempt_commits WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=?) AND upload_id=?`, uploadedBytes, now, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, uploadID)
	if err != nil {
		return fmt.Errorf("store: completion uploaded bytes: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) UpdateCompletionReadyCount(ctx context.Context, f CompletionFence, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET ready_output_count=(SELECT COUNT(*) FROM task_output_declarations d JOIN artifacts a ON a.id=d.artifact_id WHERE d.commit_id=attempt_commits.commit_id AND a.status='READY'),updated_at=? WHERE commit_id IN (SELECT commit_id FROM attempt_commits WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=? AND task_revision=?) AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`, now, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, f.Revision)
	if err != nil {
		return fmt.Errorf("store: completion ready count: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) ExpireCompletionAttempt(ctx context.Context, f CompletionFence, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET status='EXPIRED',rejected_code='COMMIT_DEADLINE_EXCEEDED',rejected_message='deadline elapsed with incomplete ready set',updated_at=? WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=? AND task_revision=? AND commit_deadline_at<? AND ready_output_count<required_output_count AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`, now, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, f.Revision, now)
	if err != nil {
		return fmt.Errorf("store: expire completion attempt: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) ExpireCompletionAttemptByID(ctx context.Context, commitID, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET status='EXPIRED',rejected_code='COMMIT_DEADLINE_EXCEEDED',rejected_message='ReconcileAttempt: commit_deadline_at elapsed',updated_at=? WHERE commit_id=? AND status IN ('DECLARED','UPLOADING','RECEIVED')`, now, commitID)
	if err != nil {
		return fmt.Errorf("store: expire completion attempt by id: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) MarkCompletionCommitted(ctx context.Context, commitID, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET status='COMMITTED',committed_at=?,updated_at=? WHERE commit_id=? AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`, now, now, commitID)
	if err != nil {
		return fmt.Errorf("store: mark completion committed: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) MarkCompletionTaskAttemptSucceeded(ctx context.Context, attemptID, workerID, leaseID, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE task_attempts SET status='SUCCEEDED',completed_at=COALESCE(completed_at,?),report_version=report_version+1,updated_at=? WHERE id=? AND worker_id=? AND lease_id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`, now, now, attemptID, workerID, leaseID)
	if err != nil {
		return fmt.Errorf("store: mark completion attempt succeeded: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) MarkCompletionTaskSucceeded(ctx context.Context, taskID, attemptID, workerID, leaseID, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE tasks SET status='SUCCEEDED',completed_at=?,updated_at=?,winning_attempt_id=?,winning_attempt_committed_at=?,winning_attempt_terminal_pending=0,revision=revision+1 WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=? AND status IN ('RUNNING','LEASED')`, now, now, attemptID, now, taskID, attemptID, workerID, leaseID)
	if err != nil {
		return fmt.Errorf("store: mark completion task succeeded: %w", err)
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
	if artifactContract && status != "AWAITING_ARTIFACT" {
		return fmt.Errorf("%w: completion job %s must be AWAITING_ARTIFACT before SUCCEEDED (status=%s)", ErrCompletionTransitionConflict, jobID, status)
	}
	if !artifactContract && status != "RUNNING" && status != "AWAITING_ARTIFACT" {
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
	rows, err := r.tx.QueryContext(ctx, `SELECT a.id,p.destination_id FROM artifacts a CROSS JOIN job_delivery_plans p WHERE a.job_id=? AND p.job_id=? AND a.status='READY' AND (a.output_kind='final_video' OR (a.output_kind='' AND a.type IN ('video','final_video'))) AND p.enabled=1`, jobID, jobID)
	if err != nil {
		return fmt.Errorf("store: completion delivery query: %w", err)
	}
	defer rows.Close()
	type key struct{ a, d string }
	seen := map[key]bool{}
	for rows.Next() {
		var a, d string
		if err := rows.Scan(&a, &d); err != nil {
			return fmt.Errorf("store: completion delivery scan: %w", err)
		}
		if a == "" || d == "" || seen[key{a, d}] {
			continue
		}
		seen[key{a, d}] = true
		if _, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO job_deliveries (delivery_id,artifact_id,destination_id,status,idempotency_key,created_at,updated_at) VALUES (?,?,?,'PENDING',?,?,?)`, `jbd_comp_`+a+`_`+d, a, d, a+`_`+d, now, now); err != nil {
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

func (s *SQLiteCompletionStore) ListCompletionUploadBindings(ctx context.Context, commitID string) ([]CompletionUploadBinding, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT d.declaration_id,d.commit_id,COALESCE(d.upload_id,''),COALESCE(d.artifact_id,''),d.task_id,d.attempt_id,ac.worker_id,ac.lease_id,ac.task_revision,d.output_kind,d.logical_name FROM task_output_declarations d JOIN attempt_commits ac ON ac.commit_id=d.commit_id WHERE d.commit_id=? ORDER BY d.rowid`, commitID)
	if err != nil {
		return nil, fmt.Errorf("store: list completion bindings: %w", err)
	}
	defer rows.Close()
	var out []CompletionUploadBinding
	for rows.Next() {
		var b CompletionUploadBinding
		if err := rows.Scan(&b.DeclarationID, &b.CommitID, &b.UploadID, &b.ArtifactID, &b.TaskID, &b.AttemptID, &b.WorkerID, &b.LeaseID, &b.Revision, &b.OutputKind, &b.LogicalName); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *SQLiteCompletionStore) GetCompletionUploadBinding(ctx context.Context, uploadID string) (*CompletionUploadBinding, error) {
	var b CompletionUploadBinding
	err := s.db.QueryRowContext(ctx, `SELECT d.declaration_id,d.commit_id,d.upload_id,COALESCE(d.artifact_id,''),d.task_id,d.attempt_id,ac.worker_id,ac.lease_id,ac.task_revision,d.output_kind,d.logical_name FROM task_output_declarations d JOIN attempt_commits ac ON ac.commit_id=d.commit_id WHERE d.upload_id=?`, uploadID).Scan(&b.DeclarationID, &b.CommitID, &b.UploadID, &b.ArtifactID, &b.TaskID, &b.AttemptID, &b.WorkerID, &b.LeaseID, &b.Revision, &b.OutputKind, &b.LogicalName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: completion upload binding not found: %w", ErrCompletionAttemptNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("store: get completion binding: %w", err)
	}
	return &b, nil
}

func (s *SQLiteCompletionStore) BindCompletionUpload(ctx context.Context, declarationID, uploadID, artifactID string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE task_output_declarations SET upload_id=?,artifact_id=?,updated_at=? WHERE declaration_id=? AND (upload_id IS NULL OR upload_id=?) AND (artifact_id IS NULL OR artifact_id=?)`, uploadID, artifactID, time.Now().UTC().Format(time.RFC3339Nano), declarationID, uploadID, artifactID)
	if err != nil {
		return fmt.Errorf("store: bind completion upload: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: bind completion upload rows affected: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("%w: declaration_id=%s", ErrCompletionBindingConflict, declarationID)
	}
	return nil
}

func (s *SQLiteCompletionStore) GetCompletionCommitTokenHash(ctx context.Context, commitID string) (string, error) {
	var h string
	err := s.db.QueryRowContext(ctx, `SELECT commit_token_hash FROM attempt_commits WHERE commit_id=?`, commitID).Scan(&h)
	if err != nil {
		return "", fmt.Errorf("store: get completion token hash: %w", err)
	}
	return h, nil
}

func (s *SQLiteCompletionStore) ScanCompletionCandidates(ctx context.Context, now, deadlineCutoff, progressCutoff, outboxCutoff string, limit int) ([]CompletionReconcileCandidate, int64, error) {
	q := `SELECT commit_id,case_label FROM (
SELECT commit_id,'deadline_expired' AS case_label FROM attempt_commits WHERE status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING') AND commit_deadline_at<?
UNION ALL SELECT ac.commit_id,'orphan_terminal_task' FROM attempt_commits ac JOIN task_attempts ta ON ta.id=ac.attempt_id WHERE ta.status IN ('FAILED','CANCELLED','TIMED_OUT') AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
UNION ALL SELECT ac.commit_id,'stale_fence' FROM attempt_commits ac JOIN tasks t ON t.task_id=ac.task_id WHERE t.lease_id!=ac.lease_id AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
UNION ALL SELECT ac.commit_id,'missing_worker' FROM attempt_commits ac LEFT JOIN workers w ON w.worker_id=ac.worker_id WHERE w.worker_id IS NULL AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
UNION ALL SELECT ac.commit_id,'missing_declarations' FROM attempt_commits ac LEFT JOIN (SELECT commit_id,COUNT(*) n FROM task_output_declarations GROUP BY commit_id) d ON d.commit_id=ac.commit_id WHERE ac.status='UPLOADING' AND COALESCE(d.n,0)=0
UNION ALL SELECT ac.commit_id,'missing_commit' FROM attempt_commits ac WHERE ac.status='RECEIVED' AND ac.required_output_count>0 AND ac.ready_output_count>=ac.required_output_count AND ac.last_progress_at<?
UNION ALL SELECT ac.commit_id,'upload_stuck' FROM attempt_commits ac WHERE ac.status='UPLOADING' AND ac.last_progress_at<?
UNION ALL SELECT ac.commit_id,'fence_expired' FROM attempt_commits ac JOIN tasks t ON t.task_id=ac.task_id WHERE t.lease_id!='' AND t.worker_id='' AND ac.status='DECLARED'
UNION ALL SELECT ac.commit_id,'outbox_pending_too_long' FROM attempt_commits ac JOIN outbox_events oe ON oe.aggregate_type='task' AND oe.aggregate_id=ac.task_id AND oe.event_type='commit_protocol.committed' AND oe.payload_json LIKE '%'||ac.commit_id||'%' WHERE oe.status='PENDING' AND oe.created_at<?
UNION ALL SELECT ac.commit_id,'required_outputs_missing' FROM attempt_commits ac WHERE ac.status='AWAITING_REQUIRED' AND ac.required_output_count>ac.ready_output_count
UNION ALL SELECT ac.commit_id,'job_all_succeeded_no_job_deliveries' FROM attempt_commits ac JOIN tasks t ON t.task_id=ac.task_id WHERE t.status='SUCCEEDED' AND ac.status='COMMITTED' AND NOT EXISTS (SELECT 1 FROM artifacts a JOIN job_deliveries jd ON jd.artifact_id=a.id WHERE a.job_id=ac.job_id)
) ORDER BY commit_id LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, now, deadlineCutoff, progressCutoff, outboxCutoff, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("store: scan completion candidates: %w", err)
	}
	defer rows.Close()
	var out []CompletionReconcileCandidate
	var deadline int64
	for rows.Next() {
		var c CompletionReconcileCandidate
		if err := rows.Scan(&c.CommitID, &c.Case); err != nil {
			return nil, 0, err
		}
		if c.Case == "deadline_expired" {
			deadline++
		}
		out = append(out, c)
	}
	return out, deadline, rows.Err()
}

var _ CompletionStore = (*SQLiteCompletionStore)(nil)
