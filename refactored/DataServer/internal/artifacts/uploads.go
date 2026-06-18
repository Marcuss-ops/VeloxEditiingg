package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// BeginUploadCommand is the input to Service.BeginUpload (Fase 1).
//
// The worker presents this BEFORE the bytes are streamed. The master
// uses the auth fields (worker_id, lease_id, attempt_number,
// expected_revision) and the in-memory job/attempt state to authorize
// the upload. The hint fields (kind, mime, expected_size, expected_sha)
// are NEVER trusted as authoritative — they are stored for diagnostics
// and surfaced to the worker only when the master disagrees.
type BeginUploadCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	// Worker-declared hints (diagnostic only).
	Kind              string
	MimeType          string
	ExpectedSizeBytes int64
	ExpectedSHA256    string
}

// FinalizeArtifactCommand is the master-side adapter from the gRPC
// ArtifactUploaded message. It carries ONLY the IDs / auth fields —
// the legacy `artifact_path`, `artifact_size`, `sha256` fields are
// ignored because they cannot be trusted (see PR 2 spec, Fase 4).
type FinalizeArtifactCommand struct {
	UploadID         string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
}

// FinalizeArtifactAndCompleteJobCommand is the single transactional
// input to FinalizeArtifactAndCompleteJob. It carries the master-
// computed values (storage_key, sha, size, mime) that were derived
// in Receive() along with the authorization fields for the CAS gates.
//
// SQL tx order (see PR 2 spec, Fase 4):
//  1. SELECT artifact_uploads -> ensure RECEIVED or FINALIZING
//  2. UPDATE jobs WHERE status=RUNNING AND assigned_to=? AND lease_id=?
//     AND revision=?              (CAS, must affect exactly 1 row)
//  3. UPDATE artifacts WHERE id=? AND job_id=? AND status='STAGING'
//                                  (CAS, must affect exactly 1 row)
//  4. UPDATE job_attempts WHERE job_id=? AND attempt_number=? AND
//     worker_id=? AND lease_id=? AND status='RENDER_FINISHED'
//                                  (CAS, must affect exactly 1 row)
//  5. INSERT outbox_events   (ARTIFACT_READY, JOB_SUCCEEDED)
//  6. INSERT job_deliveries  ON CONFLICT DO NOTHING  (idempotent)
//  7. INSERT outbox_events   (DELIVERY_CREATED)
type FinalizeArtifactAndCompleteJobCommand struct {
	UploadID         string
	ArtifactID       string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	// Master-computed values from Receive().
	StorageProvider string
	StorageKey      string
	SHA256          string
	SizeBytes       int64
	MIMEType        string

	VerifiedAt time.Time
}

// UploadSession is the persistent state of one upload.
//
// Receive and Finalize mutate it through Repository.UpdateUploadStatus.
// CreatedAt is server time; ExpiresAt is CreatedAt + Service.uploadTTL
// (default 24h, matching the spec's "blob finale senza riga DB dopo 24h"
// reconciler rule).
type UploadSession struct {
	UploadID     string
	ArtifactID   string
	JobID        string
	WorkerID     string
	LeaseID      string
	AttemptNumber    int
	ExpectedRevision int

	Kind         string
	ExpectedMIME string

	TemporaryStorageKey string

	ExpectedSizeBytes int64
	ExpectedSHA256    string

	ReceivedSizeBytes int64
	ReceivedSHA256    string

	// CREATED | UPLOADING | RECEIVED | FINALIZING | COMPLETED | FAILED | EXPIRED.
	Status    string
	CreatedAt time.Time
	ExpiresAt time.Time
	// Equals zero value when the session has not been completed.
	CompletedAt time.Time
}

// ReceiveResult is what Service.Receive returns after streaming bytes.
//
// The master-computed hash + size are stored on the row, and Status
// moves to RECEIVED so the next Finalize call can re-hash from disk
// (defence-in-depth in case Receive and the worker's Finalize report
// race or arrive out of order).
type ReceiveResult struct {
	UploadID          string
	ReceivedSizeBytes int64
	ReceivedSHA256    string
	Status            string
}

// UploadFields lets the caller update a subset of an UploadSession row.
// Each pointer is optional: nil leaves the column untouched. Status is
// required for any UpdateUploadStatus call (state machine must advance).
type UploadFields struct {
	Status            *string
	ReceivedSizeBytes *int64
	ReceivedSHA256    *string
	CompletedAt       *time.Time
}

// Repository is the narrow persistence contract for artifact_uploads rows.
// All methods treat upload_id as the canonical key. Application-level
// invariants (status state machine) live in Service — SQL CHECK constraints
// only block blatantly malformed rows.
type Repository interface {
	CreateUploadSession(ctx context.Context, session *UploadSession) error
	GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error)
	UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error
	DeleteUploadSession(ctx context.Context, uploadID string) error
	FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error)

	// TransitionUploadStatus atomically CAS-flips status from `from`
	// to `to`. Returns ErrUploadStateInvalid when 0 rows are affected
	// (row missing OR source status doesn't match). Used by Finalize
	// to serialize concurrent finalize callers at the SQL layer.
	TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error
}

// SQLITE IMPLEMENTATION
//
// SQLiteRepository implements Repository against a *sql.DB.
// SQLite serializes writers, so concurrent Create/Update on the same
// row are race-free; the application layer in Service enforces the
// state-machine legality.

type SQLiteRepository struct {
	db *sql.DB
}

// NewSQLiteRepository wraps an existing *sql.DB. The caller owns the
// connection (typically the same one used by store.SQLiteStore so
// FinalizeArtifactAndCompleteJob can join its tx via the same *sql.DB).
func NewSQLiteRepository(db *sql.DB) *SQLiteRepository {
	return &SQLiteRepository{db: db}
}

// Compile-time interface check.
var _ Repository = (*SQLiteRepository)(nil)

// CreateUploadSession inserts a new session row in CREATED.
//
// Caller must pre-fill UploadID/ArtifactID/JobID/CreatedAt/ExpiresAt;
// Status defaults to CREATED when blank.
func (r *SQLiteRepository) CreateUploadSession(ctx context.Context, s *UploadSession) error {
	if s == nil {
		return fmt.Errorf("artifacts: CreateUploadSession: nil session")
	}
	if s.UploadID == "" || s.ArtifactID == "" || s.JobID == "" {
		return fmt.Errorf("artifacts: CreateUploadSession: upload_id, artifact_id and job_id are required")
	}
	if s.Status == "" {
		s.Status = "CREATED"
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO artifact_uploads (
			upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
			status, temporary_storage_key,
			expected_size_bytes, expected_sha256,
			received_size_bytes, received_sha256,
			created_at, expires_at, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.UploadID, s.ArtifactID, s.JobID, s.AttemptNumber, s.WorkerID, s.LeaseID,
		s.Status, s.TemporaryStorageKey,
		s.ExpectedSizeBytes, nilOrString(s.ExpectedSHA256),
		s.ReceivedSizeBytes, nilOrString(s.ReceivedSHA256),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.ExpiresAt.UTC().Format(time.RFC3339),
		formatTimeNullable(s.CompletedAt),
	)
	if err != nil {
		return fmt.Errorf("artifacts: CreateUploadSession: %w", err)
	}
	return nil
}

// GetUploadSession returns a session by ID, or (nil, nil) when missing.
func (r *SQLiteRepository) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: GetUploadSession: empty uploadID")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       created_at, expires_at, completed_at
		FROM artifact_uploads WHERE upload_id = ?`, uploadID)

	var s UploadSession
	var createdAt, expiresAt string
	var completedAt sql.NullString
	if err := row.Scan(
		&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
		&s.Status, &s.TemporaryStorageKey,
		&s.ExpectedSizeBytes, &s.ExpectedSHA256,
		&s.ReceivedSizeBytes, &s.ReceivedSHA256,
		&createdAt, &expiresAt, &completedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: GetUploadSession: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, fmt.Errorf("artifacts: GetUploadSession: invalid created_at: %w", err)
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, fmt.Errorf("artifacts: GetUploadSession: invalid expires_at: %w", err)
	}
	if completedAt.Valid {
		if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
			return nil, fmt.Errorf("artifacts: GetUploadSession: invalid completed_at: %w", err)
		}
	}
	return &s, nil
}

// UpdateUploadStatus applies UploadFields atomically. Status is
// required. RowsAffected is checked: must be 1 for success, otherwise
// ErrUploadStateInvalid wraps the actual affected count.
func (r *SQLiteRepository) UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error {
	if uploadID == "" {
		return fmt.Errorf("artifacts: UpdateUploadStatus: empty uploadID")
	}
	if fields.Status == nil {
		return fmt.Errorf("artifacts: UpdateUploadStatus: status is required")
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = ?,
		    received_size_bytes = COALESCE(?, received_size_bytes),
		    received_sha256    = COALESCE(?, received_sha256),
		    completed_at       = COALESCE(?, completed_at)
		WHERE upload_id = ?`,
		*fields.Status,
		fields.ReceivedSizeBytes,
		nilOrStringPtr(fields.ReceivedSHA256),
		formatTimePtr(fields.CompletedAt),
		uploadID,
	)
	if err != nil {
		return fmt.Errorf("artifacts: UpdateUploadStatus: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: upload=%s affected=%d", ErrUploadStateInvalid, uploadID, n)
	}
	return nil
}

// TransitionUploadStatus atomically CAS-flips the upload session
// status from `from` to `to`. Returns ErrUploadStateInvalid when 0
// rows are affected (row missing OR the source status does not
// match). Used by Service.Finalize to serialize concurrent
// finalize callers at the SQL layer.
func (r *SQLiteRepository) TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error {
	if uploadID == "" || from == "" || to == "" {
		return fmt.Errorf("artifacts: TransitionUploadStatus: missing required arg")
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE artifact_uploads SET status = ? WHERE upload_id = ? AND status = ?`,
		to, uploadID, from)
	if err != nil {
		return fmt.Errorf("artifacts: TransitionUploadStatus: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: upload=%s from=%s to=%s affected=%d",
			ErrUploadStateInvalid, uploadID, from, to, n)
	}
	return nil
}

// DeleteUploadSession removes the session row. Reconciler calls this
// after EXPIRED cleanup or after COMPLETED retention window.
func (r *SQLiteRepository) DeleteUploadSession(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("artifacts: DeleteUploadSession: empty uploadID")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM artifact_uploads WHERE upload_id = ?`, uploadID); err != nil {
		return fmt.Errorf("artifacts: DeleteUploadSession: %w", err)
	}
	return nil
}

// FindStuckStaging returns CREATED/UPLOADING/FINALIZING sessions whose
// created_at is older than `olderThan`. The reconciler uses this list
// to mark them FAILED/EXPIRED. We keep the old sessions alive in DB
// (rather than delete) so audit trails survive until DeleteUploadSession
// is later called by a retention pass.
func (r *SQLiteRepository) FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       created_at, expires_at, completed_at
		FROM artifact_uploads
		WHERE status IN ('CREATED', 'UPLOADING', 'FINALIZING')
		  AND created_at < ?
		ORDER BY created_at ASC
		LIMIT ?`, olderThan.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FindStuckStaging: %w", err)
	}
	defer rows.Close()

	var out []UploadSession
	for rows.Next() {
		var s UploadSession
		var createdAt, expiresAt string
		var completedAt sql.NullString
		if err := rows.Scan(
			&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
			&s.Status, &s.TemporaryStorageKey,
			&s.ExpectedSizeBytes, &s.ExpectedSHA256,
			&s.ReceivedSizeBytes, &s.ReceivedSHA256,
			&createdAt, &expiresAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("artifacts: FindStuckStaging scan: %w", err)
		}
		if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
			return nil, fmt.Errorf("artifacts: FindStuckStaging: invalid created_at: %w", err)
		}
		if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
			return nil, fmt.Errorf("artifacts: FindStuckStaging: invalid expires_at: %w", err)
		}
		if completedAt.Valid {
			if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
				return nil, fmt.Errorf("artifacts: FindStuckStaging: invalid completed_at: %w", err)
			}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifacts: FindStuckStaging rows: %w", err)
	}
	return out, nil
}

// nilOrString maps "" -> nil so the column stores NULL rather than "",
// matching the migration's nullable TEXT columns for expected_sha256 /
// received_sha256.
func nilOrString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nilOrStringPtr(p *string) interface{} {
	if p == nil || *p == "" {
		return nil
	}
	return *p
}

func formatTimeNullable(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func formatTimePtr(p *time.Time) interface{} {
	if p == nil || p.IsZero() {
		return nil
	}
	return p.UTC().Format(time.RFC3339)
}

func parseTimeRFC3339(t *time.Time, raw string) error {
	if raw == "" {
		*t = time.Time{}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}
