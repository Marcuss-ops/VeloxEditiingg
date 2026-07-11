package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// --- Job Attempts ---
//
// cleanup/remove-job-attempts-runtime: the runtime writer surface
// (`InsertJobAttempt`, `InsertJobAttemptTx`, `UpdateJobAttemptStatus`)
// was removed. Per-attempt identity now lives in `task_attempts`
// (canonical layer; task_attempts_writer), so any caller attempting
// to insert / update a row on `job_attempts` is a regression — the
// scan_test guard in `internal/artifacts/scan_test.go::TestNoJobAttemptsWriter`
// enforces this at CI time. Any future reintroduction will trip
// that test on the next CI run.
//
// The READ surface (`GetJobAttempts`, `GetLatestJobAttempt`,
// `JobAttempt` struct) is preserved as a thin compatibility shim for
// the postgres path, which still joins `job_attempts.started_at` from
// `internal/store/postgres_jobs_repository.go` (RequeueExpiredLeases
// path is being decommissioned alongside PR-07 / job protocol removal).
type JobAttempt struct {
	ID            int    `json:"id"`
	JobID         string `json:"job_id"`
	AttemptNumber int    `json:"attempt_number"`
	WorkerID      string `json:"worker_id"`
	LeaseID       string `json:"lease_id"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at,omitempty"`
	FinishedAt    string `json:"finished_at,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	EngineVersion string `json:"engine_version,omitempty"`
	BundleHash    string `json:"bundle_hash,omitempty"`
	CreatedAt     string `json:"created_at"`
}

func (s *SQLiteStore) GetJobAttempts(jobID string, limit int) ([]JobAttempt, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.Query(
		`SELECT id, job_id, attempt_number, worker_id, lease_id, status,
		        COALESCE(started_at,''), COALESCE(finished_at,''),
		        COALESCE(error_code,''), COALESCE(engine_version,''), COALESCE(bundle_hash,''),
		        created_at
		 FROM job_attempts WHERE job_id=? ORDER BY attempt_number DESC LIMIT ?`,
		jobID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []JobAttempt
	for rows.Next() {
		var a JobAttempt
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptNumber, &a.WorkerID, &a.LeaseID,
			&a.Status, &a.StartedAt, &a.FinishedAt, &a.ErrorCode, &a.EngineVersion,
			&a.BundleHash, &a.CreatedAt); err != nil {
			continue
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

func (s *SQLiteStore) GetLatestJobAttempt(jobID string) (*JobAttempt, error) {
	attempts, err := s.GetJobAttempts(jobID, 1)
	if err != nil {
		return nil, err
	}
	if len(attempts) == 0 {
		return nil, nil
	}
	return &attempts[0], nil
}

// --- Artifacts ---

// Artifact represents a produced output (video, audio, etc.) with storage abstraction.
type Artifact struct {
	ID              string  `json:"id"`
	JobID           string  `json:"job_id"`
	AttemptID       int     `json:"attempt_id,omitempty"`
	Type            string  `json:"type"`
	StorageProvider string  `json:"storage_provider"`
	StorageKey      string  `json:"storage_key,omitempty"`
	StorageURL      string  `json:"storage_url,omitempty"`
	LocalPath       string  `json:"local_path,omitempty"`
	SHA256          string  `json:"sha256,omitempty"`
	SizeBytes       int64   `json:"size_bytes"`
	DurationSeconds float64 `json:"duration_seconds,omitempty"`
	DurationMs      int64   `json:"duration_ms,omitempty"`
	Status          string  `json:"status"`
	VerifiedAt      string  `json:"verified_at,omitempty"`
	MimeType        string  `json:"mime_type,omitempty"`
	CreatedAt       string  `json:"created_at"`
}

func (s *SQLiteStore) InsertArtifact(artifact *Artifact) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if artifact.CreatedAt == "" {
		artifact.CreatedAt = now
	}
	if artifact.ID == "" {
		artifact.ID = fmt.Sprintf("artifact_%d", time.Now().UnixNano())
	}
	if artifact.Status == "" {
		artifact.Status = "pending"
	}
	if artifact.StorageProvider == "" {
		artifact.StorageProvider = "local"
	}

	_, err := s.db.Exec(
		`INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider, storage_key,
	                        storage_url, local_path, sha256, size_bytes, duration_seconds,
	                        duration_ms, mime_type, verified_at, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		artifact.ID, artifact.JobID, nullInt(artifact.AttemptID), artifact.Type,
		artifact.StorageProvider, artifact.StorageKey, artifact.StorageURL,
		artifact.LocalPath, artifact.SHA256, artifact.SizeBytes,
		artifact.DurationSeconds, artifact.DurationMs,
		nullIfEmpty(artifact.MimeType), nullIfEmpty(artifact.VerifiedAt),
		artifact.Status, artifact.CreatedAt,
	)
	return err
}

func (s *SQLiteStore) FinalizeArtifact(id, status, storageURL string) error {
	_, err := s.db.Exec(
		`UPDATE artifacts SET status=?, storage_url=? WHERE id=?`,
		status, storageURL, id,
	)
	return err
}

func (s *SQLiteStore) GetArtifact(id string) (*Artifact, error) {
	row := s.db.QueryRow(
		`SELECT id, job_id, COALESCE(attempt_id,0), type, storage_provider,
		        COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(local_path,''),
		        COALESCE(sha256,''), COALESCE(size_bytes,0), COALESCE(duration_seconds,0.0),
		        COALESCE(duration_ms,0), COALESCE(mime_type,''), COALESCE(verified_at,''),
		        status, created_at
		 FROM artifacts WHERE id=?`, id,
	)
	var a Artifact
	err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds,
		&a.DurationMs, &a.MimeType, &a.VerifiedAt,
		&a.Status, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *SQLiteStore) GetArtifactsByJob(jobID string, limit int) ([]Artifact, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.Query(
		`SELECT id, job_id, COALESCE(attempt_id,0), type, storage_provider,
		        COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(local_path,''),
		        COALESCE(sha256,''), COALESCE(size_bytes,0), COALESCE(duration_seconds,0.0),
		        COALESCE(duration_ms,0), COALESCE(mime_type,''), COALESCE(verified_at,''),
		        status, created_at
		 FROM artifacts WHERE job_id=? ORDER BY created_at DESC LIMIT ?`,
		jobID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var artifacts []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
			&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
			&a.SizeBytes, &a.DurationSeconds,
			&a.DurationMs, &a.MimeType, &a.VerifiedAt,
			&a.Status, &a.CreatedAt); err != nil {
			continue
		}
		artifacts = append(artifacts, a)
	}
	return artifacts, rows.Err()
}

// --- Integration helpers (event logging) ---

// LogJobEvent inserts a structured event into job_events with automatic raw_json.
func (s *SQLiteStore) LogJobEvent(jobID, eventType string, extra map[string]interface{}) error {
	now := time.Now().UTC().Format(time.RFC3339)
	payload := map[string]interface{}{
		"event":     eventType,
		"job_id":    jobID,
		"timestamp": now,
	}
	for k, v := range extra {
		payload[k] = v
	}
	raw, _ := json.Marshal(payload)
	return s.InsertJobEvent(now, jobID, eventType, string(raw))
}

// UpdateArtifactStorageKey updates the storage_key and storage_url/local_path
// after an artifact has been promoted from staging to final storage.
func (s *SQLiteStore) UpdateArtifactStorageKey(ctx context.Context, artifactID, storageKey, localPath string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE artifacts SET storage_key=?, local_path=? WHERE id=?`,
		storageKey, localPath, artifactID,
	)
	return err
}

func nullInt(v int) interface{} {
	if v == 0 {
		return nil
	}
	return v
}
