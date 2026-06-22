package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/taskoutput_artifacts"
)

// SQLiteTaskOutputArtifactsRepository is the backing store for
// task_output_artifacts. Persists worker-declared artifact descriptors
// captured at TaskResult ingestion (1501 / feat/task-report-ingestion).
//
// Idempotency: Register has a UNIQUE(task_id, artifact_id) constraint.
// A replay against the same tuple returns ErrAlreadyRegistered.
//
// Construction: NewSQLiteTaskOutputArtifactsRepository(p.SQLite).
type SQLiteTaskOutputArtifactsRepository struct {
	store *SQLiteStore
}

// NewSQLiteTaskOutputArtifactsRepository constructs the canonical
// repository backed by the shared *SQLiteStore.
func NewSQLiteTaskOutputArtifactsRepository(s *SQLiteStore) *SQLiteTaskOutputArtifactsRepository {
	if s == nil {
		panic("store.NewSQLiteTaskOutputArtifactsRepository: SQLiteStore is required")
	}
	return &SQLiteTaskOutputArtifactsRepository{store: s}
}

// Register inserts a single output artifact declaration.
//
// Returns taskoutput_artifacts.ErrAlreadyRegistered on UNIQUE
// conflict (task_id, artifact_id) — Ingest translates that into a
// counted skip, NOT an error.
func (r *SQLiteTaskOutputArtifactsRepository) Register(ctx context.Context, a taskoutput_artifacts.OutputArtifact) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("taskoutput_artifacts.Register: store not initialized")
	}
	if a.TaskID == "" {
		return fmt.Errorf("taskoutput_artifacts.Register: empty TaskID")
	}
	if a.ArtifactID == "" {
		return fmt.Errorf("taskoutput_artifacts.Register: empty ArtifactID for task %s", a.TaskID)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	metadata := a.MetadataJSON
	if metadata == "" {
		metadata = "{}"
	}

	_, err := r.store.db.ExecContext(ctx,
		`INSERT INTO task_output_artifacts
		 (task_id, attempt_id, artifact_id, artifact_type, declared_path,
		  declared_size, declared_sha256, metadata_json, registered_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.TaskID, a.AttemptID, a.ArtifactID, a.ArtifactType, a.DeclaredPath,
		a.DeclaredSize, a.DeclaredSHA256, metadata, now,
	)
	if err != nil {
		if isUniqueConflict(err) {
			return taskoutput_artifacts.ErrAlreadyRegistered
		}
		return fmt.Errorf("taskoutput_artifacts.Register: %w", err)
	}
	return nil
}

// ListByTask returns all declarations for one task, ordered by id ascending.
func (r *SQLiteTaskOutputArtifactsRepository) ListByTask(ctx context.Context, taskID string) ([]taskoutput_artifacts.OutputArtifact, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("taskoutput_artifacts.ListByTask: store not initialized")
	}
	if taskID == "" {
		return nil, fmt.Errorf("taskoutput_artifacts.ListByTask: empty taskID")
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT task_id, attempt_id, artifact_id, artifact_type, declared_path,
		        declared_size, declared_sha256, metadata_json
		 FROM task_output_artifacts
		 WHERE task_id = ?
		 ORDER BY id ASC`,
		taskID,
	)
	if err != nil {
		return nil, fmt.Errorf("taskoutput_artifacts.ListByTask: query: %w", err)
	}
	defer rows.Close()
	var out []taskoutput_artifacts.OutputArtifact
	for rows.Next() {
		var a taskoutput_artifacts.OutputArtifact
		var attemptID, artType, path, sha, meta sql.NullString
		var size sql.NullInt64
		if err := rows.Scan(&a.TaskID, &attemptID, &a.ArtifactID, &artType, &path,
			&size, &sha, &meta); err != nil {
			return nil, fmt.Errorf("taskoutput_artifacts.ListByTask: scan: %w", err)
		}
		a.AttemptID = attemptID.String
		a.ArtifactType = artType.String
		a.DeclaredPath = path.String
		a.DeclaredSize = size.Int64
		a.DeclaredSHA256 = sha.String
		a.MetadataJSON = meta.String
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("taskoutput_artifacts.ListByTask: rows: %w", err)
	}
	return out, nil
}

// Repository exposes the implementation as the canonical
// taskoutput_artifacts.Repository so callers can depend on the interface
// rather than the concrete type.
func (r *SQLiteTaskOutputArtifactsRepository) Repository() taskoutput_artifacts.Repository {
	return r
}

// isUniqueConflict does a substring sniff for SQLite's UNIQUE constraint
// violation. Cheaper than parsing the error code path; matches the
// existing test helpers' style in this repo.
func isUniqueConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		errors.Is(err, sql.ErrNoRows) == false && strings.Contains(msg, "constraint failed: UNIQUE")
}
