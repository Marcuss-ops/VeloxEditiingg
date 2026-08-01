package store

// sqlite_task_atomic_persistence_task.go contains the task-entity write
// helpers used by IngestTaskResultAtomic. Each helper receives the
// coordinator-owned transaction; none opens, commits, or rolls back a
// transaction. Split out of sqlite_task_atomic_persistence_helpers.go.

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/taskgraph"
)

// persistOutputArtifacts registers declared output artifacts, skipping
// duplicates on UNIQUE conflict.
func persistOutputArtifacts(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	for _, a := range cmd.Artifacts {
		if a.ArtifactID == "" {
			continue
		}
		metadata := a.MetadataJSON
		if metadata == "" {
			metadata = "{}"
		}
		_, artErr := tx.ExecContext(ctx,
			`INSERT INTO task_output_artifacts
			 (task_id, attempt_id, artifact_id, artifact_type, declared_path,
			  declared_size, declared_sha256, metadata_json, registered_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.TaskID, a.AttemptID, a.ArtifactID, a.ArtifactType, a.DeclaredPath,
			a.DeclaredSize, a.DeclaredSHA256, metadata, now,
		)
		if artErr != nil {
			if isUniqueConflict(artErr) {
				continue
			}
			return fmt.Errorf("task ingest atomic artifact %s: %w", a.ArtifactID, artErr)
		}
	}
	return nil
}
