package store

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/taskgraph"
)

// CreateTasksAtomic persists a validated fan-out as one SQLite transaction.
// The taskgraph package owns validation; this method owns only the storage
// boundary so a process crash cannot leave a half-created DAG.
func (r *SQLiteTaskRepository) CreateTasksAtomic(ctx context.Context, tasks []taskgraph.Task) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if len(tasks) == 0 {
		return fmt.Errorf("task repository: atomic create requires tasks")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("task atomic create begin", err)
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339)
	for _, task := range tasks {
		dependsOnJSON, marshalErr := taskgraph.MarshalDependsOn(task.DependsOn)
		if marshalErr != nil {
			return fmt.Errorf("task atomic create %s: depends_on: %w", task.ID, marshalErr)
		}
		status := task.Status
		if status == "" {
			status = taskgraph.StatusPending
		}
		if task.ID == "" {
			return fmt.Errorf("task atomic create: task id is required")
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO tasks (
				task_id, job_id, project_id, render_plan_id,
				executor_id, executor_version, status, priority,
				revision, attempt_count, worker_id, lease_id,
				created_at, updated_at, depends_on
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', '', ?, ?, ?)`,
			task.ID, task.JobID, task.ProjectID, task.RenderPlanID,
			task.ExecutorID, task.ExecutorVersion, string(status), task.Priority,
			now, now, dependsOnJSON,
		); err != nil {
			return wrapDBInfrastructure("task atomic create", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("task atomic create commit", err)
	}
	return nil
}
