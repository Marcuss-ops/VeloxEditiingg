package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/taskgraph"
	"velox-shared/taskcontract"
)

var _ taskgraph.AtomicTaskGraphExtender = (*SQLiteTaskRepository)(nil)

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
		spec := &taskcontract.TaskSpec{
			Version:    taskcontract.SpecVersion,
			JobID:      task.JobID,
			ExecutorID: task.ExecutorID,
			Payload:    task.SpecPayload,
		}
		if err := spec.Validate(); err != nil {
			return fmt.Errorf("task atomic create %s: task spec: %w", task.ID, err)
		}
		payloadJSON, err := marshalSpecPayload(spec)
		if err != nil {
			return fmt.Errorf("task atomic create %s: encode task spec: %w", task.ID, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			task.ID, spec.Version, spec.MustSpecHash(), spec.ExecutorID, payloadJSON, now,
		); err != nil {
			return wrapDBInfrastructure("task atomic create spec", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("task atomic create commit", err)
	}
	return nil
}

// ExtendTaskGraphAtomic persists preparation task rows/specs, attaches them
// to an existing assembly task, and patches its durable spec atomically.
func (r *SQLiteTaskRepository) ExtendTaskGraphAtomic(ctx context.Context, parentTaskID string, tasks []taskgraph.Task, dependencies []string, patch map[string]interface{}) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("task graph extension: store not initialized")
	}
	if parentTaskID == "" || len(tasks) == 0 || len(dependencies) == 0 {
		return fmt.Errorf("task graph extension: parent, additions and dependencies are required")
	}
	conn, err := r.store.db.Conn(ctx)
	if err != nil {
		return wrapDBInfrastructure("task graph extension connection", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return wrapDBInfrastructure("task graph extension begin", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var jobID, status, workerID, rawPayload, parentExecutor string
	var specVersion int
	if err := conn.QueryRowContext(ctx, `
		SELECT t.job_id,t.status,COALESCE(t.worker_id,''),t.executor_id,
		       s.spec_version,COALESCE(s.payload_json,'{}')
		FROM tasks t JOIN task_specs s ON s.task_id=t.task_id
		WHERE t.task_id=?`, parentTaskID).Scan(&jobID, &status, &workerID, &parentExecutor, &specVersion, &rawPayload); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("task graph extension: parent task %q not found", parentTaskID)
		}
		return wrapDBInfrastructure("task graph extension parent read", err)
	}
	if (status != string(taskgraph.StatusPending) && status != string(taskgraph.StatusReady)) || workerID != "" {
		return fmt.Errorf("task graph extension: parent is no longer unleased")
	}
	payload := make(map[string]interface{})
	if rawPayload != "" && rawPayload != "{}" {
		if err := json.Unmarshal([]byte(rawPayload), &payload); err != nil {
			return fmt.Errorf("task graph extension: decode parent spec: %w", err)
		}
	}
	if pending, _ := payload["runtime_assets_pending"].(bool); !pending {
		return fmt.Errorf("task graph extension: parent runtime assets are already finalized")
	}
	for key, value := range patch {
		if value == nil {
			delete(payload, key)
		} else {
			payload[key] = value
		}
	}
	for _, task := range tasks {
		if task.JobID != jobID || task.ID == "" || task.ExecutorID == "" {
			return fmt.Errorf("task graph extension: task %q has invalid job or executor identity", task.ID)
		}
		if err := insertTaskAndSpec(ctx, conn, task); err != nil {
			return err
		}
	}
	dependsOnJSON, err := taskgraph.MarshalDependsOn(dependencies)
	if err != nil {
		return fmt.Errorf("task graph extension: encode dependencies: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if result, err := conn.ExecContext(ctx, `UPDATE tasks SET depends_on=?,revision=revision+1,updated_at=?
		WHERE task_id=? AND status IN ('PENDING','READY') AND (worker_id='' OR worker_id IS NULL)`,
		dependsOnJSON, now, parentTaskID); err != nil {
		return wrapDBInfrastructure("task graph extension parent dependencies", err)
	} else if changed, _ := result.RowsAffected(); changed != 1 {
		return taskgraph.ErrTransitionConflict
	}
	spec := &taskcontract.TaskSpec{Version: specVersion, JobID: jobID, ExecutorID: parentExecutor, Payload: payload}
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("task graph extension parent spec: %w", err)
	}
	encodedPayload, err := marshalSpecPayload(spec)
	if err != nil {
		return fmt.Errorf("task graph extension encode parent spec: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `UPDATE task_specs SET payload_json=?,spec_hash=? WHERE task_id=?`,
		encodedPayload, spec.MustSpecHash(), parentTaskID); err != nil {
		return wrapDBInfrastructure("task graph extension parent spec write", err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return wrapDBInfrastructure("task graph extension commit", err)
	}
	committed = true
	return nil
}

func insertTaskAndSpec(ctx context.Context, tx *sql.Conn, task taskgraph.Task) error {
	dependsOnJSON, err := taskgraph.MarshalDependsOn(task.DependsOn)
	if err != nil {
		return fmt.Errorf("task graph extension %s dependencies: %w", task.ID, err)
	}
	status := task.Status
	if status == "" {
		status = taskgraph.StatusPending
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks (
		task_id,job_id,project_id,render_plan_id,executor_id,executor_version,
		status,priority,revision,attempt_count,worker_id,lease_id,created_at,updated_at,depends_on
	) VALUES (?,?,?,?,?,?,?, ?,0,0,'','',?,?,?)`, task.ID, task.JobID, task.ProjectID,
		task.RenderPlanID, task.ExecutorID, task.ExecutorVersion, string(status), task.Priority,
		now, now, dependsOnJSON); err != nil {
		return wrapDBInfrastructure("task graph extension insert task", err)
	}
	spec := &taskcontract.TaskSpec{Version: taskcontract.SpecVersion, JobID: task.JobID, ExecutorID: task.ExecutorID, Payload: task.SpecPayload}
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("task graph extension task %s spec: %w", task.ID, err)
	}
	payloadJSON, err := marshalSpecPayload(spec)
	if err != nil {
		return fmt.Errorf("task graph extension task %s encode spec: %w", task.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO task_specs(task_id,spec_version,spec_hash,executor_id,payload_json,created_at)
		VALUES(?,?,?,?,?,?)`, task.ID, spec.Version, spec.MustSpecHash(), spec.ExecutorID, payloadJSON, now); err != nil {
		return wrapDBInfrastructure("task graph extension insert spec", err)
	}
	return nil
}
