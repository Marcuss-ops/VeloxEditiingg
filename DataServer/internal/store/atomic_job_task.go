package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// AtomicJobTaskCreator provides a store-level transaction coordinator that
// creates a Job and exactly one initial Task atomically. This guarantees
// the invariant: every newly enqueued render Job owns exactly one initial Task.
type AtomicJobTaskCreator struct {
	store *SQLiteStore
}

// NewAtomicJobTaskCreator constructs the coordinator.
func NewAtomicJobTaskCreator(store *SQLiteStore) *AtomicJobTaskCreator {
	return &AtomicJobTaskCreator{store: store}
}

// CreateJobWithTask atomically inserts a new Job in PENDING state and
// exactly one associated Task in PENDING state. Both writes succeed or
// both fail — there is no partial state.
//
// The task inherits identity fields from the job (JobID, ProjectID).
// The task's executor fields are populated from the provided spec.
func (c *AtomicJobTaskCreator) CreateJobWithTask(
	ctx context.Context,
	job *jobs.Job,
	taskSpec *taskgraph.TaskSpec,
	priority int,
) error {
	if c.store == nil || c.store.db == nil {
		return fmt.Errorf("atomic creator: store not initialized")
	}
	if job == nil {
		return fmt.Errorf("atomic creator: nil job")
	}
	if job.ID == "" {
		job.ID = uuid.NewString()
	}

	tx, err := c.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("atomic creator begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Insert Job
	jobPayload := "{}"
	if job.Payload != "" {
		jobPayload = job.Payload
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO jobs (
			job_id, status, max_retries, retry_count,
			video_name, project_id,
			created_at, updated_at, migrated_at,
			request_json, result_json, revision,
			run_id, job_run_id
		) VALUES (?, 'PENDING', ?, 0, ?, ?, ?, ?, ?, ?, '{}', 0, ?, ?)`,
		job.ID, job.MaxRetries, job.VideoName, job.ProjectID,
		now, now, now,
		jobPayload,
		job.RunID, job.RunID,
	)
	if err != nil {
		return fmt.Errorf("atomic creator job insert: %w", err)
	}

	// 2. Insert Task (exactly one per job)
	taskID := uuid.NewString()
	_, err = tx.ExecContext(ctx,
		`INSERT INTO tasks (
			task_id, job_id, project_id, render_plan_id,
			executor_id, executor_version, status, priority,
			revision, attempt_count, worker_id, lease_id,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, 0, 0, '', '', ?, ?)`,
		taskID, job.ID, job.ProjectID,
		taskSpec.RenderPlanID(),
		taskSpec.ExecutorID, taskSpec.Version,
		priority, now, now,
	)
	if err != nil {
		return fmt.Errorf("atomic creator task insert: %w", err)
	}

	// 3. Insert TaskSpec (validated immutable spec + hash)
	if taskSpec != nil {
		specHash := taskSpec.MustSpecHash()
		payloadJSON := "{}"
		if data, err := marshalSpecPayload(taskSpec); err == nil {
			payloadJSON = data
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			taskID, taskSpec.Version, specHash, taskSpec.ExecutorID, payloadJSON, now,
		)
		if err != nil {
			return fmt.Errorf("atomic creator task spec insert: %w", err)
		}
	}

	return tx.Commit()
}

// marshalSpecPayload serializes the spec payload to JSON.
func marshalSpecPayload(spec *taskgraph.TaskSpec) (string, error) {
	if spec == nil || spec.Payload == nil {
		return "{}", nil
	}
	data, err := json.Marshal(spec.Payload)
	if err != nil {
		return "{}", err
	}
	return string(data), nil
}
