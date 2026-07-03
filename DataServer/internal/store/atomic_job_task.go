package store

import (
	"context"
	"database/sql"
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
	store                       *SQLiteStore
	requireExplicitDeliveryPlan bool
}

// NewAtomicJobTaskCreator constructs the coordinator.
func NewAtomicJobTaskCreator(store *SQLiteStore) *AtomicJobTaskCreator {
	return &AtomicJobTaskCreator{store: store}
}

// WithDeliveryPlanPolicy configures whether every newly created render job must
// carry an explicit delivery plan in its TaskSpec payload. The setting is made
// once at bootstrap, before the creator is shared by enqueue paths.
func (c *AtomicJobTaskCreator) WithDeliveryPlanPolicy(requireExplicit bool) *AtomicJobTaskCreator {
	if c != nil {
		c.requireExplicitDeliveryPlan = requireExplicit
	}
	return c
}

// CreateJobWithTask atomically inserts a new Job in PENDING state and
// exactly one associated Task in PENDING state. Both writes succeed or
// both fail — there is no partial state.
//
// When explicit delivery plans are required, the payload must include one of:
//
//   - delivery_plan: [{"destination_id":"...","priority":0,"retry_budget":5}]
//   - delivery_destination_ids: ["destination-a", "destination-b"]
//   - delivery_destination_id: "destination-a"
//
// The plan rows are inserted inside the same transaction as Job+Task creation,
// so a render can never become visible without the delivery contract required
// to complete finalization.
func (c *AtomicJobTaskCreator) CreateJobWithTask(
	ctx context.Context,
	job *jobs.Job,
	taskSpec *taskgraph.TaskSpec,
	priority int,
) error {
	if c == nil || c.store == nil || c.store.db == nil {
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

	if err := c.CreateJobWithTaskTx(ctx, tx, job, taskSpec, priority); err != nil {
		return err
	}

	return tx.Commit()
}

// CreateJobWithTaskTx performs the Job+delivery-plan+Task+TaskSpec INSERTs
// inside the caller's transaction. This is the canonical single-writer path
// for Job+Task creation — AtomicForwardAndEnqueue and any future multi-table
// transaction MUST call this method instead of duplicating the SQL.
func (c *AtomicJobTaskCreator) CreateJobWithTaskTx(
	ctx context.Context,
	tx *sql.Tx,
	job *jobs.Job,
	taskSpec *taskgraph.TaskSpec,
	priority int,
) error {
	if c == nil || c.store == nil || c.store.db == nil {
		return fmt.Errorf("atomic creator: store not initialized")
	}
	if tx == nil {
		return fmt.Errorf("atomic creator: nil tx")
	}
	if job == nil {
		return fmt.Errorf("atomic creator: nil job")
	}
	if taskSpec == nil {
		return fmt.Errorf("atomic creator: nil task spec")
	}
	if job.ID == "" {
		job.ID = uuid.NewString()
	}

	deliveryPlan, err := parseDeliveryPlanPayload(taskSpec.Payload)
	if err != nil {
		return fmt.Errorf("atomic creator: invalid delivery plan: %w", err)
	}
	if c.requireExplicitDeliveryPlan && len(deliveryPlan) == 0 {
		return fmt.Errorf(
			"atomic creator: explicit delivery plan required; provide delivery_plan, delivery_destination_ids, or delivery_destination_id",
		)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Insert Job.
	jobPayload := "{}"
	if job.Payload != "" {
		jobPayload = job.Payload
	}

	// PR #8: dedup columns written from job.Requirements so the eligibility
	// layer + claim paths see them without a second UPDATE after creation.
	// PR #9: retry_count column dropped — attempt starts at 0.
	req := job.Requirements
	_, err = tx.ExecContext(ctx,
		`INSERT INTO jobs (
			job_id, status, max_retries,
			video_name, project_id,
			created_at, updated_at, migrated_at,
			request_json, result_json, revision,
			run_id, job_run_id,
			job_required_resource_class, job_required_temporal_mode,
			job_required_deterministic, job_required_cacheable,
			job_required_min_bandwidth_mbps
		) VALUES (?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, '{}', 0, ?, ?,
		          ?, ?, ?, ?,
		          ?)`,
		job.ID, job.MaxRetries, job.VideoName, job.ProjectID,
		now, now, now,
		jobPayload,
		job.RunID, job.RunID,
		req.ResourceClass, req.TemporalMode,
		req.Deterministic, req.Cacheable,
		req.MinBandwidthMbps,
	)
	if err != nil {
		return fmt.Errorf("atomic creator job insert: %w", err)
	}

	// 2. Snapshot and validate the delivery plan while the job insert is still
	// uncommitted. Any missing/disabled destination rolls the entire enqueue
	// back instead of surfacing only after a successful render.
	if err := insertDeliveryPlanTx(ctx, tx, job.ID, deliveryPlan, now); err != nil {
		return fmt.Errorf("atomic creator delivery plan: %w", err)
	}

	// 3. Insert Task (exactly one per job).
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

	// 4. Insert TaskSpec (validated immutable spec + hash).
	specHash := taskSpec.MustSpecHash()
	payloadJSON := "{}"
	if data, marshalErr := marshalSpecPayload(taskSpec); marshalErr == nil {
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

	// 4b. Insert TaskRequirements for placement matcher capability gating.
	for _, capability := range taskSpec.RequiredCapabilities {
		if capability == "" {
			continue
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO task_requirements (task_id, capability) VALUES (?, ?)`,
			taskID, capability,
		)
		if err != nil {
			return fmt.Errorf("atomic creator task requirements insert: %w", err)
		}
	}

	return nil
}

func insertDeliveryPlanTx(ctx context.Context, tx *sql.Tx, jobID string, plan []deliveryPlanEntry, now string) error {
	for _, entry := range plan {
		var globallyEnabled int
		err := tx.QueryRowContext(ctx,
			`SELECT enabled FROM delivery_destinations WHERE destination_id = ?`,
			entry.DestinationID,
		).Scan(&globallyEnabled)
		if err == sql.ErrNoRows {
			return fmt.Errorf("destination_id %q does not exist", entry.DestinationID)
		}
		if err != nil {
			return fmt.Errorf("validate destination_id %q: %w", entry.DestinationID, err)
		}
		if globallyEnabled != 1 {
			return fmt.Errorf("destination_id %q is globally disabled", entry.DestinationID)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO job_delivery_plans (
				job_id, destination_id, enabled, priority, retry_budget,
				metadata_json, created_at, updated_at
			) VALUES (?, ?, 1, ?, ?, ?, ?, ?)`,
			jobID, entry.DestinationID, entry.Priority, entry.RetryBudget,
			entry.MetadataJSON, now, now,
		)
		if err != nil {
			return fmt.Errorf("insert destination_id %q: %w", entry.DestinationID, err)
		}
	}
	return nil
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
