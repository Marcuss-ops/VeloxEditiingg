package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"

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
	// SQLite can briefly return BUSY/LOCKED when deterministic forwarding
	// retries enter the same atomic write window. The transaction is fully
	// rolled back by createJobWithTaskOnce before retrying, so this preserves
	// the all-or-nothing Job+Task contract while allowing the loser to
	// converge on the committed idempotent row at the enqueue layer.
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := c.createJobWithTaskOnce(ctx, job, taskSpec, priority)
		if err == nil || !isSQLiteWriteConflict(err) || attempt == maxAttempts-1 {
			return err
		}
		delay := time.Duration(attempt+1) * 5 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func (c *AtomicJobTaskCreator) createJobWithTaskOnce(
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
		return wrapDBInfrastructure("atomic creator begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := c.CreateJobWithTaskTx(ctx, tx, job, taskSpec, priority); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("atomic creator commit", err)
	}
	return nil
}

func isSQLiteWriteConflict(err error) bool {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
	}
	var sqliteErrPtr *sqlite3.Error
	if errors.As(err, &sqliteErrPtr) && sqliteErrPtr != nil {
		return sqliteErrPtr.Code == sqlite3.ErrBusy || sqliteErrPtr.Code == sqlite3.ErrLocked
	}
	return strings.Contains(err.Error(), "database is locked") || strings.Contains(err.Error(), "database table is locked")
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

	controlPlanePayload := taskSpec.DeliveryPlan
	if len(controlPlanePayload) == 0 {
		// Compatibility fallback for legacy callers that still place the
		// delivery envelope in TaskSpec.Payload. New canonical writers use
		// TaskSpec.DeliveryPlan and keep the renderer payload clean.
		controlPlanePayload = taskSpec.Payload
	}
	deliveryPlan, err := parseDeliveryPlanPayload(controlPlanePayload)
	if err != nil {
		return fmt.Errorf("atomic creator: invalid delivery plan: %w", err)
	}
	renderOnly := false
	if controlPlanePayload != nil {
		renderOnly, _ = controlPlanePayload["render_only"].(bool)
	}
	if c.requireExplicitDeliveryPlan && len(deliveryPlan) == 0 && !renderOnly {
		return fmt.Errorf(
			"atomic creator: explicit delivery plan required; provide delivery_plan, delivery_destination_ids, or delivery_destination_id",
		)
	}

	now := nowRFC3339()

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
			job_required_min_bandwidth_mbps,
			workspace_id
		) VALUES (?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, '{}', 0, ?, ?,
		          ?, ?, ?, ?,
		          ?, ?)`,
		job.ID, job.MaxRetries, job.VideoName, job.ProjectID,
		now, now, now,
		jobPayload,
		job.RunID, job.RunID,
		req.ResourceClass, req.TemporalMode,
		req.Deterministic, req.Cacheable,
		req.MinBandwidthMbps,
		job.WorkspaceID,
	)
	if err != nil {
		return wrapDBInfrastructure("atomic creator job insert", err)
	}

	// 2. Snapshot and validate the delivery plan while the job insert is still
	// uncommitted. Any missing/disabled destination rolls the entire enqueue
	// back instead of surfacing only after a successful render.
	if err := insertDeliveryPlanTx(ctx, tx, job.ID, deliveryPlan, now); err != nil {
		return fmt.Errorf("atomic creator delivery plan: %w", err)
	}
	if err := insertPublicationStatesTx(ctx, tx, job.ID, taskSpec.PublicationSpecs, now); err != nil {
		return fmt.Errorf("atomic creator publication state: %w", err)
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
		return wrapDBInfrastructure("atomic creator task insert", err)
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
		return wrapDBInfrastructure("atomic creator task spec insert", err)
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
			return wrapDBInfrastructure("atomic creator task requirements insert", err)
		}
	}

	return nil
}

// insertPublicationStatesTx makes publication intents durable at the same
// enqueue boundary as Job, TaskSpec and delivery plans. A later retry can
// therefore recover the phase checkpoint even if the process dies before the
// first delivery attempt starts.
func insertPublicationStatesTx(ctx context.Context, tx *sql.Tx, jobID string, specs []map[string]interface{}, now string) error {
	for index, spec := range specs {
		publicationID, ok := spec["publication_id"].(string)
		publicationID = strings.TrimSpace(publicationID)
		if !ok || publicationID == "" {
			return fmt.Errorf("publication_specs.%d.publication_id is required", index)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO publication_states
			(publication_id, job_id, state, revision, created_at, updated_at)
			VALUES (?, ?, 'PENDING', 0, ?, ?)
			ON CONFLICT(publication_id) DO NOTHING`, publicationID, jobID, now, now); err != nil {
			return wrapDBInfrastructure(fmt.Sprintf("publication %q", publicationID), err)
		}
	}
	return nil
}

func insertDeliveryPlanTx(ctx context.Context, tx *sql.Tx, jobID string, plan []deliveryPlanEntry, now string) error {
	for _, entry := range plan {
		// Per-destination existence + globally-enabled check is delegated
		// to validateDeliveryDestinationTx (see delivery_plan_validator.go)
		// so the check can be unit-tested without constructing a full
		// Job + TaskSpec, and so future canonical writers (completion
		// coordinator, etc.) can reuse the same guard without depending
		// on the atomic creator type.
		if err := validateDeliveryDestinationTx(ctx, tx, entry.DestinationID); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx,
			`INSERT INTO job_delivery_plans (
				job_id, destination_id, enabled, priority, retry_budget,
				metadata_json, created_at, updated_at
			) VALUES (?, ?, 1, ?, ?, ?, ?, ?)
			ON CONFLICT(job_id, destination_id) DO UPDATE SET
				enabled = excluded.enabled,
				priority = excluded.priority,
				retry_budget = excluded.retry_budget,
				metadata_json = excluded.metadata_json,
				updated_at = excluded.updated_at`,
			jobID, entry.DestinationID, entry.Priority, entry.RetryBudget,
			entry.MetadataJSON, now, now,
		)
		if err != nil {
			return wrapDBInfrastructure(fmt.Sprintf("insert destination_id %q", entry.DestinationID), err)
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
