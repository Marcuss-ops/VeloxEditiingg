// Package store / store_pipeline_runs.go
//
// Typed repository for the pipeline_runs table (migrations 011/088).
//
// A pipeline_run is the durable aggregate that tracks the lifecycle of a
// client-initiated generation pipeline. It is created BEFORE any remote
// call is made and exposes a single, versioned status to API clients.
//
// Idempotency: the UNIQUE index on idempotency_key means two requests
// with the same key converge on the same pipeline_run row. InsertPipelineRun
// uses INSERT OR IGNORE + lookup-by-key to return the persisted state in
// both the freshly-created and the already-existed cases.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/pipelineruns"
)

// ErrPipelineRunNoRow is returned when a lookup misses.
var ErrPipelineRunNoRow = errors.New("store: pipeline run row not found")

// InsertPipelineRunResult is returned by InsertPipelineRun to distinguish
// between a new insert (Created=true) and an idempotent duplicate
// (Created=false, Run returns the existing row looked up by idempotency_key).
type InsertPipelineRunResult struct {
	Created bool
	Run     *pipelineruns.PipelineRun
}

// InsertPipelineRun persists a new pipeline_run. Idempotent on
// idempotency_key via INSERT OR IGNORE enforced by the UNIQUE index.
//
// Returns an InsertPipelineRunResult:
//   - Created=true, Run=pr when the row was newly inserted.
//   - Created=false, Run=<existing row> when the idempotency_key already
//     existed. The existing row is looked up by idempotency_key and
//     returned so callers always receive the persisted state.
func (s *SQLiteStore) InsertPipelineRun(ctx context.Context, pr *pipelineruns.PipelineRun) (*InsertPipelineRunResult, error) {
	if pr == nil {
		return nil, fmt.Errorf("store: InsertPipelineRun: nil pipeline run")
	}
	if pr.ID == "" {
		return nil, fmt.Errorf("store: InsertPipelineRun: id is required")
	}
	if pr.IdempotencyKey == "" {
		return nil, fmt.Errorf("store: InsertPipelineRun: idempotency_key is required")
	}
	if pr.CreatedAt.IsZero() {
		pr.CreatedAt = time.Now().UTC()
	}
	if pr.UpdatedAt.IsZero() {
		pr.UpdatedAt = pr.CreatedAt
	}
	if pr.Status == "" {
		pr.Status = pipelineruns.StatusAccepted
	}
	createdAtStr := pr.CreatedAt.UTC().Format(time.RFC3339)
	updatedAtStr := pr.UpdatedAt.UTC().Format(time.RFC3339)
	completedAtStr := ""
	if !pr.CompletedAt.IsZero() {
		completedAtStr = pr.CompletedAt.UTC().Format(time.RFC3339)
	}

	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO pipeline_runs
		 (id, request_id, idempotency_key, user_id, campaign_id, campaign_item_id,
		  status, current_stage, remote_provider, remote_job_id, forwarding_id,
		  velox_job_id, artifact_id, delivery_id,
		  requested_payload_json, normalized_payload_json, result_json,
		  error_code, error_message, failed_stage,
		  created_at, updated_at, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pr.ID, pr.RequestID, pr.IdempotencyKey,
		pr.UserID, pr.CampaignID, pr.CampaignItemID,
		string(pr.Status), pr.CurrentStage,
		pr.RemoteProvider, pr.RemoteJobID, pr.ForwardingID,
		pr.VeloxJobID, pr.ArtifactID, pr.DeliveryID,
		pr.RequestedPayloadJSON, pr.NormalizedPayloadJSON, pr.ResultJSON,
		pr.ErrorCode, pr.ErrorMessage, pr.FailedStage,
		createdAtStr, updatedAtStr, completedAtStr,
	)
	if err != nil {
		return nil, fmt.Errorf("store: InsertPipelineRun: %w", err)
	}

	affected, _ := res.RowsAffected()
	if affected == 1 {
		return &InsertPipelineRunResult{Created: true, Run: pr}, nil
	}

	// Duplicate — look up the existing row by idempotency_key.
	existing, err := s.GetPipelineRunByIdempotencyKey(ctx, pr.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("store: InsertPipelineRun: duplicate lookup: %w", err)
	}
	return &InsertPipelineRunResult{Created: false, Run: existing}, nil
}

// pipelineRunColumns is the canonical SELECT column list, in the same
// order as scanPipelineRun expects.
const pipelineRunColumns = `id, request_id, idempotency_key, user_id, campaign_id, campaign_item_id,
        status, current_stage, remote_provider, remote_job_id, forwarding_id,
        velox_job_id, artifact_id, delivery_id,
        requested_payload_json, normalized_payload_json, result_json,
        error_code, error_message, failed_stage,
        created_at, updated_at, completed_at`

type pipelineRunRowScanner interface {
	Scan(dest ...any) error
}

// scanPipelineRun maps a SQL row into a *pipelineruns.PipelineRun.
// created_at / updated_at / completed_at are stored as RFC3339 TEXT;
// empty strings map to the zero time.Time.
func scanPipelineRun(row pipelineRunRowScanner) (*pipelineruns.PipelineRun, error) {
	var (
		pr                   pipelineruns.PipelineRun
		status               string
		createdAt, updatedAt sql.NullString
		completedAt          sql.NullString
	)
	err := row.Scan(
		&pr.ID, &pr.RequestID, &pr.IdempotencyKey,
		&pr.UserID, &pr.CampaignID, &pr.CampaignItemID,
		&status, &pr.CurrentStage,
		&pr.RemoteProvider, &pr.RemoteJobID, &pr.ForwardingID,
		&pr.VeloxJobID, &pr.ArtifactID, &pr.DeliveryID,
		&pr.RequestedPayloadJSON, &pr.NormalizedPayloadJSON, &pr.ResultJSON,
		&pr.ErrorCode, &pr.ErrorMessage, &pr.FailedStage,
		&createdAt, &updatedAt, &completedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrPipelineRunNoRow
	}
	if err != nil {
		return nil, fmt.Errorf("store: scan pipeline run: %w", err)
	}
	pr.Status = pipelineruns.Status(status)
	pr.CreatedAt = parseRFC3339(createdAt.String)
	pr.UpdatedAt = parseRFC3339(updatedAt.String)
	pr.CompletedAt = parseRFC3339(completedAt.String)
	return &pr, nil
}

// parseRFC3339 parses a TEXT-stored RFC3339 timestamp. Empty string
// returns the zero time.Time.
func parseRFC3339(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// GetPipelineRun returns a single pipeline_run by its primary key, or
// ErrPipelineRunNoRow when missing.
func (s *SQLiteStore) GetPipelineRun(ctx context.Context, id string) (*pipelineruns.PipelineRun, error) {
	if id == "" {
		return nil, fmt.Errorf("store: GetPipelineRun: empty id")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+pipelineRunColumns+`
		 FROM pipeline_runs WHERE id = ?`, id)
	return scanPipelineRun(row)
}

// GetPipelineRunByIdempotencyKey returns a single pipeline_run by its
// idempotency_key, or ErrPipelineRunNoRow when missing.
func (s *SQLiteStore) GetPipelineRunByIdempotencyKey(ctx context.Context, key string) (*pipelineruns.PipelineRun, error) {
	if key == "" {
		return nil, fmt.Errorf("store: GetPipelineRunByIdempotencyKey: empty idempotency_key")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+pipelineRunColumns+`
		 FROM pipeline_runs WHERE idempotency_key = ?`, key)
	return scanPipelineRun(row)
}

// GetPipelineRunByRequestID returns a single pipeline_run by request_id,
// or ErrPipelineRunNoRow when missing. request_id is non-unique in the
// schema (an index, not a unique constraint) so this returns the most
// recently created row matching the request_id.
func (s *SQLiteStore) GetPipelineRunByRequestID(ctx context.Context, requestID string) (*pipelineruns.PipelineRun, error) {
	if requestID == "" {
		return nil, fmt.Errorf("store: GetPipelineRunByRequestID: empty request_id")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+pipelineRunColumns+`
		 FROM pipeline_runs WHERE request_id = ?
		 ORDER BY created_at DESC LIMIT 1`, requestID)
	return scanPipelineRun(row)
}

// UpdatePipelineRunRemoteJob stamps the remote_job_id and remote_provider
// onto an existing pipeline_run. Used after the remote-engine call
// returns a job id. Updates updated_at.
func (s *SQLiteStore) UpdatePipelineRunRemoteJob(ctx context.Context, id, remoteProvider, remoteJobID string) error {
	if id == "" {
		return fmt.Errorf("store: UpdatePipelineRunRemoteJob: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET remote_provider = ?, remote_job_id = ?, updated_at = ?
		 WHERE id = ?`,
		remoteProvider, remoteJobID, now, id,
	)
	if err != nil {
		return fmt.Errorf("store: UpdatePipelineRunRemoteJob: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}

// UpdatePipelineRunForwarding stamps the forwarding_id onto an existing
// pipeline_run and optionally advances the status. Used after the
// creator_forwardings PENDING row is persisted. Updates updated_at.
func (s *SQLiteStore) UpdatePipelineRunForwarding(ctx context.Context, id, forwardingID string, status pipelineruns.Status) error {
	if id == "" {
		return fmt.Errorf("store: UpdatePipelineRunForwarding: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET forwarding_id = ?, status = ?, updated_at = ?
		 WHERE id = ?`,
		forwardingID, string(status), now, id,
	)
	if err != nil {
		return fmt.Errorf("store: UpdatePipelineRunForwarding: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}

// UpdatePipelineRunStatus sets the aggregated status and current_stage
// on an existing pipeline_run. When stage is empty the column is left
// untouched. Updates updated_at. When status is terminal
// (COMPLETED/FAILED/CANCELLED) and completed_at is empty, it is stamped.
func (s *SQLiteStore) UpdatePipelineRunStatus(ctx context.Context, id string, status pipelineruns.Status, stage string) error {
	if id == "" {
		return fmt.Errorf("store: UpdatePipelineRunStatus: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	var stmt string
	var args []interface{}
	if stage != "" {
		stmt = `UPDATE pipeline_runs
		        SET status = ?, current_stage = ?, updated_at = ?`
		args = []interface{}{string(status), stage, now}
	} else {
		stmt = `UPDATE pipeline_runs
		        SET status = ?, updated_at = ?`
		args = []interface{}{string(status), now}
	}
	if status.Terminal() {
		stmt += `, completed_at = COALESCE(NULLIF(completed_at, ''), ?)`
		args = append(args, now)
	}
	stmt += ` WHERE id = ?`
	args = append(args, id)

	result, err := s.db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return fmt.Errorf("store: UpdatePipelineRunStatus: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}

// UpdatePipelineRunVeloxJob stamps the velox_job_id onto an existing
// pipeline_run and optionally advances the status. Used after a sync
// forward succeeds and the Velox job is created. Updates updated_at.
func (s *SQLiteStore) UpdatePipelineRunVeloxJob(ctx context.Context, id, veloxJobID string, status pipelineruns.Status) error {
	if id == "" {
		return fmt.Errorf("store: UpdatePipelineRunVeloxJob: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET velox_job_id = ?, status = ?, updated_at = ?
		 WHERE id = ?`,
		veloxJobID, string(status), now, id,
	)
	if err != nil {
		return fmt.Errorf("store: UpdatePipelineRunVeloxJob: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}

// UpdatePipelineRunResult stamps the result_json onto an existing
// pipeline_run. Used after the remote engine returns a response (sync
// forward or async persist) so the full remote payload is durably
// stored for audit and debugging. Updates updated_at.
func (s *SQLiteStore) UpdatePipelineRunResult(ctx context.Context, id, resultJSON string) error {
	if id == "" {
		return fmt.Errorf("store: UpdatePipelineRunResult: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET result_json = ?, updated_at = ?
		 WHERE id = ?`,
		resultJSON, now, id,
	)
	if err != nil {
		return fmt.Errorf("store: UpdatePipelineRunResult: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}

// ClearPipelineRunError resets error_code, error_message and failed_stage
// to empty strings on an existing pipeline_run. Used by the retry handler
// to clear the error state before re-submitting. Updates updated_at.
func (s *SQLiteStore) ClearPipelineRunError(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("store: ClearPipelineRunError: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET error_code = '', error_message = '', failed_stage = '',
		     completed_at = '', updated_at = ?
		 WHERE id = ?`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("store: ClearPipelineRunError: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}

// UpdatePipelineRunError stamps error_code, error_message and failed_stage
// onto an existing pipeline_run and transitions to FAILED. Updates
// updated_at and completed_at.
func (s *SQLiteStore) UpdatePipelineRunError(ctx context.Context, id, code, message, failedStage string) error {
	if id == "" {
		return fmt.Errorf("store: UpdatePipelineRunError: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE pipeline_runs
		 SET status = ?, error_code = ?, error_message = ?, failed_stage = ?,
		     updated_at = ?, completed_at = COALESCE(NULLIF(completed_at, ''), ?)
		 WHERE id = ?`,
		string(pipelineruns.StatusFailed), code, message, failedStage, now, now, id,
	)
	if err != nil {
		return fmt.Errorf("store: UpdatePipelineRunError: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}
