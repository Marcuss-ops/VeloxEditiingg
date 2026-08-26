package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"velox-server/internal/deliverystore"
	"velox-server/internal/pipelineruns"
	"velox-server/internal/storecore"
)

// The methods in this file are the M2M repository boundary. Admin callers
// continue to use the legacy unscoped methods; M2M callers must provide both
// the canonical target_job_id and external_client_id.
//
// Why this file stays cross-domain (not a leaf):
//
// The single shared abstraction here is the creator_forwardings.external_client_id
// ownership predicate (JOIN/EXISTS) applied as the M2M authorization gate on
// read/write variants of EIGHT separate domain repositories — pipeline_runs,
// jobs, artifacts, job_deliveries, job_events, job_attempts, task_attempts,
// and worker_asset_downloads. Those methods return each domain's own row
// types (pipelineruns.PipelineRun, Artifact, JobDelivery, JobEvent,
// JobAttempt, TaskAttemptSnapshot, AssetDownloadProgressView) and reuse the
// domain scan helpers (scanPipelineRun, scanJobRow, jobColumns, scanArtifacts,
// scanAssetDownloadProgress). A leaf `ownership` package would have to import
// internal/store for those types/helpers — which the leaf boundary test
// forbids — or force a nine-domain type migration. The forwarding-only piece
// (MarkCreatorForwardingCancelledForClient) was extracted to forwardingstore;
// the remaining methods are ownership-scoped variants of the domains they
// belong to, so they stay here until each domain is itself extracted to a
// leaf.

func requireM2MClient(clientID string) error {
	if strings.TrimSpace(clientID) == "" {
		return storecore.ErrCreatorForwardingNoRow
	}
	return nil
}

func (s *SQLiteStore) GetPipelineRunForClient(ctx context.Context, id, clientID string) (*pipelineruns.PipelineRun, error) {
	if id == "" || requireM2MClient(clientID) != nil {
		return nil, ErrPipelineRunNoRow
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT pr.id, pr.request_id, pr.idempotency_key, pr.user_id, pr.campaign_id, pr.campaign_item_id,
		        pr.status, pr.current_stage, pr.remote_provider, pr.remote_job_id, pr.forwarding_id,
		        pr.velox_job_id, pr.artifact_id, pr.delivery_id,
		        pr.requested_payload_json, pr.normalized_payload_json, pr.result_json,
		        pr.error_code, pr.error_message, pr.failed_stage,
		        pr.created_at, pr.updated_at, pr.completed_at
		 FROM pipeline_runs pr
		 JOIN creator_forwardings cf
		   ON cf.forwarding_id = pr.forwarding_id
		  AND cf.external_client_id = ?
		 WHERE pr.id = ?`, strings.TrimSpace(clientID), id)
	return scanPipelineRun(row)
}

func (s *SQLiteStore) GetPipelineRunByRequestIDForClient(ctx context.Context, requestID, clientID string) (*pipelineruns.PipelineRun, error) {
	if requestID == "" || requireM2MClient(clientID) != nil {
		return nil, ErrPipelineRunNoRow
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT pr.id, pr.request_id, pr.idempotency_key, pr.user_id, pr.campaign_id, pr.campaign_item_id,
		        pr.status, pr.current_stage, pr.remote_provider, pr.remote_job_id, pr.forwarding_id,
		        pr.velox_job_id, pr.artifact_id, pr.delivery_id,
		        pr.requested_payload_json, pr.normalized_payload_json, pr.result_json,
		        pr.error_code, pr.error_message, pr.failed_stage,
		        pr.created_at, pr.updated_at, pr.completed_at
		 FROM pipeline_runs pr
		 JOIN creator_forwardings cf
		   ON cf.forwarding_id = pr.forwarding_id
		  AND cf.external_client_id = ?
		 WHERE pr.request_id = ?
		 ORDER BY pr.created_at DESC LIMIT 1`, strings.TrimSpace(clientID), requestID)
	return scanPipelineRun(row)
}

func pipelineRunOwnershipClause() string {
	return `EXISTS (
		SELECT 1 FROM creator_forwardings cf
		 WHERE cf.forwarding_id = pipeline_runs.forwarding_id
		   AND cf.external_client_id = ?
	)`
}

func (s *SQLiteStore) updatePipelineRunForClient(ctx context.Context, id, clientID, statement string, args ...any) error {
	if id == "" || requireM2MClient(clientID) != nil {
		return ErrPipelineRunNoRow
	}
	args = append(args, id, strings.TrimSpace(clientID))
	result, err := s.db.ExecContext(ctx, statement+` AND `+pipelineRunOwnershipClause(), args...)
	if err != nil {
		return err
	}
	n, err := readRowsAffected(result, "update pipeline run for client")
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrPipelineRunNoRow
	}
	return nil
}

func (s *SQLiteStore) UpdatePipelineRunRemoteJobForClient(ctx context.Context, id, clientID, provider, remoteJobID string) error {
	now := nowRFC3339()
	return s.updatePipelineRunForClient(ctx, id, clientID,
		`UPDATE pipeline_runs SET remote_provider = ?, remote_job_id = ?, updated_at = ? WHERE pipeline_runs.id = ?`,
		provider, remoteJobID, now)
}

func (s *SQLiteStore) UpdatePipelineRunForwardingForClient(ctx context.Context, id, clientID, forwardingID string, status pipelineruns.Status) error {
	now := nowRFC3339()
	return s.updatePipelineRunForClient(ctx, id, clientID,
		`UPDATE pipeline_runs SET forwarding_id = ?, status = ?, updated_at = ? WHERE pipeline_runs.id = ?`,
		forwardingID, string(status), now)
}

func (s *SQLiteStore) UpdatePipelineRunResultForClient(ctx context.Context, id, clientID, resultJSON string) error {
	now := nowRFC3339()
	return s.updatePipelineRunForClient(ctx, id, clientID,
		`UPDATE pipeline_runs SET result_json = ?, updated_at = ? WHERE pipeline_runs.id = ?`,
		resultJSON, now)
}

func (s *SQLiteStore) ClearPipelineRunErrorForClient(ctx context.Context, id, clientID string) error {
	now := nowRFC3339()
	return s.updatePipelineRunForClient(ctx, id, clientID,
		`UPDATE pipeline_runs
		 SET error_code = '', error_message = '', failed_stage = '', completed_at = '', updated_at = ?
		 WHERE pipeline_runs.id = ?`, now)
}

func (s *SQLiteStore) UpdatePipelineRunStatusForClient(ctx context.Context, id, clientID string, status pipelineruns.Status, stage string) error {
	if id == "" || requireM2MClient(clientID) != nil {
		return ErrPipelineRunNoRow
	}
	now := nowRFC3339()
	set := `status = ?, updated_at = ?`
	args := []any{string(status), now}
	if stage != "" {
		set = `status = ?, current_stage = ?, updated_at = ?`
		args = []any{string(status), stage, now}
	}
	if status.Terminal() {
		set += `, completed_at = COALESCE(NULLIF(completed_at, ''), ?)`
		args = append(args, now)
	}
	return s.updatePipelineRunForClient(ctx, id, clientID,
		`UPDATE pipeline_runs SET `+set+` WHERE pipeline_runs.id = ?`, args...)
}

func (s *SQLiteStore) UpdatePipelineRunErrorForClient(ctx context.Context, id, clientID, code, message, failedStage string) error {
	now := nowRFC3339()
	return s.updatePipelineRunForClient(ctx, id, clientID,
		`UPDATE pipeline_runs
		 SET status = ?, error_code = ?, error_message = ?, failed_stage = ?,
		     updated_at = ?, completed_at = COALESCE(NULLIF(completed_at, ''), ?)
		 WHERE pipeline_runs.id = ?`,
		string(pipelineruns.StatusFailed), code, message, failedStage, now, now)
}

func (s *SQLiteStore) GetJobForClient(ctx context.Context, jobID, clientID string) (map[string]any, error) {
	if jobID == "" || requireM2MClient(clientID) != nil {
		return nil, sql.ErrNoRows
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM jobs j
		 WHERE j.job_id = ?
		   AND EXISTS (SELECT 1 FROM creator_forwardings cf
		               WHERE cf.target_job_id = j.job_id
		                 AND cf.external_client_id = ?)`, jobID, strings.TrimSpace(clientID))
	return scanJobRow(row)
}

func (s *SQLiteStore) GetArtifactsByJobForClient(ctx context.Context, jobID, clientID string, limit int) ([]Artifact, error) {
	if jobID == "" || requireM2MClient(clientID) != nil {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT a.id, a.job_id, COALESCE(a.attempt_id,0), a.type, a.storage_provider,
		        COALESCE(a.storage_key,''), COALESCE(a.storage_url,''), COALESCE(a.local_path,''),
		        COALESCE(a.sha256,''), COALESCE(a.size_bytes,0), COALESCE(a.duration_seconds,0.0),
		        COALESCE(a.duration_ms,0), COALESCE(a.mime_type,''), COALESCE(a.verified_at,''),
		        a.status, a.created_at
		 FROM artifacts a
		 WHERE a.job_id = ?
		   AND EXISTS (SELECT 1 FROM creator_forwardings cf
		               WHERE cf.target_job_id = a.job_id
		                 AND cf.external_client_id = ?)
		 ORDER BY a.created_at DESC LIMIT ?`, jobID, strings.TrimSpace(clientID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanArtifacts(rows)
}

func scanArtifacts(rows *sql.Rows) ([]Artifact, error) {
	var out []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
			&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256, &a.SizeBytes,
			&a.DurationSeconds, &a.DurationMs, &a.MimeType, &a.VerifiedAt,
			&a.Status, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) ListJobDeliveriesByJobForClient(ctx context.Context, jobID, clientID string) ([]deliverystore.JobDelivery, error) {
	if jobID == "" || requireM2MClient(clientID) != nil {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT jd.delivery_id, jd.artifact_id, jd.destination_id, jd.status,
		        COALESCE(jd.idempotency_key,''), COALESCE(jd.remote_id,''), COALESCE(jd.remote_url,''),
		        jd.created_at, jd.updated_at, COALESCE(jd.next_attempt_at,''), jd.attempt_count,
		        COALESCE(jd.last_error_code,''), COALESCE(jd.last_error_message,''),
		        jd.max_attempts, COALESCE(jd.completed_at,'')
		 FROM job_deliveries jd JOIN artifacts a ON a.id = jd.artifact_id
		 WHERE a.job_id = ?
		   AND EXISTS (SELECT 1 FROM creator_forwardings cf
		               WHERE cf.target_job_id = a.job_id
		                 AND cf.external_client_id = ?)
		 ORDER BY jd.delivery_id ASC`, jobID, strings.TrimSpace(clientID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []deliverystore.JobDelivery
	for rows.Next() {
		var d deliverystore.JobDelivery
		if err := rows.Scan(&d.DeliveryID, &d.ArtifactID, &d.DestinationID, &d.Status,
			&d.IdempotencyKey, &d.RemoteID, &d.RemoteURL, &d.CreatedAt, &d.UpdatedAt,
			&d.NextAttemptAt, &d.AttemptCount, &d.LastError, &d.LastErrorMessage,
			&d.MaxAttempts, &d.CompletedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) ListJobEventsForClient(ctx context.Context, jobID, clientID string, limit int) ([]JobEvent, error) {
	if jobID == "" || requireM2MClient(clientID) != nil {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT e.timestamp, e.job_id, e.event, e.raw_json
		 FROM job_events e
		 WHERE e.job_id = ?
		   AND EXISTS (SELECT 1 FROM creator_forwardings cf
		               WHERE cf.target_job_id = e.job_id
		                 AND cf.external_client_id = ?)
		 ORDER BY e.timestamp DESC LIMIT ?`, jobID, strings.TrimSpace(clientID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobEvent
	for rows.Next() {
		var e JobEvent
		if err := rows.Scan(&e.Timestamp, &e.JobID, &e.Event, &e.RawJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) GetJobAttemptsForClient(ctx context.Context, jobID, clientID string, limit int) ([]JobAttempt, error) {
	if jobID == "" || requireM2MClient(clientID) != nil {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT ja.id, ja.job_id, ja.attempt_number, ja.worker_id, ja.lease_id, ja.status,
		        COALESCE(ja.started_at,''), COALESCE(ja.finished_at,''),
		        COALESCE(ja.error_code,''), COALESCE(ja.engine_version,''), COALESCE(ja.bundle_hash,''),
		        ja.created_at
		 FROM job_attempts ja
		 WHERE ja.job_id = ?
		   AND EXISTS (SELECT 1 FROM creator_forwardings cf
		               WHERE cf.target_job_id = ja.job_id
		                 AND cf.external_client_id = ?)
		 ORDER BY ja.attempt_number DESC LIMIT ?`, jobID, strings.TrimSpace(clientID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobAttempt
	for rows.Next() {
		var a JobAttempt
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptNumber, &a.WorkerID, &a.LeaseID,
			&a.Status, &a.StartedAt, &a.FinishedAt, &a.ErrorCode, &a.EngineVersion,
			&a.BundleHash, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) GetLatestTaskAttemptForJobForClient(ctx context.Context, jobID, clientID string) (*TaskAttemptSnapshot, error) {
	if jobID == "" || requireM2MClient(clientID) != nil {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT ta.task_id, ta.id, ta.job_id, ta.worker_id, ta.lease_id, ta.attempt_number
		 FROM task_attempts ta
		 WHERE ta.job_id = ?
		   AND EXISTS (SELECT 1 FROM creator_forwardings cf
		               WHERE cf.target_job_id = ta.job_id
		                 AND cf.external_client_id = ?)
		 ORDER BY ta.created_at DESC LIMIT 1`, jobID, strings.TrimSpace(clientID))
	var snap TaskAttemptSnapshot
	if err := row.Scan(&snap.TaskID, &snap.AttemptID, &snap.JobID, &snap.WorkerID, &snap.LeaseID, &snap.AttemptNumber); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &snap, nil
}

func (s *SQLiteStore) ListAssetDownloadProgressForJobForClient(ctx context.Context, jobID, clientID string) ([]AssetDownloadProgressView, error) {
	if jobID == "" || requireM2MClient(clientID) != nil {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.job_id, d.worker_id, d.asset_key, d.asset_id, d.role, d.state,
		       d.bytes_downloaded, d.bytes_total, d.bytes_per_second, d.eta_seconds,
		       d.attempt, d.shared_waiters, d.cache_hit, d.updated_at,
		       d.error_code, d.error_detail, r.task_id, r.scene_ids_json
		FROM job_asset_refs r
	JOIN worker_asset_downloads d ON d.worker_id=r.worker_id AND d.asset_key=r.asset_key
	WHERE r.job_id=?
	  AND EXISTS (SELECT 1 FROM creator_forwardings cf
	              WHERE cf.target_job_id = r.job_id
	                AND cf.external_client_id = ?)
	ORDER BY d.updated_at DESC, d.asset_key ASC`, jobID, strings.TrimSpace(clientID))
	if err != nil {
		return nil, fmt.Errorf("asset download progress: list scoped job %s: %w", jobID, err)
	}
	defer rows.Close()
	return scanAssetDownloadProgress(rows, jobID)
}

func scanAssetDownloadProgress(rows *sql.Rows, jobID string) ([]AssetDownloadProgressView, error) {
	var out []AssetDownloadProgressView
	for rows.Next() {
		var v AssetDownloadProgressView
		var cacheHit int
		var sceneJSON string
		if err := rows.Scan(&v.JobID, &v.WorkerID, &v.AssetKey, &v.AssetID, &v.Role, &v.State,
			&v.BytesDownloaded, &v.BytesTotal, &v.BytesPerSecond, &v.ETASeconds,
			&v.Attempt, &v.SharedWaiters, &cacheHit, &v.UpdatedAt,
			&v.ErrorCode, &v.ErrorDetail, &v.TaskID, &sceneJSON); err != nil {
			return nil, fmt.Errorf("asset download progress: scan job %s: %w", jobID, err)
		}
		v.CacheHit = cacheHit != 0
		if sceneJSON != "" {
			_ = json.Unmarshal([]byte(sceneJSON), &v.SceneIDs)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
