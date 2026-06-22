package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/costmodel"
)

// ClaimNextPendingJob atomically claims the next pending/queued job for a worker.
// Reads columns directly (not raw_json), then writes the claim via result_json.
// Returns the updated result_json blob, per-job Requirements from dedicated
// columns, and true if a job was claimed.
// PR #6: Requirements return is from columns only; no _requirements in result_json.
func (s *SQLiteStore) ClaimNextPendingJob(workerID string, allowedJobTypes []string, now time.Time) ([]byte, costmodel.JobRequirements, bool, error) {
	var zeroReq costmodel.JobRequirements
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, zeroReq, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// PR #9: assigned_to, claimed_by, retry_count columns dropped.
	// Use attempt column as retry count proxy; filter only by PENDING status.
	rows, err := tx.Query(
		`SELECT job_id, status, job_fingerprint, run_id, job_run_id,
		        video_name, project_id, COALESCE(attempt, 0) as retry_count, request_json, result_json,
		        COALESCE(job_required_resource_class, ''),
		        COALESCE(job_required_temporal_mode, ''),
		        COALESCE(job_required_deterministic, 0),
		        COALESCE(job_required_cacheable, 0),
		        COALESCE(job_required_min_bandwidth_mbps, 0.0)
		 FROM jobs
		 WHERE UPPER(status) = 'PENDING'
		 ORDER BY COALESCE(updated_at, created_at) ASC, job_id ASC`,
	)
	if err != nil {
		return nil, zeroReq, false, err
	}

	nowISO := now.UTC().Format(time.RFC3339)
	nowUnix := now.UTC().Unix()

	for rows.Next() {
		var (
			jobID, status, jobFingerprint, runID, jobRunID sql.NullString
			videoName, projectID                              sql.NullString
			retryCount                                        sql.NullInt64
			requestJSON, resultJSON                           sql.NullString
			requiredResourceClass, requiredTemporalMode        sql.NullString
			requiredDeterministic, requiredCacheable          sql.NullInt64
			requiredMinBandwidthMbps                          sql.NullFloat64
		)
		if err := rows.Scan(&jobID, &status, &jobFingerprint, &runID, &jobRunID,
			&videoName, &projectID, &retryCount, &requestJSON, &resultJSON,
			&requiredResourceClass, &requiredTemporalMode,
			&requiredDeterministic, &requiredCacheable, &requiredMinBandwidthMbps); err != nil {
			rows.Close()
			return nil, zeroReq, false, err
		}

		// Check job type filter if specified (parse request_json properly to avoid substring false positives)
		if len(allowedJobTypes) > 0 && requestJSON.Valid && requestJSON.String != "" {
			var req map[string]any
			if err := json.Unmarshal([]byte(requestJSON.String), &req); err == nil {
				if !jobTypeAllowed(req, allowedJobTypes) {
					continue
				}
			}
		}

		newRetry := int(retryCount.Int64) + 1
		leaseID := uuid.NewString()
		leaseExpiry := now.UTC().Add(30 * time.Minute).Format(time.RFC3339)

		// Build the updated result_json blob with claim data.
		// PR #7/#9: runtime fields removed — tasks carry them.
		// lease_id, attempt, lease_expiry kept for ClaimNextResult parsing.
		resultMap := make(map[string]any)
		if resultJSON.Valid && resultJSON.String != "" {
			_ = json.Unmarshal([]byte(resultJSON.String), &resultMap)
		}
		resultMap["job_id"] = jobID.String
		resultMap["status"] = "LEASED"
		resultMap["lease_id"] = leaseID
		resultMap["lease_expiry"] = leaseExpiry
		resultMap["attempt"] = newRetry
		resultMap["contract_version"] = 3
		resultMap["updated_at"] = nowUnix

		updatedResult, err := json.Marshal(resultMap)
		if err != nil {
			rows.Close()
			return nil, zeroReq, false, err
		}

		result, err := tx.Exec(
			`UPDATE jobs
			 SET status = ?, worker_name = ?, attempt = ?,
			     result_json = ?, updated_at = ?, migrated_at = ?,
			     assigned_at = ?,
			     claimed_at = ?
			 WHERE job_id = ?
			   AND UPPER(status) = 'PENDING'`,
			"LEASED", workerID, newRetry,
			string(updatedResult), nowISO, nowISO,
			nowISO,
			nowISO,
			jobID.String,
		)
		if err != nil {
			rows.Close()
			return nil, zeroReq, false, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			rows.Close()
			return nil, zeroReq, false, err
		}
		if affected == 0 {
			continue
		}

		// Record history (not in blob — separate table)
		history := []map[string]any{{
			"status":    "LEASED",
			"timestamp": nowISO,
			"worker_id": workerID,
			"message":   fmt.Sprintf("Job assigned to worker %s", workerID),
		}}
		if err := s.replaceJobHistoryTx(tx, jobID.String, history); err != nil {
			rows.Close()
			return nil, zeroReq, false, err
		}

		// Record job attempt
		// cleanup/remove-job-attempts-runtime: no longer write to job_attempts.
		// Per-attempt identity now lives on task_attempts (canonical); the
		// canonical claim path is the task-native dispatch (PR-04). The
		// legacy_job_dispatch path is being decommissioned alongside PR-07.

		if err := tx.Commit(); err != nil {
			rows.Close()
			return nil, zeroReq, false, err
		}
		rows.Close()

		claimedReq := costmodel.JobRequirements{
			ResourceClass:    costmodel.ResourceClass(strings.TrimSpace(requiredResourceClass.String)),
			TemporalMode:     costmodel.TemporalMode(strings.TrimSpace(requiredTemporalMode.String)),
			Deterministic:    requiredDeterministic.Int64 != 0,
			Cacheable:        requiredCacheable.Int64 != 0,
			MinBandwidthMbps: requiredMinBandwidthMbps.Float64,
		}

		_ = s.LogJobEvent(jobID.String, "job_claimed", map[string]interface{}{
			"worker_id": workerID, "lease_id": leaseID, "attempt": newRetry,
		})
		return bytes.Clone(updatedResult), claimedReq, true, nil
	}

	if err := rows.Err(); err != nil {
		return nil, zeroReq, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, zeroReq, false, err
	}
	return nil, zeroReq, false, nil
}

func jobTypeAllowed(payload map[string]any, allowedJobTypes []string) bool {
	if len(allowedJobTypes) == 0 {
		return true
	}

	jobType := strings.TrimSpace(asString(payload["job_type"]))
	if jobType == "" {
		if params, ok := payload["parameters"].(map[string]any); ok {
			jobType = strings.TrimSpace(asString(params["job_type"]))
		}
	}
	if jobType == "" {
		return true
	}

	for _, allowed := range allowedJobTypes {
		if strings.EqualFold(strings.TrimSpace(allowed), jobType) {
			return true
		}
	}
	return false
}

// =============================================================================
// PR-04.6: cost-rank claim path. ClaimNextPendingJobForWorker is the
// sibling of ClaimNextPendingJob that scores a bounded pool of pending
// candidates against the supplied costmodel.WorkerProfile, filters
// Eligible=true, and CAS-claims the lowest-scored (best-fit) pair.
// =============================================================================

// rankCandidate captures the per-row data needed to score and CAS a
// single pending job for the rank loop. Lives in store_jobs.go as a
// package-local type so it doesn't leak into the public store API.
// PR #6: all 5 Requirements columns read directly; no JSON fallback.
type rankCandidate struct {
	jobID                  string
	retryCount             int64
	requestJSON            string
	resultJSON             string
	reqRC                  string
	reqTM                  string
	reqDeterministic       bool
	reqCacheable           bool
	reqMinBandwidthMbps    float64
	updatedAt              string
}

func (s *SQLiteStore) ClaimNextPendingJobForWorker(
	ctx context.Context,
	workerID string,
	allowedJobTypes []string,
	profile costmodel.WorkerProfile,
	maxCandidates int,
	now time.Time,
) ([]byte, costmodel.JobRequirements, bool, error) {
	if s == nil || s.db == nil {
		return nil, costmodel.JobRequirements{}, false, fmt.Errorf("store not initialized")
	}
	if workerID == "" {
		return nil, costmodel.JobRequirements{}, false, fmt.Errorf("claim with empty workerID")
	}
	if maxCandidates <= 0 {
		maxCandidates = 20
	}
	if maxCandidates > 100 {
		maxCandidates = 100
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	var rankZeroReq costmodel.JobRequirements

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, rankZeroReq, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// 1. Over-fetch PENDING candidates (4x cap) to absorb
	//    job_type filter dropouts; keep FIFO pre-sort so the rank's
	//    secondary tiebreak (updated_at ASC) is consistent across
	//    score-tied candidates.
	//    PR #9: assigned_to column dropped — filter only by PENDING status.
	//    Use attempt column as retry count proxy.
	rows, err := tx.Query(
		`SELECT job_id, COALESCE(attempt, 0) as retry_count,
		        COALESCE(request_json, ''),
		        COALESCE(result_json, ''),
		        COALESCE(job_required_resource_class, ''),
		        COALESCE(job_required_temporal_mode, ''),
		        COALESCE(job_required_deterministic, 0),
		        COALESCE(job_required_cacheable, 0),
		        COALESCE(job_required_min_bandwidth_mbps, 0.0),
		        COALESCE(updated_at, created_at)
		 FROM jobs
		 WHERE UPPER(status) = 'PENDING'
		 ORDER BY COALESCE(updated_at, created_at) ASC, job_id ASC
		 LIMIT ?`,
		maxCandidates*4,
	)
	if err != nil {
		return nil, rankZeroReq, false, err
	}

	var candidates []rankCandidate
	for rows.Next() {
		var (
			jobID                               sql.NullString
			retryCount                          sql.NullInt64
			requestJSON, resultJSON             sql.NullString
			reqRC, reqTM                        sql.NullString
			reqDeterministic, reqCacheable      sql.NullInt64
			reqMinBandwidthMbps                 sql.NullFloat64
			updatedAt                           sql.NullString
		)
		if err := rows.Scan(&jobID, &retryCount, &requestJSON, &resultJSON,
			&reqRC, &reqTM,
			&reqDeterministic, &reqCacheable, &reqMinBandwidthMbps,
			&updatedAt); err != nil {
			rows.Close()
			return nil, rankZeroReq, false, err
		}
		if !jobID.Valid || strings.TrimSpace(jobID.String) == "" {
			continue
		}
		// Parse request_json once; reuse for type filter.
		var payloadMap map[string]any
		if requestJSON.Valid && requestJSON.String != "" {
			_ = json.Unmarshal([]byte(requestJSON.String), &payloadMap)
		}
		if len(allowedJobTypes) > 0 && !jobTypeAllowed(payloadMap, allowedJobTypes) {
			continue
		}
		if len(candidates) >= maxCandidates {
			break
		}
		candidates = append(candidates, rankCandidate{
			jobID:               jobID.String,
			retryCount:          retryCount.Int64,
			requestJSON:         requestJSON.String,
			resultJSON:          resultJSON.String,
			reqRC:               reqRC.String,
			reqTM:               reqTM.String,
			reqDeterministic:    reqDeterministic.Int64 != 0,
			reqCacheable:        reqCacheable.Int64 != 0,
			reqMinBandwidthMbps: reqMinBandwidthMbps.Float64,
			updatedAt:           updatedAt.String,
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, rankZeroReq, false, err
	}
	if len(candidates) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, rankZeroReq, false, err
		}
		return nil, rankZeroReq, false, nil
	}

	// 2. Reconstruct Requirements + Score against profile; filter
	//    Eligible=true; capture per-component breakdown for explain.
	//    PR #6: Requirements read from dedicated columns only; no JSON fallback.
	type scoredCandidate struct {
		jobID               string
		retry               int64
		resultJSON          string
		reqRC               string
		reqTM               string
		reqDeterministic    bool
		reqCacheable        bool
		reqMinBandwidthMbps float64
		score               float64
		exp                 costmodel.Explanation
		updatedAt           string
	}
	sel := make([]scoredCandidate, 0, len(candidates))
	for _, c := range candidates {
		req := reconstructRankRequirements(c.reqRC, c.reqTM, c.reqDeterministic, c.reqCacheable, c.reqMinBandwidthMbps)
		cost, exp := costmodel.Score(profile, req)
		if !cost.Eligible {
			continue
		}
		sel = append(sel, scoredCandidate{
			jobID:               c.jobID,
			retry:               c.retryCount,
			resultJSON:          c.resultJSON,
			reqRC:               c.reqRC,
			reqTM:               c.reqTM,
			reqDeterministic:    c.reqDeterministic,
			reqCacheable:        c.reqCacheable,
			reqMinBandwidthMbps: c.reqMinBandwidthMbps,
			score:               cost.Score,
			exp:                 exp,
			updatedAt:           c.updatedAt,
		})
	}
	if len(sel) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, costmodel.JobRequirements{}, false, err
		}
		return nil, costmodel.JobRequirements{}, false, nil
	}

	// 3. Sort ASC by Score (lower = better), then updated_at ASC for
	//    deterministic tiebreak across ties.
	sort.SliceStable(sel, func(i, j int) bool {
		if sel[i].score != sel[j].score {
			return sel[i].score < sel[j].score
		}
		return sel[i].updatedAt < sel[j].updatedAt
	})

	nowISO := now.UTC().Format(time.RFC3339)
	nowUnix := now.UTC().Unix()
	leaseExpiry := now.UTC().Add(30 * time.Minute).Format(time.RFC3339)

	// 4. Try CAS-claim each in sorted order. The per-id CAS body
	//    intentionally mirrors ClaimNextPendingJob's per-row CAS
	//    so the PR-04.6 diff stays minimal and the FIFO path is
	//    unaffected. A follow-up PR may extract a shared inner
	//    helper once both paths are stable.
	for _, sc := range sel {
		newRetry := int(sc.retry) + 1
		leaseID := uuid.NewString()

		// PR #7/#9: runtime fields removed — tasks carry them.
		// lease_id, attempt, lease_expiry kept for ClaimNextResult.
		resultMap := make(map[string]any)
		if sc.resultJSON != "" {
			_ = json.Unmarshal([]byte(sc.resultJSON), &resultMap)
		}
		resultMap["job_id"] = sc.jobID
		resultMap["status"] = "LEASED"
		resultMap["lease_id"] = leaseID
		resultMap["lease_expiry"] = leaseExpiry
		resultMap["attempt"] = newRetry
		resultMap["contract_version"] = 3
		resultMap["updated_at"] = nowUnix

		updatedResult, err := json.Marshal(resultMap)
		if err != nil {
			return nil, costmodel.JobRequirements{}, false, err
		}

		res, err := tx.Exec(
			`UPDATE jobs
			 SET status = ?, worker_name = ?, attempt = ?,
			     result_json = ?, updated_at = ?, migrated_at = ?,
			     assigned_at = ?,
			     claimed_at = ?
			 WHERE job_id = ?
			   AND UPPER(status) = 'PENDING'`,
			"LEASED", workerID, newRetry,
			string(updatedResult), nowISO, nowISO,
			nowISO,
			nowISO,
			sc.jobID,
		)
		if err != nil {
			return nil, costmodel.JobRequirements{}, false, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, costmodel.JobRequirements{}, false, err
		}
		if affected == 0 {
			continue // race: another worker claimed; try next-scored
		}

		history := []map[string]any{{
			"status":    "LEASED",
			"timestamp": nowISO,
			"worker_id": workerID,
			"message":   fmt.Sprintf("Job assigned to worker %s (rank-path)", workerID),
		}}
		if err := s.replaceJobHistoryTx(tx, sc.jobID, history); err != nil {
			return nil, rankZeroReq, false, err
		}
			// cleanup/remove-job-attempts-runtime: no longer write to job_attempts.
			// Per-attempt identity now lives on task_attempts (canonical); the
			// canonical claim path is the task-native dispatch (PR-04). The
			// legacy_job_dispatch path is being decommissioned alongside PR-07.
		if err := tx.Commit(); err != nil {
			return nil, costmodel.JobRequirements{}, false, err
		}
		claimedReq := reconstructRankRequirements(sc.reqRC, sc.reqTM, sc.reqDeterministic, sc.reqCacheable, sc.reqMinBandwidthMbps)
		_ = s.LogJobEvent(sc.jobID, "job_claimed", map[string]interface{}{
			"worker_id":    workerID,
			"lease_id":     leaseID,
			"attempt":      newRetry,
			"rank_score":   sc.score,
			"rank_eligible": true,
			"rank_bandwidth_fit": sc.exp.BandwidthFit,
		})
		return bytes.Clone(updatedResult), claimedReq, true, nil
	}

	// No candidate's CAS succeeded (every best-scored row was
	// raced by another worker this round). Caller maps nil → nil
	// into ErrNoClaimableJob at the repository layer.
	if err := tx.Commit(); err != nil {
		return nil, costmodel.JobRequirements{}, false, err
	}
	return nil, costmodel.JobRequirements{}, false, nil
}

// reconstructRankRequirements builds a costmodel.JobRequirements from dedicated
// column values (PR #6). All 5 fields come from columns; no JSON fallback.
func reconstructRankRequirements(rc, tm string, deterministic, cacheable bool, minBandwidthMbps float64) costmodel.JobRequirements {
	return costmodel.JobRequirements{
		ResourceClass:    costmodel.ResourceClass(strings.TrimSpace(rc)),
		TemporalMode:     costmodel.TemporalMode(strings.TrimSpace(tm)),
		Deterministic:    deterministic,
		Cacheable:        cacheable,
		MinBandwidthMbps: minBandwidthMbps,
	}
}
