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
// Returns the updated result_json blob and true if a job was claimed.
//
// PR-04.5: the per-job Requirements (columns
// job_required_resource_class + job_required_temporal_mode + the
// `_requirements` JSON sub-object inside request_json) are mirrored
// into the result_json blob under the same `_requirements` key. The
// future-rank site (PR-04.6) reads them straight from the blob; the
// reader path (jobs.Writer.Get → jobs.Job.Requirements) reconstructs
// them from the dedicated columns (canonical) with the JSON fallback
// for legacy rows.
func (s *SQLiteStore) ClaimNextPendingJob(workerID string, allowedJobTypes []string, now time.Time) ([]byte, bool, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Read candidate jobs with their status and assigned_to columns (not raw_json)
	rows, err := tx.Query(
		`SELECT job_id, status, assigned_to, claimed_by, job_fingerprint, run_id, job_run_id,
		        video_name, project_id, retry_count, request_json, result_json,
		        COALESCE(job_required_resource_class, ''),
		        COALESCE(job_required_temporal_mode, '')
		 FROM jobs
		 WHERE UPPER(status) = 'PENDING'
		   AND COALESCE(assigned_to, '') = ''
		 ORDER BY COALESCE(updated_at, created_at) ASC, job_id ASC`,
	)
	if err != nil {
		return nil, false, err
	}

	nowISO := now.UTC().Format(time.RFC3339)
	nowUnix := now.UTC().Unix()

	for rows.Next() {
		var (
			jobID, status, assignedTo, claimedBy, jobFingerprint, runID, jobRunID sql.NullString
			videoName, projectID                                                  sql.NullString
			retryCount                                                            sql.NullInt64
			requestJSON, resultJSON                                               sql.NullString
			requiredResourceClass, requiredTemporalMode                            sql.NullString
		)
		if err := rows.Scan(&jobID, &status, &assignedTo, &claimedBy, &jobFingerprint, &runID, &jobRunID,
			&videoName, &projectID, &retryCount, &requestJSON, &resultJSON,
			&requiredResourceClass, &requiredTemporalMode); err != nil {
			rows.Close()
			return nil, false, err
		}

		// Double-check safety: already claimed
		if assignedTo.Valid && strings.TrimSpace(assignedTo.String) != "" {
			continue
		}
		if claimedBy.Valid && strings.TrimSpace(claimedBy.String) != "" {
			continue
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

		// Build the updated result_json blob with claim data
		resultMap := make(map[string]any)
		if resultJSON.Valid && resultJSON.String != "" {
			_ = json.Unmarshal([]byte(resultJSON.String), &resultMap)
		}
		resultMap["job_id"] = jobID.String
		resultMap["status"] = "LEASED"
		resultMap["assigned_to"] = workerID
		resultMap["worker_name"] = workerID
		resultMap["assigned_at"] = nowISO
		resultMap["claimed_by"] = workerID
		resultMap["claimed_at"] = nowISO
		resultMap["lease_id"] = leaseID
		resultMap["lease_expiry"] = leaseExpiry
		resultMap["lease_expires_at"] = leaseExpiry
		resultMap["attempt"] = newRetry
		resultMap["contract_version"] = 2
		resultMap["updated_at"] = nowUnix
		resultMap["retry_count"] = newRetry

		// PR-04.5 + PR-04.6: mirror per-job Requirements into
		// result_json so the dispatch path
		// (handler_workers.sendPushJobOffer + future rank site) can
		// read them straight from the response blob without bouncing
		// through jobs.Writer.Get. ResourceClass + TemporalMode are
		// sourced from the dedicated columns; the rank-only
		// Deterministic + Cacheable + MinBandwidthMbps come from the
		// JSON sub-object inside request_json (already canonical
		// there; PR-04.6 only adds the bandwidth field).
		if requiredResourceClass.Valid && strings.TrimSpace(requiredResourceClass.String) != "" {
			resultMap["_requirements"] = map[string]any{
				"resource_class": strings.TrimSpace(requiredResourceClass.String),
				"temporal_mode":  strings.TrimSpace(requiredTemporalMode.String),
			}
			if requestJSON.Valid && requestJSON.String != "" {
				var reqParsed map[string]any
				if err := json.Unmarshal([]byte(requestJSON.String), &reqParsed); err == nil {
					if sub, ok := reqParsed["_requirements"].(map[string]any); ok {
						if v, ok := sub["deterministic"].(bool); ok {
							resultMap["_requirements"].(map[string]any)["deterministic"] = v
						}
						if v, ok := sub["cacheable"].(bool); ok {
							resultMap["_requirements"].(map[string]any)["cacheable"] = v
						}
						if v, ok := sub["min_bandwidth_mbps"].(float64); ok {
							resultMap["_requirements"].(map[string]any)["min_bandwidth_mbps"] = v
						}
					}
				}
			}
		}

		updatedResult, err := json.Marshal(resultMap)
		if err != nil {
			rows.Close()
			return nil, false, err
		}

		result, err := tx.Exec(
			`UPDATE jobs
			 SET status = ?, assigned_to = ?, worker_name = ?, retry_count = ?, attempt = ?,
			     result_json = ?, updated_at = ?, migrated_at = ?,
			     assigned_at = ?,
			     lease_id = ?, lease_expiry = ?,
			     claimed_by = ?, claimed_at = ?
			 WHERE job_id = ?
			   AND UPPER(status) = 'PENDING'
			   AND COALESCE(assigned_to, '') = ''`,
			"LEASED", workerID, workerID, newRetry, newRetry,
			string(updatedResult), nowISO, nowISO,
			nowISO,
			leaseID, leaseExpiry,
			workerID, nowISO,
			jobID.String,
		)
		if err != nil {
			rows.Close()
			return nil, false, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			rows.Close()
			return nil, false, err
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
			return nil, false, err
		}

		// Record job attempt
		insertedID, attemptErr := s.InsertJobAttemptTx(tx, jobID.String, newRetry, workerID, leaseID)
		if attemptErr != nil {
			rows.Close()
			return nil, false, fmt.Errorf("failed to record job attempt: %w", attemptErr)
		}

		if err := tx.Commit(); err != nil {
			rows.Close()
			return nil, false, err
		}
		rows.Close()

		if insertedID > 0 {
			_ = s.LogJobEvent(jobID.String, "job_claimed", map[string]interface{}{
				"worker_id": workerID, "lease_id": leaseID, "attempt": newRetry,
			})
		}
		return bytes.Clone(updatedResult), true, nil
	}

	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return nil, false, nil
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
type rankCandidate struct {
	jobID                 string
	retryCount            int64
	requestJSON           string
	resultJSON            string
	reqRC                 string
	reqTM                 string
	updatedAt             string
}

func (s *SQLiteStore) ClaimNextPendingJobForWorker(
	ctx context.Context,
	workerID string,
	allowedJobTypes []string,
	profile costmodel.WorkerProfile,
	maxCandidates int,
	now time.Time,
) ([]byte, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, fmt.Errorf("store not initialized")
	}
	if workerID == "" {
		return nil, false, fmt.Errorf("claim with empty workerID")
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

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// 1. Over-fetch PENDING candidates (4x cap) to absorb
	//    job_type filter dropouts; keep FIFO pre-sort so the rank's
	//    secondary tiebreak (updated_at ASC) is consistent across
	//    score-tied candidates.
	rows, err := tx.Query(
		`SELECT job_id, retry_count,
		        COALESCE(request_json, ''),
		        COALESCE(result_json, ''),
		        COALESCE(job_required_resource_class, ''),
		        COALESCE(job_required_temporal_mode, ''),
		        COALESCE(updated_at, created_at)
		 FROM jobs
		 WHERE UPPER(status) = 'PENDING'
		   AND COALESCE(assigned_to, '') = ''
		 ORDER BY COALESCE(updated_at, created_at) ASC, job_id ASC
		 LIMIT ?`,
		maxCandidates*4,
	)
	if err != nil {
		return nil, false, err
	}

	var candidates []rankCandidate
	for rows.Next() {
		var (
			jobID                          sql.NullString
			retryCount                     sql.NullInt64
			requestJSON, resultJSON        sql.NullString
			reqRC, reqTM, updatedAt        sql.NullString
		)
		if err := rows.Scan(&jobID, &retryCount, &requestJSON, &resultJSON,
			&reqRC, &reqTM, &updatedAt); err != nil {
			rows.Close()
			return nil, false, err
		}
		if !jobID.Valid || strings.TrimSpace(jobID.String) == "" {
			continue
		}
		// Parse request_json once; reuse for type filter + req fallback.
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
			jobID:       jobID.String,
			retryCount:  retryCount.Int64,
			requestJSON: requestJSON.String,
			resultJSON:  resultJSON.String,
			reqRC:       reqRC.String,
			reqTM:       reqTM.String,
			updatedAt:   updatedAt.String,
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(candidates) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	// 2. Reconstruct Requirements + Score against profile; filter
	//    Eligible=true; capture per-component breakdown for explain.
	//    reqRC + reqTM + requestJSON are carried into each entry so the
	//    CAS loop below can re-stitch the result_json._requirements
	//    subobject when CLAIMing the row (parallel to
	//    ClaimNextPendingJob's mirror logic).
	type scoredCandidate struct {
		jobID       string
		retry       int64
		resultJSON  string
		reqRC       string
		reqTM       string
		requestJSON string
		score       float64
		exp         costmodel.Explanation
		updatedAt   string
	}
	sel := make([]scoredCandidate, 0, len(candidates))
	for _, c := range candidates {
		req := reconstructRankRequirements(c.reqRC, c.reqTM, c.requestJSON)
		cost, exp := costmodel.Score(profile, req)
		if !cost.Eligible {
			continue
		}
		sel = append(sel, scoredCandidate{
			jobID:       c.jobID,
			retry:       c.retryCount,
			resultJSON:  c.resultJSON,
			reqRC:       c.reqRC,
			reqTM:       c.reqTM,
			requestJSON: c.requestJSON,
			score:       cost.Score,
			exp:         exp,
			updatedAt:   c.updatedAt,
		})
	}
	if len(sel) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
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

		resultMap := make(map[string]any)
		if sc.resultJSON != "" {
			_ = json.Unmarshal([]byte(sc.resultJSON), &resultMap)
		}
		resultMap["job_id"] = sc.jobID
		resultMap["status"] = "LEASED"
		resultMap["assigned_to"] = workerID
		resultMap["worker_name"] = workerID
		resultMap["assigned_at"] = nowISO
		resultMap["claimed_by"] = workerID
		resultMap["claimed_at"] = nowISO
		resultMap["lease_id"] = leaseID
		resultMap["lease_expiry"] = leaseExpiry
		resultMap["lease_expires_at"] = leaseExpiry
		resultMap["attempt"] = newRetry
		resultMap["contract_version"] = 2
		resultMap["updated_at"] = nowUnix
		resultMap["retry_count"] = newRetry

		// PR-04.5 + PR-04.6: mirror per-job Requirements into
		// result_json so the offer-decoder side reads the same
		// payload whether it goes through jobsRepo.Get (column path)
		// or directly through result_json (legacy decoder path).
		rcTrim := strings.TrimSpace(sc.reqRC)
		tmTrim := strings.TrimSpace(sc.reqTM)
		if rcTrim != "" || tmTrim != "" {
			reqSub := map[string]any{
				"resource_class": rcTrim,
				"temporal_mode":  tmTrim,
			}
			if sc.requestJSON != "" {
				var reqParsed map[string]any
				if err := json.Unmarshal([]byte(sc.requestJSON), &reqParsed); err == nil {
					if sub, ok := reqParsed["_requirements"].(map[string]any); ok {
						if v, ok := sub["deterministic"].(bool); ok {
							reqSub["deterministic"] = v
						}
						if v, ok := sub["cacheable"].(bool); ok {
							reqSub["cacheable"] = v
						}
						if v, ok := sub["min_bandwidth_mbps"].(float64); ok {
							reqSub["min_bandwidth_mbps"] = v
						}
					}
				}
			}
			resultMap["_requirements"] = reqSub
		}

		updatedResult, err := json.Marshal(resultMap)
		if err != nil {
			return nil, false, err
		}

		res, err := tx.Exec(
			`UPDATE jobs
			 SET status = ?, assigned_to = ?, worker_name = ?, retry_count = ?, attempt = ?,
			     result_json = ?, updated_at = ?, migrated_at = ?,
			     assigned_at = ?,
			     lease_id = ?, lease_expiry = ?,
			     claimed_by = ?, claimed_at = ?
			 WHERE job_id = ?
			   AND UPPER(status) = 'PENDING'
			   AND COALESCE(assigned_to, '') = ''`,
			"LEASED", workerID, workerID, newRetry, newRetry,
			string(updatedResult), nowISO, nowISO,
			nowISO,
			leaseID, leaseExpiry,
			workerID, nowISO,
			sc.jobID,
		)
		if err != nil {
			return nil, false, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, false, err
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
			return nil, false, err
		}
		if _, err := s.InsertJobAttemptTx(tx, sc.jobID, newRetry, workerID, leaseID); err != nil {
			return nil, false, fmt.Errorf("failed to record job attempt: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		_ = s.LogJobEvent(sc.jobID, "job_claimed", map[string]interface{}{
			"worker_id":    workerID,
			"lease_id":     leaseID,
			"attempt":      newRetry,
			"rank_score":   sc.score,
			"rank_eligible": true,
			"rank_bandwidth_fit": sc.exp.BandwidthFit,
		})
		return bytes.Clone(updatedResult), true, nil
	}

	// No candidate's CAS succeeded (every best-scored row was
	// raced by another worker this round). Caller maps nil → nil
	// into ErrNoClaimableJob at the repository layer.
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return nil, false, nil
}

// reconstructRankRequirements mirrors reconstructRequirements in
// sqlite_jobs_writer.go per-job; localised here so the rank loop
// stays decoupled from the canonical JobRecord path. The two
// implementations are kept textually identical (single-tx race
// rows use this one; reader-path decode uses the other).
func reconstructRankRequirements(rc, tm, requestJSON string) costmodel.JobRequirements {
	rcTrim := strings.TrimSpace(rc)
	tmTrim := strings.TrimSpace(tm)
	if rcTrim == "" && tmTrim == "" {
		if requestJSON == "" {
			return costmodel.DefaultRequirements()
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(requestJSON), &payload); err != nil {
			return costmodel.DefaultRequirements()
		}
		jsonReq := readRankRequirementsFromJSON(payload)
		if jsonReq.ResourceClass != "" || jsonReq.TemporalMode != "" ||
			jsonReq.Deterministic || jsonReq.Cacheable || jsonReq.MinBandwidthMbps > 0 {
			return jsonReq
		}
		return costmodel.DefaultRequirements()
	}
	var jsonReq costmodel.JobRequirements
	if requestJSON != "" {
		var payload map[string]any
		if err := json.Unmarshal([]byte(requestJSON), &payload); err == nil {
			jsonReq = readRankRequirementsFromJSON(payload)
		}
	}
	return costmodel.JobRequirements{
		ResourceClass:    costmodel.ResourceClass(rcTrim),
		TemporalMode:     costmodel.TemporalMode(tmTrim),
		Deterministic:    jsonReq.Deterministic,
		Cacheable:        jsonReq.Cacheable,
		MinBandwidthMbps: jsonReq.MinBandwidthMbps,
	}
}

// readRankRequirementsFromJSON reads the `_requirements` sub-object
// from a parsed payload without leaning on the sqlite_jobs_writer
// helpers (which have different field shapes from the rank loop's
// local reconstruction). Single source of truth for the rank path;
// mirror of requirementsFromPayload in sqlite_jobs_writer.go BUT
// adapted to ONLY return non-zero fields when present so the rank
// loop's row-level reconstruction stays in lockstep with what the
// column read would have produced for legacy rows.
func readRankRequirementsFromJSON(payload map[string]any) costmodel.JobRequirements {
	if payload == nil {
		return costmodel.JobRequirements{}
	}
	raw, ok := payload["_requirements"].(map[string]any)
	if !ok || raw == nil {
		return costmodel.JobRequirements{}
	}
	req := costmodel.JobRequirements{}
	if v, ok := raw["resource_class"].(string); ok {
		req.ResourceClass = costmodel.ResourceClass(strings.TrimSpace(v))
	}
	if v, ok := raw["temporal_mode"].(string); ok {
		req.TemporalMode = costmodel.TemporalMode(strings.TrimSpace(v))
	}
	if v, ok := raw["deterministic"].(bool); ok {
		req.Deterministic = v
	}
	if v, ok := raw["cacheable"].(bool); ok {
		req.Cacheable = v
	}
	if v, ok := raw["min_bandwidth_mbps"].(float64); ok {
		req.MinBandwidthMbps = v
	}
	return req
}
