package store

// store_jobs_claim_ranked.go — ranked/cost-model claim path
// (PR-04.6 sibling of the FIFO path in store_jobs_claim_fifo.go).
//
// Flow:
//  1. Over-fetch PENDING candidates (4× cap) into a bounded
//     candidate pool — FIFO pre-sort gives a deterministic
//     (updated_at ASC) tiebreak across score-tied ranks.
//  2. Reconstruct Requirements from dedicated columns and
//     score each candidate against the supplied costmodel.WorkerProfile.
//     Filter Eligible=true and capture the per-component breakdown
//     for the explain payload.
//  3. Sort ASC by Score (lower = better), then by updated_at ASC for
//     deterministic tiebreak across ties.
//  4. Attempt CAS-claim each candidate in sorted order via the
//     shared write helper (claimJobTx in store_jobs_claim_tx.go).
//
// Per-row CAS is owned by `claimJobTx`; this file owns:
//   - the over-fetch pool (4× cap → bounded pool)
//   - the scoring sort
//   - tx.Commit() on success / exhaustion
//   - LogJobEvent with the FIG-rank payload (rank_score,
//     rank_eligible, rank_bandwidth_fit on top of FIFO fields).
//
// PR #6: Requirements read from dedicated columns only; no JSON
// fallback. PR #9: assigned_to column dropped — filter only by
// PENDING status.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"velox-server/internal/costmodel"
)

// rankCandidate captures the per-row data needed to score and CAS a
// single pending job for the rank loop. Lives in
// store_jobs_claim_ranked.go as a package-local type so it doesn't
// leak into the public store API.
//
// PR #6: all 5 Requirements columns read directly; no JSON fallback.
type rankCandidate struct {
	jobID               string
	retryCount          int64
	requestJSON         string
	resultJSON          string
	reqRC               string
	reqTM               string
	reqDeterministic    bool
	reqCacheable        bool
	reqMinBandwidthMbps float64
	updatedAt           string
}

// reconstructRankRequirements builds a costmodel.JobRequirements
// from dedicated column values (PR #6). All 5 fields come from
// columns; no JSON fallback. Underscores-prefixed field names match
// the SQL column casing used in the SELECT body.
func reconstructRankRequirements(rc, tm string, deterministic, cacheable bool, minBandwidthMbps float64) costmodel.JobRequirements {
	return costmodel.JobRequirements{
		ResourceClass:    costmodel.ResourceClass(strings.TrimSpace(rc)),
		TemporalMode:     costmodel.TemporalMode(strings.TrimSpace(tm)),
		Deterministic:    deterministic,
		Cacheable:        cacheable,
		MinBandwidthMbps: minBandwidthMbps,
	}
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

	// ── 1. Over-fetch PENDING candidates (4× cap) ─────────────────────
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
	defer rows.Close()

	var candidates []rankCandidate
	for rows.Next() {
		var (
			jobID                          sql.NullString
			retryCount                     sql.NullInt64
			requestJSON, resultJSON        sql.NullString
			reqRC, reqTM                   sql.NullString
			reqDeterministic, reqCacheable sql.NullInt64
			reqMinBandwidthMbps            sql.NullFloat64
			updatedAt                      sql.NullString
		)
		if err := rows.Scan(&jobID, &retryCount, &requestJSON, &resultJSON,
			&reqRC, &reqTM,
			&reqDeterministic, &reqCacheable, &reqMinBandwidthMbps,
			&updatedAt); err != nil {
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
	if err := rows.Err(); err != nil {
		return nil, rankZeroReq, false, err
	}
	if len(candidates) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, rankZeroReq, false, err
		}
		return nil, rankZeroReq, false, nil
	}

	// ── 2. Reconstruct Requirements + Score against profile; filter
	// Eligible=true; capture per-component breakdown for explain. ────
	type scoredCandidate struct {
		jobID, resultJSON, reqRC, reqTM, updatedAt string
		retry                                     int64
		reqDeterministic, reqCacheable             bool
		reqMinBandwidthMbps                        float64
		score                                      float64
		exp                                        costmodel.Explanation
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

	// ── 3. Sort ASC by Score (lower = better), then updated_at ASC for
	// deterministic tiebreak across ties. ───────────────────────────
	sort.SliceStable(sel, func(i, j int) bool {
		if sel[i].score != sel[j].score {
			return sel[i].score < sel[j].score
		}
		return sel[i].updatedAt < sel[j].updatedAt
	})

	// ── 4. Try CAS-claim each in sorted order. Per-id CAS body lives
	// in claimJobTx; LogJobEvent payload carries the rank fields
	// (rank_score, rank_eligible, rank_bandwidth_fit) that the FIFO
	// path does NOT emit, so this is the divergence point between
	// the two path files. ─────────────────────────────────────────────
	for _, sc := range sel {
		requirements := reconstructRankRequirements(
			sc.reqRC, sc.reqTM, sc.reqDeterministic, sc.reqCacheable, sc.reqMinBandwidthMbps)

		outcome, err := s.claimJobTx(
			ctx, tx,
			sc.jobID, int(sc.retry)+1,
			sc.resultJSON, workerID, now,
			fmt.Sprintf("Job assigned to worker %s (rank-path)", workerID),
			requirements,
		)
		if errors.Is(err, ErrClaimCASLost) {
			// Race: another worker claimed; try next-scored candidate.
			continue
		}
		if err != nil {
			return nil, rankZeroReq, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, rankZeroReq, false, err
		}

		_ = s.LogJobEvent(sc.jobID, "job_claimed", map[string]interface{}{
			"worker_id":          workerID,
			"lease_id":           outcome.LeaseID,
			"attempt":            outcome.NewRetry,
			"rank_score":         sc.score,
			"rank_eligible":      true,
			"rank_bandwidth_fit": sc.exp.BandwidthFit,
		})
		return outcome.ResultJSON, outcome.Requirements, true, nil
	}

	// No candidate's CAS succeeded (every best-scored row was raced
	// by another worker this round). Caller maps nil → nil into
	// ErrNoClaimableJob at the repository layer.
	if err := tx.Commit(); err != nil {
		return nil, costmodel.JobRequirements{}, false, err
	}
	return nil, costmodel.JobRequirements{}, false, nil
}
