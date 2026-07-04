package store

// store_jobs_claim_tx.go — shared private write-side helper for the
// two exported job-claim paths (FIFO + ranked/cost-model):
//
//   - ClaimNextPendingJob          (store_jobs_claim_fifo.go)
//   - ClaimNextPendingJobForWorker (store_jobs_claim_ranked.go)
//
// claimJobTx runs the per-row write side (result_json overlay, CAS
// gated on PENDING, LEASED history append) and does NOT commit; the
// caller owns tx lifecycle and the path-specific LogJobEvent payload.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/costmodel"
)

// ErrClaimCASLost is the sentinel returned by claimJobTx when the
// per-row UPDATE affected zero rows (i.e. another worker raced us
// and the PENDING row is no longer claimable). Callers should treat
// it as a SOFT failure — "try the next candidate" — rather than
// aborting the whole claim loop.
var ErrClaimCASLost = errors.New("store: claim CAS lost (row no longer PENDING)")

// leaseOutcome is the per-job snapshot claimJobTx returns on success.
// Holds the canonical result_json blob the worker will hand off, a
// fresh lease_id, the new retry count, and the requirements column
// set the caller passed in (so callers don't have to re-scan after
// the CAS).
//
// Lower-case on purpose: this is package-private. The exported
// surface of this package is unchanged — the public claim functions
// still `(result_json, JobRequirements, claimed_bool, error)`.
type leaseOutcome struct {
	ResultJSON   []byte
	LeaseID      string
	NewRetry     int
	Requirements costmodel.JobRequirements
}

// claimJobTx runs the shared write-side on the supplied open tx
// (NO commit): result_json overlay, CAS UPDATE gated on PENDING,
// append LEASED history. Zero-affected UPDATE returns ErrClaimCASLost.
func (s *SQLiteStore) claimJobTx(
	_ context.Context,
	tx *sql.Tx,
	jobID string,
	newRetry int,
	sourceResultJSON string,
	workerID string,
	now time.Time,
	historyMessage string,
	requirements costmodel.JobRequirements,
) (leaseOutcome, error) {
	var zero leaseOutcome
	if s == nil || tx == nil {
		return zero, fmt.Errorf("store: claimJobTx requires non-nil store and tx")
	}
	if jobID == "" || workerID == "" {
		return zero, fmt.Errorf("store: claimJobTx requires non-empty jobID and workerID")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	nowISO := now.UTC().Format(time.RFC3339)
	nowUnix := now.UTC().Unix()
	leaseExpiry := now.UTC().Add(30 * time.Minute).Format(time.RFC3339)
	leaseID := uuid.NewString()

	// ── result_json overlay (LEASE-state fields kept for downstream parsing).
	resultMap := make(map[string]any)
	if sourceResultJSON != "" {
		_ = json.Unmarshal([]byte(sourceResultJSON), &resultMap)
	}
	resultMap["job_id"] = jobID
	resultMap["status"] = "LEASED"
	resultMap["lease_id"] = leaseID
	resultMap["lease_expiry"] = leaseExpiry
	resultMap["attempt"] = newRetry
	resultMap["contract_version"] = 3
	resultMap["updated_at"] = nowUnix

	updatedResult, err := json.Marshal(resultMap)
	if err != nil {
		return zero, err
	}

	// ── CAS-gated UPDATE (status must be 'PENDING'); zero affected → ErrClaimCASLost.
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
		jobID,
	)
	if err != nil {
		return zero, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return zero, err
	}
	if affected == 0 {
		// Concurrent winner already flipped PENDING → LEASED.
		// Caller skips to next candidate without committing.
		return zero, ErrClaimCASLost
	}

	// ── History (not in blob — separate table).
	history := []map[string]any{{
		"status":    "LEASED",
		"timestamp": nowISO,
		"worker_id": workerID,
		"message":   historyMessage,
	}}
	if err := s.replaceJobHistoryTx(tx, jobID, history); err != nil {
		return zero, err
	}

	return leaseOutcome{
		ResultJSON:   bytes.Clone(updatedResult),
		LeaseID:      leaseID,
		NewRetry:     newRetry,
		Requirements: requirements,
	}, nil
}

// jobTypeAllowed is the canonical payload-shape filter for BOTH
// claimed paths. Lives here (not in either path file) because both
// callers parse request_json identically to apply the same filter.
//
// Returns true when the payload's job_type (top-level or nested
// under `parameters`) is in the allowedJobTypes list (case-insensitive
// trim). An empty/missing job_type is treated as "not filtered out"
// — we don't fail closed on missing type info because the PENDING
// status filtering already constrains the row pool.
//
// Empty allowedJobTypes means "no filter".
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
