package completion

import (
	"context"
	"fmt"
	"time"
)

// reconcile_scan.go owns the supervisor's candidate scan
// (scanCandidates): the 11-case UNION SELECT that surfaces the
// attempt_commits rows in mid-state on every tick. The dispatch
// of those candidates to Coordinator.ReconcileAttempt lives in
// reconcile_dispatch.go; the supervisor types + loop live in
// reconcile_supervisor.go.

// scanCandidates returns the (commit_id, case) pairs the
// supervisor wants to escalate. Returns the count of
// deadline-expired rows separately so the deadline counter is
// always incremented even if the supervisor's dispatch path
// later no-ops on the same row.
//
// The 11-case UNION is intentionally single-trip: SQLite handles
// the OR-of-cases planner-side. A non-existent table or column
// surfaces as a scan error which the supervisor logs and
// continues.
func (s *ReconcileSupervisor) scanCandidates(ctx context.Context) ([]ReconcileCandidate, int64, error) {
	q := `
SELECT commit_id, case_label
FROM (
    -- 1. DECLARED|UPLOADING and commit_deadline_at < NOW
    SELECT commit_id, 'deadline_expired' AS case_label
      FROM attempt_commits
     WHERE status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')
       AND commit_deadline_at < ?
    UNION ALL
    -- 2. orphan terminal task
    SELECT ac.commit_id, 'orphan_terminal_task' AS case_label
      FROM attempt_commits ac
      JOIN task_attempts ta ON ta.id = ac.attempt_id
     WHERE ta.status IN ('FAILED','CANCELLED','TIMED_OUT')
       AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
    UNION ALL
    -- 3. stale fence: lease_id mismatch vs tasks row
    SELECT ac.commit_id, 'stale_fence' AS case_label
      FROM attempt_commits ac
      JOIN tasks t ON t.task_id = ac.task_id
     WHERE t.lease_id != ac.lease_id
       AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
    UNION ALL
    -- 4. missing worker
    SELECT ac.commit_id, 'missing_worker' AS case_label
      FROM attempt_commits ac
      LEFT JOIN workers w ON w.worker_id = ac.worker_id
     WHERE w.worker_id IS NULL
       AND ac.status NOT IN ('COMMITTED','EXPIRED','CLEANED')
    UNION ALL
    -- 5. UPLOADING with zero declarations
    SELECT ac.commit_id, 'missing_declarations' AS case_label
      FROM attempt_commits ac
      LEFT JOIN (
          SELECT commit_id, COUNT(*) AS n
            FROM task_output_declarations
           GROUP BY commit_id
      ) d ON d.commit_id = ac.commit_id
     WHERE ac.status = 'UPLOADING'
       AND COALESCE(d.n, 0) = 0
    UNION ALL
    -- 6. all required declarations RECEIVED but no progress
    SELECT ac.commit_id, 'missing_commit' AS case_label
      FROM attempt_commits ac
     WHERE ac.status = 'RECEIVED'
       AND ac.required_output_count > 0
       AND ac.ready_output_count >= ac.required_output_count
       AND ac.last_progress_at < ?
    UNION ALL
    -- 7. upload stuck
    SELECT ac.commit_id, 'upload_stuck' AS case_label
      FROM attempt_commits ac
     WHERE ac.status = 'UPLOADING'
       AND ac.last_progress_at < ?
    UNION ALL
    -- 8. fence expired: lease was issued (t.lease_id != '')
    --    but the task has no worker assigned (t.worker_id = '').
    --    This is the canonical "lease issued, never picked up"
    --    state — distinct from case 3 (stale_fence) which is
    --    "lease_id mismatch after a reaped lease".
    --    Orthogonal: case 3 is t.lease_id != ac.lease_id;
    --    case 8 is t.worker_id = '' AND t.lease_id != ''.
    SELECT ac.commit_id, 'fence_expired' AS case_label
      FROM attempt_commits ac
      JOIN tasks t ON t.task_id = ac.task_id
     WHERE t.lease_id != ''
       AND t.worker_id = ''
       AND ac.status = 'DECLARED'
    UNION ALL
    -- 9. outbox event PENDING too long
    -- The outbox row is keyed by aggregate_id=task_id (see
    -- coordinator.CommitAttempt step 6); the commit_id lives
    -- in payload_json. We JOIN on (aggregate_type, aggregate_id)
    -- and additionally verify the payload carries the commit_id
    -- (LIKE match) so a stale event for a sibling task does
    -- not surface as outbox_pending_too_long for THIS attempt.
    SELECT ac.commit_id, 'outbox_pending_too_long' AS case_label
      FROM attempt_commits ac
      JOIN outbox_events oe
        ON oe.aggregate_type = 'task'
       AND oe.aggregate_id = ac.task_id
       AND oe.event_type = 'commit_protocol.committed'
       AND oe.payload_json LIKE '%' || ac.commit_id || '%'
     WHERE oe.status = 'PENDING'
       AND oe.created_at < ?
    UNION ALL
    -- 10. required_outputs missing
    SELECT ac.commit_id, 'required_outputs_missing' AS case_label
      FROM attempt_commits ac
     WHERE ac.status = 'AWAITING_REQUIRED'
       AND ac.required_output_count > ac.ready_output_count
    UNION ALL
    -- 11. all tasks SUCCEEDED but no job_deliveries rows
    -- The job_deliveries table has no commit_id column. The
    -- canonical join is via artifacts:
    --   artifacts.job_id  = ac.job_id
    --   job_deliveries.artifact_id = artifacts.id
    -- A "missing deliveries" state is detected at the JOB level
    -- (all artifacts in this job have zero delivery rows).
    SELECT ac.commit_id, 'job_all_succeeded_no_job_deliveries' AS case_label
      FROM attempt_commits ac
      JOIN tasks t ON t.task_id = ac.task_id
     WHERE t.status = 'SUCCEEDED'
       AND ac.status = 'COMMITTED'
       AND NOT EXISTS (
           SELECT 1
             FROM artifacts a
             JOIN job_deliveries jd ON jd.artifact_id = a.id
            WHERE a.job_id = ac.job_id
       )
)
ORDER BY commit_id
LIMIT ?`
	now := time.Now().UTC()
	deadlineCutoff := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	progressCutoff := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	outboxCutoff := now.Add(-1 * time.Hour).Format(time.RFC3339Nano)
	// Placeholder order (5 total) maps to the ? marks in the
	// UNION query above:
	//   #1 case 1 (deadline_expired)      → commit_deadline_at < ?
	//   #2 case 6 (missing_commit)        → last_progress_at < ?
	//   #3 case 7 (upload_stuck)          → last_progress_at < ?
	//   #4 case 9 (outbox_pending)        → oe.created_at < ?
	//   #5 LIMIT                         → s.Limit
	rows, err := s.DB.QueryContext(ctx, q,
		now.Format(time.RFC3339Nano), // #1 deadline_expired
		deadlineCutoff,               // #2 missing_commit
		progressCutoff,               // #3 upload_stuck
		outboxCutoff,                 // #4 outbox_pending_too_long
		s.Limit,                      // #5 LIMIT
	)
	if err != nil {
		return nil, 0, fmt.Errorf("reconcile: scan: %w", err)
	}
	defer rows.Close()
	var out []ReconcileCandidate
	var deadlineExpired int64
	for rows.Next() {
		var commitID, caseLabel string
		if err := rows.Scan(&commitID, &caseLabel); err != nil {
			return nil, 0, fmt.Errorf("reconcile: scan row: %w", err)
		}
		if commitID == "" || caseLabel == "" {
			continue
		}
		if caseLabel == string(CaseDeadlineExpired) {
			deadlineExpired++
		}
		// Dedup: skip if seen in a prior tick (the map is GC'd at
		// seenCap; bounded-window double-fire acceptable).
		s.seenMu.Lock()
		if _, ok := s.seenIDs[commitID+":"+caseLabel]; ok {
			s.seenMu.Unlock()
			continue
		}
		s.seenIDs[commitID+":"+caseLabel] = now
		s.seenMu.Unlock()
		out = append(out, ReconcileCandidate{CommitID: commitID, Case: ReconcileCase(caseLabel)})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("reconcile: rows: %w", err)
	}
	return out, deadlineExpired, nil
}
