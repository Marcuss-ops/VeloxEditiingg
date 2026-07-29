// Package dispatchable provides the canonical "next dispatchable
// jobs" SELECT shared by two consumers:
//
//   - The master scheduler (DataServer/internal/taskgraph) reads this
//     function to feed the placement matcher. After matching, the
//     scheduler routes to the atomic claim path
//     (taskgraph.Repository.ClaimNextWithAttemptAtomic) which is the
//     ONLY writer of leased-task state. ListNextDispatchableJobs
//     itself never writes.
//
//   - The asset-cache snapshot service
//     (DataServer/internal/protectedasset or equivalent, see
//     docs/architecture/asset-cache-protected-snapshot.md Pass 5)
//     reads the same SELECT to get the next N jobs whose Drive
//     clips must be protected from the worker's cache cleaner.
//
// Single source of truth: changing the WHERE / ORDER BY / LIMIT
// shape here changes both consumers' view in lockstep. No WHEREs
// or ORDER BYs are duplicated across packages.
package dispatchable

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Querier is the minimal surface this package needs. Both *sql.DB
// and *sql.Tx satisfy it, so callers may run the query inside an
// existing transaction when needed (the snapshot service does not,
// but the scheduler might later).
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// DefaultLimit is the canonical lookahead cap when a caller passes
// limit <= 0. Matched to the design doc's "prossimi 10 job" — the
// snapshot endpoint specifically targets this value.
const DefaultLimit = 10

// Job is the canonical row shape returned by ListNextDispatchableJobs.
// It is a *superset* of placement.TaskCandidate plus a Payload
// column. Both consumers ignore the fields they do not need:
//
//   - Scheduler uses TaskID, JobID, Revision, Priority, CreatedAt,
//     ExecutorID/Version, RequiredCapabilities. (Payload is loaded
//     lazily on Claim, so it is currently redundant-but-cheap for
//     scheduler use; keeping it in the same SELECT keeps the WHERE /
//     ORDER BY contract in one place.)
//   - Snapshot uses JobID + Payload to extract Drive URLs. Executor
//     and Capabilities are ignored.
//
// One rich SELECT beats two narrow SELECTs that could drift.
type Job struct {
	TaskID               string
	JobID                string
	Revision             int
	Priority             int
	CreatedAt            time.Time
	ExecutorID           string
	ExecutorVersion      int
	RequiredCapabilities []string
	Payload              json.RawMessage

	// PlacementPinWorkerID is extracted from the task spec payload
	// (_placement_pin_worker_id). When non-empty, the placement
	// matcher will only dispatch this task to the named worker.
	PlacementPinWorkerID string
}

// listQuery is the canonical SELECT. Mirror of the SQL previously
// inlined in DataServer/internal/store/sqlite_task_query.go's
// ListReadyCandidates, augmented with the LEFT JOIN to task_specs
// for payload_json (snapshot use case). The existing
// sqlite_task_repository_list_ready_candidates_test.go verifies
// the projection for the scheduler; the snapshot-side tests
// (added in Pass 12 with the ADR) verify the payload projection.
//
// Mirrors ClauseVille WHERE/ORDER BY semantics from
// sqlite_task_atomic.go::ClaimNextWithAttemptAtomic lines 90-92 to
// guarantee consistent ordering between selection and claim.
const listQuery = `
SELECT t.task_id,
       t.job_id,
       t.revision,
       t.priority,
       t.created_at,
       t.executor_id,
       t.executor_version,
       COALESCE(GROUP_CONCAT(tr.capability), '') AS required_capabilities,
       COALESCE(ts.payload_json, '')            AS payload_json
FROM tasks t
LEFT JOIN task_requirements tr ON tr.task_id = t.task_id
LEFT JOIN task_specs       ts  ON ts.task_id = t.task_id
WHERE t.status = 'READY'
  AND (t.worker_id = '' OR t.worker_id IS NULL)
GROUP BY t.task_id
ORDER BY t.priority DESC, t.created_at ASC
LIMIT ?`

// ListNextDispatchableJobs returns up to `limit` dispatch-ready
// jobs ordered for the dispatcher (priority DESC, created_at ASC).
//
// Pure SELECT: no writes, no claims. Callers that need to actually
// LEASE a task must NOT use this — they must route through the
// scheduler's atomic claim path
// (taskgraph.Repository.ClaimNextWithAttemptAtomic) so the audit
// tuple (status, worker_id, lease_id, attempt_id, attempt_number)
// is stamped in one transaction.
//
// limit <= 0 defaults to DefaultLimit (10). Pass an explicit value
// for production use; the default is here to keep wiring ergonomic
// in tests + wiring.
func ListNextDispatchableJobs(ctx context.Context, db Querier, limit int) ([]Job, error) {
	if db == nil {
		return nil, fmt.Errorf("dispatchable.ListNextDispatchableJobs: db is nil")
	}
	if limit <= 0 {
		limit = DefaultLimit
	}
	rows, err := db.QueryContext(ctx, listQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("dispatchable.ListNextDispatchableJobs: query: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		var (
			j          Job
			createdAt  string
			capsConcat sql.NullString
			payloadStr sql.NullString
		)
		if err := rows.Scan(
			&j.TaskID, &j.JobID, &j.Revision, &j.Priority, &createdAt,
			&j.ExecutorID, &j.ExecutorVersion, &capsConcat, &payloadStr,
		); err != nil {
			return nil, fmt.Errorf("dispatchable.ListNextDispatchableJobs: scan: %w", err)
		}
		if t, perr := parseRFC3339(createdAt); perr == nil {
			j.CreatedAt = t
		}
		if capsConcat.Valid && capsConcat.String != "" {
			j.RequiredCapabilities = strings.Split(capsConcat.String, ",")
		}
		if payloadStr.Valid && payloadStr.String != "" {
			j.Payload = json.RawMessage(payloadStr.String)
			// Extract per-job placement pin from the task spec payload.
			var payloadMap map[string]interface{}
			if json.Unmarshal(j.Payload, &payloadMap) == nil {
				if pin, ok := payloadMap["_placement_pin_worker_id"].(string); ok && pin != "" {
					j.PlacementPinWorkerID = pin
				}
			}
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dispatchable.ListNextDispatchableJobs: rows: %w", err)
	}
	return out, nil
}

// parseRFC3339 accepts RFC3339Nano (preferred) and plain RFC3339
// (second precision). The tasks.created_at column is written in
// RFC3339-second precision so RFC3339 is the primary form; Nano
// is accepted defensively.
func parseRFC3339(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}
