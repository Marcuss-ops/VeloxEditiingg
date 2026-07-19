package store

import (
	"database/sql"
	"encoding/json"
)

// worker_snapshot_mapping.go owns the small set of helpers that translate
// between the worker's raw payload (a map[string]any) and the SQLite
// upsert column list. They are colocated so any change to the storage
// schema only needs a single paired edit (counterpart in migration +
// helper in this file + caller in store_worker_snapshot.go).

// workerSQLExec abstracts the SQL exec surface so upsertWorkerExec can
// run either directly on s.db or inside a caller-provided transaction.
// The interface is intentionally minimal: Exec is the only method used
// by the snapshot upsert path.
type workerSQLExec interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// workerActiveTaskCount derives the active-task count from a worker
// payload map. Multiple keys are tried in turn (counter first, then
// metric field counters, then list-length fallback) so the snapshot
// stays correct across heartbeat-format generations without changing
// the upsert column list itself.
func workerActiveTaskCount(m map[string]any, metric func(string) any) int64 {
	if n := int64Value(m["active_task_count"]); n != 0 {
		return n
	}
	if n := int64Value(metric("active_task_count")); n != 0 {
		return n
	}
	if n := int64Value(metric("active_jobs_count")); n != 0 {
		return n
	}
	if n := int64Value(metric("active_tasks")); n != 0 {
		return n
	}
	metrics, _ := m["metrics"].(map[string]any)
	if items, ok := metrics["active_jobs"].([]any); ok {
		return int64(len(items))
	}
	return 0
}

// jsonString serialises a value to its JSON string form for storage in
// TEXT columns. nil values are stored as the empty string; marshal errors
// are also emitted as the empty string so the snapshot upsert never
// fails on a single bad nested value.
func jsonString(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
