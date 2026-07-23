package store

import (
	"encoding/json"
	"fmt"
	"time"
)

// store_worker_snapshot.go owns the worker_snapshot row lifecycle:
// bootstrap registration, full upsert, single-row reads, delete, and
// the list-all query. The helpers used by the upsert itself
// (workerSQLExec / workerActiveTaskCount / jsonString) live in
// worker_snapshot_mapping.go, deliberately not in this file, so the
// UPSERT SQL stays the single source of truth for snapshot shape.

// UpsertWorker creates or updates a worker record.
// Uses UPSERT (ON CONFLICT DO UPDATE) for idempotent writes.
func (s *SQLiteStore) UpsertWorker(raw []byte) error {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	workerID := asString(m["worker_id"])
	if workerID == "" {
		return fmt.Errorf("upsert worker: missing worker_id")
	}

	now := time.Now().UTC().Format(time.RFC3339)

	return s.upsertWorkerExec(s.db, m, raw, now)
}

// EnsureWorkerRecord creates the minimum worker snapshot required before a
// session can be persisted. Registration may issue the session token before
// the first heartbeat arrives; keeping this small bootstrap row here preserves
// the worker_sessions foreign-key/trigger invariant without inventing runtime
// state.
func (s *SQLiteStore) EnsureWorkerRecord(workerID string) error {
	if workerID == "" {
		return fmt.Errorf("ensure worker: missing worker_id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`INSERT INTO workers
		(worker_id,worker_name,status,schedulable,drain,raw_json,migrated_at,node_id,node_role)
		VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(worker_id) DO NOTHING`,
		workerID, workerID, "REGISTERING", 0, 0, `{}`, now, workerID, "worker")
	return err
}

func (s *SQLiteStore) upsertWorkerExec(exec workerSQLExec, m map[string]any, raw []byte, now string) error {
	workerID := asString(m["worker_id"])
	sched := boolInt(m["schedulable"])
	drain := boolInt(m["drain"])
	metrics, _ := m["metrics"].(map[string]any)
	metric := func(key string) any { return metrics[key] }
	_, err := exec.Exec(
		`INSERT INTO workers (
			worker_id, worker_name, status, last_heartbeat,
			schedulable, drain, worker_group,
			display_name, ip_address, first_seen, current_job,
			code_version, bundle_version, bundle_hash,
			protocol_version, engine_version,
			node_id, node_role, cluster_id, host_fingerprint, certificate_fingerprint,
			connection_status, connection_reason, session_active, current_task_id,
			active_task_count, task_slots, cpu_utilization_ratio, memory_used_bytes,
			disk_free_bytes, jobs_completed, jobs_failed, connected_at, last_heartbeat_at, updated_at,
			recent_logs, recent_errors, readiness, metrics, capabilities,
			raw_json, migrated_at
		) VALUES (
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET
			worker_name=excluded.worker_name,
			status=excluded.status,
			last_heartbeat=excluded.last_heartbeat,
			schedulable=excluded.schedulable,
			drain=excluded.drain,
			worker_group=excluded.worker_group,
			display_name=excluded.display_name,
			ip_address=excluded.ip_address,
			first_seen=COALESCE(NULLIF(excluded.first_seen, ''), workers.first_seen),
			current_job=excluded.current_job,
			code_version=excluded.code_version,
			bundle_version=excluded.bundle_version,
			bundle_hash=excluded.bundle_hash,
			protocol_version=excluded.protocol_version,
			engine_version=excluded.engine_version,
			node_id=excluded.node_id, node_role=excluded.node_role, cluster_id=excluded.cluster_id,
			host_fingerprint=excluded.host_fingerprint, certificate_fingerprint=excluded.certificate_fingerprint,
			connection_status=excluded.connection_status, connection_reason=excluded.connection_reason,
			session_active=excluded.session_active, current_task_id=excluded.current_task_id,
			active_task_count=excluded.active_task_count, task_slots=excluded.task_slots,
			cpu_utilization_ratio=excluded.cpu_utilization_ratio, memory_used_bytes=excluded.memory_used_bytes,
			disk_free_bytes=excluded.disk_free_bytes, jobs_completed=excluded.jobs_completed,
			jobs_failed=excluded.jobs_failed, connected_at=COALESCE(NULLIF(excluded.connected_at, ''), workers.connected_at),
			last_heartbeat_at=excluded.last_heartbeat_at, updated_at=excluded.updated_at,
			recent_logs=excluded.recent_logs,
			recent_errors=excluded.recent_errors,
			readiness=excluded.readiness,
			metrics=excluded.metrics,
			capabilities=excluded.capabilities,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		workerID,
		asString(m["worker_name"]), asString(m["status"]), asString(m["last_heartbeat"]),
		sched, drain, asString(m["worker_group"]),
		asString(m["display_name"]), asString(m["ip_address"]),
		asString(m["first_seen"]), asString(m["current_job"]),
		asString(m["code_version"]), asString(m["bundle_version"]),
		asString(m["bundle_hash"]), asString(m["protocol_version"]),
		asString(m["engine_version"]),
		asString(m["node_id"]), defaultString(m["node_role"], "worker"), asString(m["cluster_id"]),
		asString(m["host_fingerprint"]), asString(m["certificate_fingerprint"]),
		asString(m["connection_status"]), asString(m["connection_reason"]), boolInt(m["session_active"]),
		defaultString(m["current_task_id"], asString(m["current_job"])), workerActiveTaskCount(m, metric), int64OrDefault(m["task_slots"], int64OrDefault(metric("task_slots"), 1)),
		floatOrMetric(m["cpu_utilization_ratio"], metric("cpu_utilization_ratio")), int64OrDefault(m["memory_used_bytes"], int64Value(metric("memory_used_bytes"))), int64OrDefault(m["disk_free_bytes"], int64Value(metric("disk_free_bytes"))),
		int64OrDefault(m["jobs_completed"], int64Value(metric("jobs_completed"))), int64OrDefault(m["jobs_failed"], int64Value(metric("jobs_failed"))), asString(m["connected_at"]),
		defaultString(m["last_heartbeat_at"], asString(m["last_heartbeat"])), now,
		jsonString(m["recent_logs"]), jsonString(m["recent_errors"]),
		jsonString(m["readiness"]), jsonString(m["metrics"]), jsonString(m["capabilities"]),
		string(raw), now,
	)
	return err
}

// GetWorker returns a single worker as a map by ID.
func (s *SQLiteStore) GetWorker(workerID string) (map[string]any, error) {
	var raw string
	err := s.db.QueryRow(`SELECT raw_json FROM workers WHERE worker_id = ?`, workerID).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// GetWorkerByWorkspace returns a worker only if it belongs to the given
// workspace. Rows with NULL workspace_id are not returned.
func (s *SQLiteStore) GetWorkerByWorkspace(workerID string, workspaceID int64) (map[string]any, error) {
	var raw string
	err := s.db.QueryRow(`SELECT raw_json FROM workers WHERE worker_id = ? AND workspace_id = ?`, workerID, workspaceID).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// DeleteWorker removes a worker record.
func (s *SQLiteStore) DeleteWorker(workerID string) error {
	_, err := s.db.Exec(`DELETE FROM workers WHERE worker_id = ?`, workerID)
	return err
}

// ListWorkers returns all workers as raw JSON maps, ordered by last heartbeat descending.
func (s *SQLiteStore) ListWorkers() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT raw_json FROM workers ORDER BY last_heartbeat DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

// ListWorkersByWorkspace returns workers scoped to an InstaEdit
// workspace. Only rows with an explicit workspace_id are returned.
func (s *SQLiteStore) ListWorkersByWorkspace(workspaceID int64) ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT raw_json FROM workers WHERE workspace_id = ? ORDER BY last_heartbeat DESC`, workspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, rows.Err()
}

// ReplaceWorkers has been removed. Use individual UpsertWorker + SetWorkerRevoked instead.
// This was a bulk DELETE+re-insert approach that caused unnecessary write amplification.
