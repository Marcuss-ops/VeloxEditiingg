package store

import (
	"encoding/json"
	"time"
)

// UpsertWorker creates or updates a worker record.
// Uses UPSERT (ON CONFLICT DO UPDATE) for idempotent writes.
func (s *SQLiteStore) UpsertWorker(raw []byte) error {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	workerID := asString(m["worker_id"])
	if workerID == "" {
		return nil
	}

	sched := 0
	if b, ok := m["schedulable"].(bool); ok && b {
		sched = 1
	}
	drain := 0
	if b, ok := m["drain"].(bool); ok && b {
		drain = 1
	}
	now := time.Now().UTC().Format(time.RFC3339)

	_, err := s.db.Exec(
		`INSERT INTO workers (
			worker_id, worker_name, status, last_heartbeat,
			schedulable, drain, worker_group,
			display_name, ip_address, first_seen, current_job,
			code_version, bundle_version, bundle_hash,
			protocol_version, engine_version,
			recent_logs, recent_errors, readiness, metrics, capabilities,
			raw_json, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

// SetWorkerRevoked sets the revoked flag for a worker in worker_flags.
func (s *SQLiteStore) SetWorkerRevoked(workerID string, revoked bool) error {
	revInt := 0
	if revoked {
		revInt = 1
	}
	now := time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(map[string]any{
		"worker_id": workerID,
		"revoked":   revoked,
		"updated_at": now,
	})
	_, err := s.db.Exec(
		`INSERT INTO worker_flags (worker_id, revoked, quarantined, raw_json, migrated_at)
		 VALUES (?, ?, 0, ?, ?)
		 ON CONFLICT(worker_id) DO UPDATE SET
			revoked=excluded.revoked,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		workerID, revInt, string(raw), now,
	)
	return err
}

// GetRevokedWorkers returns the list of all revoked worker IDs.
func (s *SQLiteStore) GetRevokedWorkers() ([]string, error) {
	rows, err := s.db.Query(`SELECT worker_id FROM worker_flags WHERE revoked = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ReplaceWorkers has been removed. Use individual UpsertWorker + SetWorkerRevoked instead.
// This was a bulk DELETE+re-insert approach that caused unnecessary write amplification.

// jsonString serializes a value to JSON string, or returns "{}"/"[]" for nil.
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
