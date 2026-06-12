package store

import (
	"encoding/json"
	"time"
)

func (s *SQLiteStore) ReplaceWorkers(rawWorkers map[string][]byte, revoked map[string]bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec("DELETE FROM workers"); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM worker_flags"); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for workerID, raw := range rawWorkers {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		sched := 0
		if b, ok := m["schedulable"].(bool); ok && b {
			sched = 1
		}
		drain := 0
		if b, ok := m["drain"].(bool); ok && b {
			drain = 1
		}
		if _, err := tx.Exec(
			`INSERT INTO workers (
				worker_id, worker_name, status, last_heartbeat, schedulable, drain, worker_group, raw_json, migrated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			workerID, asString(m["worker_name"]), asString(m["status"]), asString(m["last_heartbeat"]),
			sched, drain, asString(m["worker_group"]), string(raw), now,
		); err != nil {
			return err
		}
		rev := 0
		if revoked[workerID] {
			rev = 1
		}
		flagRaw, _ := json.Marshal(map[string]any{"worker_id": workerID, "revoked": rev == 1, "quarantined": false})
		if _, err := tx.Exec(
			`INSERT INTO worker_flags (worker_id, revoked, quarantined, raw_json, migrated_at)
			 VALUES (?, ?, 0, ?, ?)`,
			workerID, rev, string(flagRaw), now,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

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
			worker_id, worker_name, status, last_heartbeat, schedulable, drain, worker_group, raw_json, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET
			worker_name=excluded.worker_name,
			status=excluded.status,
			last_heartbeat=excluded.last_heartbeat,
			schedulable=excluded.schedulable,
			drain=excluded.drain,
			worker_group=excluded.worker_group,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		workerID, asString(m["worker_name"]), asString(m["status"]), asString(m["last_heartbeat"]),
		sched, drain, asString(m["worker_group"]), string(raw), now,
	)
	return err
}

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
