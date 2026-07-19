package store

import (
	"encoding/json"
	"time"
)

// store_worker_flags.go owns the worker_flags row: the revoked bit and
// the locked three-key audit blob ({worker_id, revoked, updated_at}).
// The blob shape is enforced at runtime by
// TestSetWorkerRevoked_RawJsonShapeContract in store_workers_test.go.

// SetWorkerRevoked sets the revoked flag for a worker in worker_flags.
//
// SECURITY / SHAPE CONTRACT — read before "harmonizing" with workers.raw_json:
// The `raw_json` blob here is INTENTIONALLY a separate three-key audit
// shape ({worker_id, revoked, updated_at}), NOT a WorkerInfo copy. WorkerInfo
// carries read-time-hydrated fields (SessionActive, ConnectionStatus) that
// must NEVER be persisted (see workers.ScrubForPersist) — adding them to
// this blob would reintroduce the persistence-leak class fixed by that
// helper, but without a matching read-time hydrator on this side (there is
// none, and none should exist). The shape is locked by
// TestSetWorkerRevoked_RawJsonShapeContract below. If a future change needs
// structured flag metadata beyond the three-key blob, add explicit columns
// to worker_flags — keep raw_json as the audit map it is today.
func (s *SQLiteStore) SetWorkerRevoked(workerID string, revoked bool) error {
	revInt := 0
	if revoked {
		revInt = 1
	}
	now := time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(map[string]any{
		"worker_id":  workerID,
		"revoked":    revoked,
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
