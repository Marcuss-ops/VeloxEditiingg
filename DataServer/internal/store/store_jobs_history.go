package store

import (
	"database/sql"
	"encoding/json"
)

func (s *SQLiteStore) replaceJobHistoryTx(tx *sql.Tx, jobID string, history []map[string]any) error {
	if _, err := tx.Exec(`DELETE FROM job_history WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	for _, hm := range history {
		hraw, _ := json.Marshal(hm)
		if _, err := tx.Exec(
			`INSERT INTO job_history (job_id, status, event_ts, worker_id, message, raw_json)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			jobID, asString(hm["status"]), toISO(hm["timestamp"]), asString(hm["worker_id"]), asString(hm["message"]), string(hraw),
		); err != nil {
			return err
		}
	}
	return nil
}

}
