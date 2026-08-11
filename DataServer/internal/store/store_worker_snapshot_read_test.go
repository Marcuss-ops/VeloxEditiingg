package store

import (
	"testing"
)

func TestListWorkersFailsClosedOnCorruptSnapshot(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-snapshot-read.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.DB().Exec(`INSERT INTO workers (worker_id, worker_name, raw_json, migrated_at)
		VALUES ('corrupt-worker', 'corrupt-worker', '{not-json', datetime('now'))`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListWorkers(); err == nil {
		t.Fatal("ListWorkers returned success for corrupt raw_json")
	}

	if _, err := s.DB().Exec(`UPDATE workers SET workspace_id=7 WHERE worker_id='corrupt-worker'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListWorkersByWorkspace(7); err == nil {
		t.Fatal("ListWorkersByWorkspace returned success for corrupt raw_json")
	}
}
