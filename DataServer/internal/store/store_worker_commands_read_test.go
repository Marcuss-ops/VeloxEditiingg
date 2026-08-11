package store

import "testing"

func TestGetPendingCommandsFailsClosedOnCorruptPayload(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-command-read.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.InsertCommand(&PersistedCommand{
		CommandID: "command-corrupt", WorkerID: "worker-command", CommandType: "restart",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE worker_commands SET payload_json='{not-json' WHERE command_id='command-corrupt'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPendingCommands("worker-command"); err == nil {
		t.Fatal("GetPendingCommands returned success for corrupt payload_json")
	}
}

func TestGetPendingCommandsFailsClosedOnCorruptTimestamp(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-command-timestamp.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.InsertCommand(&PersistedCommand{
		CommandID: "command-timestamp", WorkerID: "worker-command", CommandType: "restart",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE worker_commands SET created_at='not-a-timestamp' WHERE command_id='command-timestamp'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPendingCommands("worker-command"); err == nil {
		t.Fatal("GetPendingCommands returned success for corrupt created_at")
	}
}
