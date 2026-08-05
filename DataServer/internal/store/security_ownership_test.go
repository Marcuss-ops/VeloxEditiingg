package store

import (
	"testing"
	"time"
)

func TestSecurity_AckCommandRejectsOwnershipMismatch(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/ownership.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	cmd := &PersistedCommand{
		CommandID:      "security-command-1",
		WorkerID:       "worker-owner",
		CommandType:    "drain",
		Status:         "pending",
		CreatedAt:      time.Now().UTC(),
		ExpiresAt:      ptrTime(time.Now().UTC().Add(time.Hour)),
		IdempotencyKey: "security-command-1",
	}
	if _, err := s.InsertCommand(cmd); err != nil {
		t.Fatalf("InsertCommand: %v", err)
	}

	if err := s.AckCommandByID("worker-attacker", cmd.CommandID); err == nil {
		t.Fatal("ownership-mismatched worker acknowledged another worker's command")
	}

	var status string
	if err := s.db.QueryRow(`SELECT status FROM worker_commands WHERE command_id = ?`, cmd.CommandID).Scan(&status); err != nil {
		t.Fatalf("read command status: %v", err)
	}
	if status != "pending" {
		t.Fatalf("command status=%q after ownership mismatch, want pending", status)
	}

	if err := s.AckCommandByID("worker-owner", cmd.CommandID); err != nil {
		t.Fatalf("owner AckCommandByID: %v", err)
	}
}

func TestSecurity_RevokedSessionCannotValidate(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/revoked-session.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	const tokenHash = "security-token-hash"
	if err := s.EnsureWorkerRecord("worker-revoked"); err != nil {
		t.Fatalf("EnsureWorkerRecord: %v", err)
	}
	if err := s.InsertSession(&PersistedSession{
		SessionID:   "security-session-1",
		WorkerID:    "worker-revoked",
		SessionType: "asset",
		TokenHash:   tokenHash,
		ExpiresAt:   time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}
	if got, err := s.ValidateSession(tokenHash); err != nil || got == nil {
		t.Fatalf("ValidateSession before revoke: got=%v err=%v", got, err)
	}

	if err := s.RevokeWorkerSessions("worker-revoked"); err != nil {
		t.Fatalf("RevokeWorkerSessions: %v", err)
	}
	got, err := s.ValidateSession(tokenHash)
	if err != nil {
		t.Fatalf("ValidateSession after revoke: %v", err)
	}
	if got != nil {
		t.Fatalf("revoked session validated successfully: %+v", got)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
