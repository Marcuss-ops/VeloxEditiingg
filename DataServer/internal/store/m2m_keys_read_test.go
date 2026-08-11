package store

import (
	"context"
	"testing"
)

func TestGetM2MAPIKeyFailsClosedOnCorruptTimestamp(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/m2m-key-read.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.DB().Exec(`INSERT INTO m2m_api_keys
		(client_id, secret_hash, scopes, created_at, updated_at)
		VALUES ('client-corrupt', 'hash', 'jobs.submit', 'not-a-time', datetime('now'))`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetM2MAPIKeyByClientID(context.Background(), "client-corrupt"); err == nil {
		t.Fatal("GetM2MAPIKeyByClientID returned success for corrupt created_at")
	}
}

func TestListM2MAuditLogFailsClosedOnCorruptTimestamp(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/m2m-audit-read.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.DB().Exec(`INSERT INTO m2m_api_keys
		(client_id, secret_hash, scopes) VALUES ('client-audit', 'hash', 'jobs.submit')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO m2m_audit_log
		(client_id, status_code, created_at) VALUES ('client-audit', 200, 'not-a-time')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListM2MAuditLog(context.Background(), "client-audit", 10); err == nil {
		t.Fatal("ListM2MAuditLog returned success for corrupt created_at")
	}
}
