package deliveryrelay_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/store/deliveryrelay"

	_ "github.com/mattn/go-sqlite3"
)

func newTestStore(t *testing.T) *deliveryrelay.SessionStore {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "relay.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE drive_relay_sessions (
		upload_id TEXT NOT NULL,
		artifact_id TEXT NOT NULL,
		job_id TEXT NOT NULL DEFAULT '',
		destination_id TEXT NOT NULL,
		publication_id TEXT NOT NULL,
		folder_id TEXT NOT NULL DEFAULT '',
		session_uri TEXT NOT NULL DEFAULT '',
		expires_at TEXT NOT NULL DEFAULT '',
		part_size INTEGER NOT NULL DEFAULT 0,
		next_offset INTEGER NOT NULL DEFAULT 0,
		in_flight INTEGER NOT NULL DEFAULT 0,
		state TEXT NOT NULL DEFAULT 'PREPARED',
		remote_id TEXT,
		remote_url TEXT,
		verified_sha256 TEXT,
		verified_size INTEGER,
		last_error TEXT,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (upload_id, destination_id, publication_id)
	)`)
	if err != nil {
		t.Fatalf("create fixture table: %v", err)
	}
	// SaveSession backfills job_id from artifact_uploads (self-contained row
	// invariant); the fixture mirrors that parent table minimally, including
	// the seeded parent row the production flow always has when a relay
	// session is prepared.
	_, err = db.Exec(`CREATE TABLE artifact_uploads (
		upload_id TEXT PRIMARY KEY,
		job_id TEXT NOT NULL DEFAULT ''
	)`)
	if err != nil {
		t.Fatalf("create artifact_uploads fixture: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_uploads (upload_id, job_id) VALUES ('up1', 'job1')`); err != nil {
		t.Fatalf("seed artifact_uploads: %v", err)
	}
	return deliveryrelay.New(db)
}

func TestSaveAndListSessionsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	err := s.SaveSession(ctx, deliveryrelay.Session{
		UploadID: "up1", ArtifactID: "art1", DestinationID: "dest1",
		PublicationID: "pub1", FolderID: "folder1", SessionURI: "https://drive.example/resumable",
		PartSize: 8 * 1024 * 1024, State: "PREPARED",
	})
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	got, err := s.GetSession(ctx, "up1", "dest1", "pub1")
	if err != nil || got == nil {
		t.Fatalf("GetSession = (%v, %v), want session", got, err)
	}
	if got.SessionURI != "https://drive.example/resumable" || got.State != "PREPARED" || got.InFlight {
		t.Fatalf("GetSession round-trip mismatch: %+v", got)
	}
	if err := s.MarkInFlight(ctx, *got); err != nil {
		t.Fatalf("MarkInFlight: %v", err)
	}
	// The CAS must fail closed for a second claim at the same offset.
	if err := s.MarkInFlight(ctx, *got); err == nil {
		t.Fatal("second MarkInFlight at the same offset succeeded")
	}
	if err := s.SaveProgress(ctx, *got, 8*1024*1024, false, "UPLOADED_UNVERIFIED", "file-123", "https://drive.example/view", ""); err != nil {
		t.Fatalf("SaveProgress: %v", err)
	}
	updated, err := s.MarkVerified(ctx, "up1", "abc123", 8*1024*1024)
	if err != nil || updated != 1 {
		t.Fatalf("MarkVerified = (%d, %v), want 1", updated, err)
	}
	// VERIFIED sessions disappear from the active list.
	active, err := s.ListSessions(ctx, "up1")
	if err != nil || len(active) != 0 {
		t.Fatalf("ListSessions after verify = (%d sessions, %v), want 0", len(active), err)
	}
	ev, err := s.GetVerifiedEvidence(ctx, "art1", "dest1", "pub1")
	if err != nil || ev == nil {
		t.Fatalf("GetVerifiedEvidence = (%v, %v), want evidence", ev, err)
	}
	if ev.RemoteID != "file-123" || ev.SHA256 != "abc123" || ev.SizeBytes != 8*1024*1024 {
		t.Fatalf("GetVerifiedEvidence mismatch: %+v", ev)
	}
}

func TestSaveSessionResetsPriorProgress(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	base := deliveryrelay.Session{
		UploadID: "up1", ArtifactID: "art1", DestinationID: "dest1", PublicationID: "pub1",
		FolderID: "f", SessionURI: "uri-1", PartSize: 1024, State: "PREPARED",
	}
	if err := s.SaveSession(ctx, base); err != nil {
		t.Fatalf("first SaveSession: %v", err)
	}
	if err := s.SaveProgress(ctx, base, 512, false, "UPLOADING", "remote-1", "url-1", ""); err != nil {
		t.Fatalf("SaveProgress: %v", err)
	}
	// Re-preparing the same key must reset offset/state/remote identity.
	reprepped := base
	reprepped.SessionURI = "uri-2"
	if err := s.SaveSession(ctx, reprepped); err != nil {
		t.Fatalf("second SaveSession: %v", err)
	}
	got, err := s.GetSession(ctx, "up1", "dest1", "pub1")
	if err != nil || got == nil {
		t.Fatalf("GetSession = (%v, %v)", got, err)
	}
	if got.NextOffset != 0 || got.State != "PREPARED" || got.RemoteID != "" || got.SessionURI != "uri-2" {
		t.Fatalf("SaveSession did not reset prior progress: %+v", got)
	}
}

func TestFailSessionKeepsTerminalStates(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	base := deliveryrelay.Session{
		UploadID: "up1", ArtifactID: "art1", DestinationID: "d", PublicationID: "p",
		SessionURI: "u", PartSize: 256, State: "PREPARED",
	}
	if err := s.SaveSession(ctx, base); err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	sess, _ := s.GetSession(ctx, "up1", "d", "p")
	if err := s.FailSession(ctx, *sess, context.DeadlineExceeded); err != nil {
		t.Fatalf("FailSession: %v", err)
	}
	got, _ := s.GetSession(ctx, "up1", "d", "p")
	if got.State != "FAILED" {
		t.Fatalf("FailSession did not apply: %+v", got)
	}

	// UPLOADED_UNVERIFIED is terminal for FailSession purposes: reach it via
	// the public progress API, then prove FailSession leaves it alone.
	if err := s.SaveProgress(ctx, *got, 256, false, "UPLOADED_UNVERIFIED", "remote-1", "url-1", ""); err != nil {
		t.Fatalf("SaveProgress to UPLOADED_UNVERIFIED: %v", err)
	}
	if err := s.FailSession(ctx, *got, context.Canceled); err != nil {
		t.Fatalf("FailSession on UPLOADED_UNVERIFIED: %v", err)
	}
	got, _ = s.GetSession(ctx, "up1", "d", "p")
	if got.State != "UPLOADED_UNVERIFIED" {
		t.Fatalf("FailSession regressed a terminal state: %+v", got)
	}

	// VERIFIED is terminal too, and MarkVerified must not resurrect it.
	if _, err := s.MarkVerified(ctx, "up1", "sha-ok", 256); err != nil {
		t.Fatalf("MarkVerified on UPLOADED_UNVERIFIED session: %v", err)
	}
	if err := s.FailSession(ctx, *got, context.Canceled); err != nil {
		t.Fatalf("FailSession on VERIFIED: %v", err)
	}
	got, _ = s.GetSession(ctx, "up1", "d", "p")
	if got.State != "VERIFIED" {
		t.Fatalf("FailSession regressed VERIFIED: %+v", got)
	}
}

func TestSessionExpiryAndAbort(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	base := deliveryrelay.Session{
		UploadID: "up1", ArtifactID: "art1", DestinationID: "d", PublicationID: "p",
		SessionURI: "u", PartSize: 256, State: "PREPARED",
	}
	if _, ok, err := s.SessionExpiresAt(ctx, "up1", "d", "p"); ok || err != nil {
		t.Fatalf("SessionExpiresAt on missing row = (%v, %v), want (false, nil)", ok, err)
	}
	if err := s.SaveSession(ctx, base); err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	until, ok, err := s.SessionExpiresAt(ctx, "up1", "d", "p")
	if err != nil || !ok {
		t.Fatalf("SessionExpiresAt = (%v, %v, %v), want expiry", until, ok, err)
	}
	if time.Until(until) < 6*24*time.Hour {
		t.Fatalf("session lifetime too short: %v", until)
	}
	if err := s.AbortSessions(ctx, "up1"); err != nil {
		t.Fatalf("AbortSessions: %v", err)
	}
	got, _ := s.GetSession(ctx, "up1", "d", "p")
	if got.State != "FAILED" || got.SessionURI != "" {
		t.Fatalf("AbortSessions did not clear the session: %+v", got)
	}
}
