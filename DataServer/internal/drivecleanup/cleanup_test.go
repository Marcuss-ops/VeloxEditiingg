package drivecleanup

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/audittrail"
	"velox-server/internal/store"
)

type fakeHTTPError struct{ status int }

func (e fakeHTTPError) Error() string   { return "drive error" }
func (e fakeHTTPError) HTTPStatus() int { return e.status }

type fakeDrive struct {
	metadata  map[string]*FileMetadata
	targets   []string
	getErr    error
	deleteErr error
}

func (f *fakeDrive) GetFileMetadata(_ context.Context, id string) (*FileMetadata, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.metadata[id], nil
}
func (f *fakeDrive) DeleteFile(_ context.Context, id string) error {
	f.targets = append(f.targets, id)
	return f.deleteErr
}

type fakeAudit struct{ events []audittrail.Event }

type fakeVerifier struct{ err error }

func (f *fakeVerifier) VerifyDuplicateDeliveryRecord(_ context.Context, _ store.DuplicateDeliveryRecord) error {
	return f.err
}

func (f *fakeAudit) AppendAuditEventIdempotent(_ context.Context, event audittrail.Event) error {
	for _, existing := range f.events {
		if existing.ID == event.ID {
			return nil
		}
	}
	f.events = append(f.events, event)
	return nil
}

func testRecord() store.DuplicateDeliveryRecord {
	return store.DuplicateDeliveryRecord{JobID: "job-1", ArtifactID: "artifact-1", DeliveryID: "delivery-2", CanonicalDeliveryID: "delivery-1", DriveFileIDCorrect: "correct", DriveFileIDDuplicate: "duplicate", DestinationID: "folder-a", CreatedAt: "2026-08-04T10:00:00Z", DuplicateCreatedAt: "2026-08-04T10:01:00Z", DuplicateStatus: "SUCCEEDED"}
}

func TestApplyDryRunNeverDeletesAndAuditsPlanIdempotently(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{"correct": {ID: "correct"}, "duplicate": {ID: "duplicate"}}}
	audit := &fakeAudit{}
	manifest := Manifest{GeneratedAt: "2026-08-04T11:00:00Z", Records: []store.DuplicateDeliveryRecord{testRecord()}}
	verifier := &fakeVerifier{}
	got, err := Apply(context.Background(), drive, audit, verifier, manifest, true, "operator-1", time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got.Deleted != 0 || got.CanonicalChecked != 1 || len(drive.targets) != 0 {
		t.Fatalf("dry-run had remote effect: %#v %#v", got, drive.targets)
	}
	if len(audit.events) != 1 || audit.events[0].Action != "DRIVE_DUPLICATE_CLEANUP_PLANNED" {
		t.Fatalf("audit=%#v", audit.events)
	}
	_, err = Apply(context.Background(), drive, audit, verifier, manifest, true, "operator-1", time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.events) != 1 {
		t.Fatalf("retry created duplicate audit rows: %d", len(audit.events))
	}
	applyResult, err := Apply(context.Background(), drive, audit, verifier, manifest, false, "operator-1", time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if applyResult.Deleted != 1 || len(drive.targets) != 1 || drive.targets[0] != "duplicate" {
		t.Fatalf("apply result=%#v targets=%v", applyResult, drive.targets)
	}
	if len(audit.events) != 3 {
		t.Fatalf("dry-run/apply audit events=%d, want 3", len(audit.events))
	}
	if audit.events[0].ID == audit.events[1].ID || audit.events[1].ID == audit.events[2].ID {
		t.Fatalf("dry-run/apply audit IDs must differ: %#v", audit.events)
	}
	if !strings.Contains(audit.events[1].MetadataJSON, `"mode":"apply"`) || !strings.Contains(audit.events[2].MetadataJSON, `"manifest_generated_at":"2026-08-04T11:00:00Z"`) {
		t.Fatalf("mode/timestamp missing from apply audit: %#v", audit.events)
	}
}

func TestApplyRejectsStaleManifestBeforeRemoteVerification(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{"correct": {ID: "correct"}}}
	audit := &fakeAudit{}
	verifier := &fakeVerifier{err: errors.New("stale manifest")}
	_, err := Apply(context.Background(), drive, audit, verifier, Manifest{GeneratedAt: "2026-08-04T11:00:00Z", Records: []store.DuplicateDeliveryRecord{testRecord()}}, false, "operator", time.Now())
	if err == nil || !strings.Contains(err.Error(), "verify manifest record") {
		t.Fatalf("err=%v", err)
	}
	if len(drive.targets) != 0 {
		t.Fatalf("remote deletion occurred for stale manifest: %v", drive.targets)
	}
}

func TestApplyRejectsUnavailableDuplicateMetadata(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{"correct": {ID: "correct"}}}
	audit := &fakeAudit{}
	_, err := Apply(context.Background(), drive, audit, &fakeVerifier{}, Manifest{GeneratedAt: "2026-08-04T11:00:00Z", Records: []store.DuplicateDeliveryRecord{testRecord()}}, false, "operator", time.Now())
	if err == nil || !strings.Contains(err.Error(), "metadata unavailable") {
		t.Fatalf("err=%v", err)
	}
	if len(drive.targets) != 0 {
		t.Fatalf("deleted with unavailable duplicate metadata: %v", drive.targets)
	}
}

func TestApplyRequiresCanonicalBeforeDeletingDuplicate(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{}}
	audit := &fakeAudit{}
	_, err := Apply(context.Background(), drive, audit, &fakeVerifier{}, Manifest{GeneratedAt: "2026-08-04T11:00:00Z", Records: []store.DuplicateDeliveryRecord{testRecord()}}, false, "operator", time.Now())
	if err == nil || !strings.Contains(err.Error(), "verify canonical") {
		t.Fatalf("err=%v", err)
	}
	if len(drive.targets) != 0 {
		t.Fatalf("deleted without canonical proof: %v", drive.targets)
	}
}

func TestApplyDeletesOnlyDuplicateAndTreatsNotFoundAsIdempotent(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{"correct": {ID: "correct"}, "duplicate": {ID: "duplicate"}}, deleteErr: fakeHTTPError{status: 404}}
	audit := &fakeAudit{}
	got, err := Apply(context.Background(), drive, audit, &fakeVerifier{}, Manifest{GeneratedAt: "2026-08-04T11:00:00Z", Records: []store.DuplicateDeliveryRecord{testRecord()}}, false, "operator", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got.Deleted != 0 || got.AlreadyAbsent != 1 || len(drive.targets) != 1 || drive.targets[0] != "duplicate" {
		t.Fatalf("result=%#v targets=%v", got, drive.targets)
	}
	if len(audit.events) != 2 || audit.events[0].Action != "DRIVE_DUPLICATE_CLEANUP_PLANNED" || audit.events[1].Action != "DRIVE_DUPLICATE_DELETED" {
		t.Fatalf("audit=%#v", audit.events)
	}
}

func TestParseManifestRejectsDuplicateRemoteIDAndMalformedTimestamp(t *testing.T) {
	base := `"job_id":"j","artifact_id":"a","destination_id":"x","canonical_delivery_id":"c","created_at":"2026-08-04T10:00:00Z","duplicate_created_at":"2026-08-04T10:01:00Z","duplicate_status":"SUCCEEDED"`
	duplicate := `{"generated_at":"2026-08-04T11:00:00Z","records":[{"drive_file_id_correct":"c1","drive_file_id_duplicate":"d1",` + base + `,"delivery_id":"d1"},{"drive_file_id_correct":"c1","drive_file_id_duplicate":"d1",` + base + `,"delivery_id":"d2"}]}`
	if _, err := ParseManifest([]byte(duplicate)); err == nil {
		t.Fatal("duplicate remote ID unexpectedly accepted")
	}
	malformed := `{"generated_at":"2026-08-04T11:00:00Z","records":[{"drive_file_id_correct":"c1","drive_file_id_duplicate":"d1","job_id":"j","canonical_delivery_id":"c","delivery_id":"d1","artifact_id":"a","destination_id":"x","created_at":"not-a-time","duplicate_created_at":"2026-08-04T10:01:00Z","duplicate_status":"SUCCEEDED"}]}`
	if _, err := ParseManifest([]byte(malformed)); err == nil {
		t.Fatal("malformed timestamp unexpectedly accepted")
	}
}

func TestParseManifestRejectsSameOrMissingIDs(t *testing.T) {
	for _, raw := range []string{`{"generated_at":"2026-08-04T11:00:00Z","records":[{"drive_file_id_correct":"same","drive_file_id_duplicate":"same","job_id":"j","canonical_delivery_id":"c","delivery_id":"d","artifact_id":"a","destination_id":"x","created_at":"t1","duplicate_created_at":"t2"}]}`, `{"generated_at":"2026-08-04T11:00:00Z","records":[{"drive_file_id_correct":"","drive_file_id_duplicate":"d","job_id":"j","canonical_delivery_id":"c","delivery_id":"d","artifact_id":"a","destination_id":"x","created_at":"t1","duplicate_created_at":"t2"}]}`, `{"generated_at":"2026-08-04T11:00:00Z","records":[{"drive_file_id_correct":"c","drive_file_id_duplicate":"d","job_id":"j","canonical_delivery_id":"c1","delivery_id":"d1","artifact_id":"a","destination_id":"x","created_at":"t1","duplicate_created_at":"t2"}]}`} {
		if _, err := ParseManifest([]byte(raw)); err == nil {
			t.Fatalf("expected invalid manifest rejection: %s", raw)
		}
	}
}
