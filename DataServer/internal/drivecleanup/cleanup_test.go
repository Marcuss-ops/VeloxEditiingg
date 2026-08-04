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
	return store.DuplicateDeliveryRecord{JobID: "job-1", ArtifactID: "artifact-1", DeliveryID: "delivery-2", CanonicalDeliveryID: "delivery-1", DriveFileIDCorrect: "correct", DriveFileIDDuplicate: "duplicate", DestinationID: "folder-a", CreatedAt: "2026-08-04T10:00:00Z", DuplicateCreatedAt: "2026-08-04T10:01:00Z"}
}

func TestApplyDryRunNeverContactsDriveAndAuditsPlanIdempotently(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{"correct": {ID: "correct"}}}
	audit := &fakeAudit{}
	manifest := Manifest{Records: []store.DuplicateDeliveryRecord{testRecord()}}
	got, err := Apply(context.Background(), drive, audit, manifest, true, "operator-1", time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got.Deleted != 0 || got.CanonicalChecked != 1 || len(drive.targets) != 0 {
		t.Fatalf("dry-run had remote effect: %#v %#v", got, drive.targets)
	}
	if len(audit.events) != 1 || audit.events[0].Action != "DRIVE_DUPLICATE_CLEANUP_PLANNED" {
		t.Fatalf("audit=%#v", audit.events)
	}
	_, err = Apply(context.Background(), drive, audit, manifest, true, "operator-1", time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.events) != 1 {
		t.Fatalf("retry created duplicate audit rows: %d", len(audit.events))
	}
}

func TestApplyRequiresCanonicalBeforeDeletingDuplicate(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{}}
	audit := &fakeAudit{}
	_, err := Apply(context.Background(), drive, audit, Manifest{Records: []store.DuplicateDeliveryRecord{testRecord()}}, false, "operator", time.Now())
	if err == nil || !strings.Contains(err.Error(), "verify canonical") {
		t.Fatalf("err=%v", err)
	}
	if len(drive.targets) != 0 {
		t.Fatalf("deleted without canonical proof: %v", drive.targets)
	}
}

func TestApplyDeletesOnlyDuplicateAndTreatsNotFoundAsIdempotent(t *testing.T) {
	drive := &fakeDrive{metadata: map[string]*FileMetadata{"correct": {ID: "correct"}}, deleteErr: errors.New("API error (404): not found")}
	audit := &fakeAudit{}
	got, err := Apply(context.Background(), drive, audit, Manifest{Records: []store.DuplicateDeliveryRecord{testRecord()}}, false, "operator", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got.Deleted != 0 || got.AlreadyAbsent != 1 || len(drive.targets) != 1 || drive.targets[0] != "duplicate" {
		t.Fatalf("result=%#v targets=%v", got, drive.targets)
	}
	if len(audit.events) != 1 || audit.events[0].Action != "DRIVE_DUPLICATE_DELETED" {
		t.Fatalf("audit=%#v", audit.events)
	}
}

func TestParseManifestRejectsSameOrMissingIDs(t *testing.T) {
	for _, raw := range []string{`{"records":[{"drive_file_id_correct":"same","drive_file_id_duplicate":"same","delivery_id":"d","artifact_id":"a","destination_id":"x"}]}`, `{"records":[{"drive_file_id_correct":"","drive_file_id_duplicate":"d","delivery_id":"d","artifact_id":"a","destination_id":"x"}]}`} {
		if _, err := ParseManifest([]byte(raw)); err == nil {
			t.Fatalf("expected invalid manifest rejection: %s", raw)
		}
	}
}
