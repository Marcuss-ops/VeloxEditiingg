package deliveries

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/store"
)

type opaqueDestinationCaptureProvider struct {
	destinationID         string
	externalDestinationID string
}

func (p *opaqueDestinationCaptureProvider) Name() string { return "social_gateway" }

func (p *opaqueDestinationCaptureProvider) Deliver(_ context.Context, _ *store.Artifact, destination *Destination, _, _ string) (*Result, error) {
	p.destinationID = destination.DestinationID
	p.externalDestinationID = destination.ExternalDestinationID
	return &Result{Success: true, RemoteID: "social-delivery-opaque-1"}, nil
}

func TestDeliveryRunnerHydratesOpaqueDestinationBeforeProviderDispatch(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "opaque-runner.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	const (
		localDestinationID    = "instaedit_ext-group-a"
		externalDestinationID = "ext-group-a"
		artifactID            = "artifact-opaque-runner"
		deliveryID            = "delivery-opaque-runner"
	)
	if err := db.Delivery().InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:         localDestinationID,
		Provider:              "social_gateway",
		ExternalDestinationID: externalDestinationID,
		Enabled:               true,
		ConfigurationJSON:     `{"workspace_id":42,"platform":"youtube","platform_account_id":101}`,
	}); err != nil {
		t.Fatalf("InsertDeliveryDestination: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           "job-opaque-runner",
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "opaque.mp4"),
		SHA256:          "opaque-sha256",
		SizeBytes:       1,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("InsertArtifact: %v", err)
	}
	if err := db.Delivery().InsertJobDelivery(&store.JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  localDestinationID,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    3,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("InsertJobDelivery: %v", err)
	}

	capture := &opaqueDestinationCaptureProvider{}
	registry := NewRegistry()
	registry.Register(capture)
	runner := NewDeliveryRunner(DefaultRunnerConfig(), registry, db.Delivery(), db, "opaque-runner")
	leases, err := db.Delivery().ClaimDeliveries(context.Background(), "opaque-runner", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %d, want 1", len(leases))
	}
	if err := runner.processLease(context.Background(), leases[0]); err != nil {
		t.Fatalf("processLease: %v", err)
	}

	if capture.destinationID != localDestinationID {
		t.Fatalf("provider destination_id = %q, want local opaque mapping %q", capture.destinationID, localDestinationID)
	}
	if capture.externalDestinationID != externalDestinationID {
		t.Fatalf("provider external_destination_id = %q, want opaque InstaEdit ID %q", capture.externalDestinationID, externalDestinationID)
	}
	row, err := db.Delivery().GetJobDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if row.Status != "SUCCEEDED" || row.RemoteID != "social-delivery-opaque-1" {
		t.Fatalf("delivery result = status=%q remote_id=%q, want SUCCEEDED/social-delivery-opaque-1", row.Status, row.RemoteID)
	}
}
