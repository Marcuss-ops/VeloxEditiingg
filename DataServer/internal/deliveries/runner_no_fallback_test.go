package deliveries

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
	"velox-server/internal/deliverystore"

	"velox-server/internal/store"
)

type unavailableDestinationProvider struct {
	attempted []string
	err       error
}

func (p *unavailableDestinationProvider) Name() string { return "drive" }

func (p *unavailableDestinationProvider) Deliver(_ context.Context, _ *store.Artifact, destination *Destination, _, _ string) (*Result, error) {
	p.attempted = append(p.attempted, destination.DestinationID)
	return nil, errors.Join(p.err, errors.New("destination unavailable"))
}

func TestDeliveryRunner_UnavailableExplicitDestinationDoesNotFallback(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "delivery-no-fallback.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	const (
		destinationA = "drive-folder-a"
		destinationB = "drive-folder-b"
		artifactID   = "artifact-no-fallback"
		deliveryID   = "delivery-no-fallback"
	)
	now := time.Now().UTC().Format(time.RFC3339)
	for _, destinationID := range []string{destinationA, destinationB} {
		if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{
			DestinationID:         destinationID,
			Provider:              "drive",
			ExternalDestinationID: "external-" + destinationID,
			Enabled:               true,
			FolderID:              destinationID,
			ConfigurationJSON:     "{}",
		}); err != nil {
			t.Fatalf("InsertDeliveryDestination(%s): %v", destinationID, err)
		}
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           "job-no-fallback",
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "artifact.mp4"),
		SHA256:          "no-fallback-sha",
		SizeBytes:       1,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("InsertArtifact: %v", err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  destinationA,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    3,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("InsertJobDelivery: %v", err)
	}

	// Destination A is explicit but unavailable. The runner must attempt only
	// A, fail the delivery, and never select the enabled destination B or
	// create a second delivery row.
	provider := &unavailableDestinationProvider{err: ErrProviderNotConfigured}
	registry := NewRegistry()
	registry.Register(provider)
	runner := NewDeliveryRunner(DefaultRunnerConfig(), registry, db.Delivery(), db, "no-fallback-runner")
	leases, err := db.Delivery().ClaimDeliveries(context.Background(), "no-fallback-runner", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 1 || leases[0].DestinationID != destinationA {
		t.Fatalf("claimed leases = %#v, want exactly explicit destination %q", leases, destinationA)
	}
	if err := runner.processLease(context.Background(), leases[0]); err == nil {
		t.Fatal("processLease: unavailable explicit destination must fail")
	}
	if len(provider.attempted) != 1 || provider.attempted[0] != destinationA {
		t.Fatalf("provider attempts = %v, want exactly [%q]", provider.attempted, destinationA)
	}

	row, err := db.Delivery().GetJobDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if row.Status != "FAILED" || row.LastError != "PROVIDER_NOT_CONFIGURED" {
		t.Fatalf("delivery = status=%q error=%q, want FAILED/PROVIDER_NOT_CONFIGURED", row.Status, row.LastError)
	}
	var count int
	if err := db.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ?`, artifactID).Scan(&count); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if count != 1 {
		t.Fatalf("delivery rows for artifact = %d, want 1; implicit fallback created another delivery", count)
	}
	var destinationCount int
	if err := db.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ? AND destination_id = ?`, artifactID, destinationB).Scan(&destinationCount); err != nil {
		t.Fatalf("count fallback destination: %v", err)
	}
	if destinationCount != 0 {
		t.Fatalf("fallback destination %q received %d delivery rows", destinationB, destinationCount)
	}

	// A transient failure follows the same explicit-target rule: it may be
	// retried, but it must remain attached to A and never be rerouted to B.
	provider.err = ErrProviderTransient
	const (
		transientArtifactID = "artifact-no-fallback-transient"
		transientDeliveryID = "delivery-no-fallback-transient"
	)
	if err := db.InsertArtifact(&store.Artifact{
		ID:              transientArtifactID,
		JobID:           "job-no-fallback-transient",
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "transient-artifact.mp4"),
		SHA256:          "no-fallback-transient-sha",
		SizeBytes:       1,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("InsertArtifact transient: %v", err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{
		DeliveryID:     transientDeliveryID,
		ArtifactID:     transientArtifactID,
		DestinationID:  destinationA,
		Status:         "PENDING",
		IdempotencyKey: transientDeliveryID,
		MaxAttempts:    3,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("InsertJobDelivery transient: %v", err)
	}
	transientLeases, err := db.Delivery().ClaimDeliveries(context.Background(), "no-fallback-runner", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDeliveries transient: %v", err)
	}
	if len(transientLeases) != 1 || transientLeases[0].DestinationID != destinationA {
		t.Fatalf("transient claimed leases = %#v, want explicit destination %q", transientLeases, destinationA)
	}
	if err := runner.processLease(context.Background(), transientLeases[0]); err != nil {
		t.Fatalf("transient processLease should schedule retry, got %v", err)
	}
	transientRow, err := db.Delivery().GetJobDelivery(context.Background(), transientDeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery transient: %v", err)
	}
	if transientRow.Status != "RETRY_WAIT" || transientRow.LastError != "TRANSIENT" {
		t.Fatalf("transient delivery = status=%q error=%q, want RETRY_WAIT/TRANSIENT", transientRow.Status, transientRow.LastError)
	}
	if len(provider.attempted) != 2 || provider.attempted[1] != destinationA {
		t.Fatalf("provider attempts after transient failure = %v, want A-only retry path", provider.attempted)
	}
	if err := db.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM job_deliveries WHERE destination_id = ?`, destinationB).Scan(&destinationCount); err != nil {
		t.Fatalf("count transient fallback destination: %v", err)
	}
	if destinationCount != 0 {
		t.Fatalf("transient fallback destination %q received %d delivery rows", destinationB, destinationCount)
	}
}
