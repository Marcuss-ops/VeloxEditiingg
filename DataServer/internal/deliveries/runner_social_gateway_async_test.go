package deliveries

import (
	"context"
	"path/filepath"
	"testing"
	"time"
	"velox-server/internal/deliverystore"

	"velox-server/internal/publicationstate"
	"velox-server/internal/store"
)

type asyncSocialGatewayTestProvider struct {
	remoteStatus string
	uploads      int
	reconciles   int
}

func (p *asyncSocialGatewayTestProvider) Name() string { return "social_gateway" }

func (p *asyncSocialGatewayTestProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	p.uploads++
	return &Result{Success: true, Status: ResultStatusSubmittedToProvider, RemoteID: "social-delivery-async-1"}, nil
}

func (p *asyncSocialGatewayTestProvider) Reconcile(context.Context, string, string) (*Result, error) {
	p.reconciles++
	if p.remoteStatus != "published" {
		return &Result{Status: ResultStatusRemoteProcessing}, nil
	}
	return &Result{
		Success:  true,
		Status:   ResultStatusPublished,
		RemoteID: "youtube-video-async-1",
	}, nil
}

func TestDeliveryRunnerSocialGatewayWaitsForRemotePublication(t *testing.T) {
	ctx := context.Background()
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "social-gateway-async.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		jobID         = "job-social-gateway-async"
		publicationID = "publication-social-gateway-async"
		artifactID    = "artifact-social-gateway-async"
		destinationID = "destination-social-gateway-async"
		deliveryID    = "delivery-social-gateway-async"
	)
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{
		DestinationID:         destinationID,
		Provider:              "social_gateway",
		ExternalDestinationID: "external-social-destination",
		Enabled:               true,
		ConfigurationJSON:     "{}",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           jobID,
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "video.mp4"),
		SHA256:          "social-gateway-async-sha",
		SizeBytes:       1,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  destinationID,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    3,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreatePublicationState(ctx, publicationID); err != nil {
		t.Fatal(err)
	}

	provider := &asyncSocialGatewayTestProvider{remoteStatus: "processing"}
	registry := NewRegistry()
	registry.Register(provider)
	// Production explicitly registers Social Gateway through the legacy
	// phase adapter during the migration. Keep this test on that exact path.
	registry.RegisterLegacyPhaseProvider(provider)
	runner := NewDeliveryRunner(&RunnerConfig{
		LeaseDuration:   time.Minute,
		MaxAttempts:     3,
		BackoffSchedule: []time.Duration{0},
	}, registry, db.Delivery(), db, "social-gateway-async-runner")

	leases, err := db.Delivery().ClaimDeliveries(ctx, runner.identity, time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("first claim: %v leases=%d", err, len(leases))
	}
	if err := runner.processLease(ctx, leases[0]); err != nil {
		t.Fatalf("first process: %v", err)
	}

	row, err := db.Delivery().GetJobDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "RETRY_WAIT" {
		t.Fatalf("accepted remote operation was finalized locally: delivery status=%q", row.Status)
	}
	state, err := db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.RetryWait || state.RetryFrom != publicationstate.Verifying {
		t.Fatalf("publication state after remote processing: %+v", state)
	}
	if provider.uploads != 1 || provider.reconciles != 1 {
		t.Fatalf("provider calls after acceptance: uploads=%d reconciles=%d; want 1/1", provider.uploads, provider.reconciles)
	}

	// Make the retry immediately claimable without waiting for wall-clock
	// precision in SQLite's RFC3339 scheduler column.
	if _, err := db.DB().Exec(`UPDATE job_deliveries SET next_attempt_at = datetime('now','-1 second') WHERE delivery_id = ?`, deliveryID); err != nil {
		t.Fatal(err)
	}
	provider.remoteStatus = "published"
	leases, err = db.Delivery().ClaimDeliveries(ctx, runner.identity, time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("reconcile claim: %v leases=%d", err, len(leases))
	}
	if err := runner.processLease(ctx, leases[0]); err != nil {
		t.Fatalf("reconcile process: %v", err)
	}

	row, err = db.Delivery().GetJobDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "SUCCEEDED" || row.RemoteID != "youtube-video-async-1" {
		t.Fatalf("delivery after remote publication: status=%q remote_id=%q", row.Status, row.RemoteID)
	}
	state, err = db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.Published || state.RemoteID != "youtube-video-async-1" {
		t.Fatalf("publication state after remote publication: state=%s remote_id=%q", state.State, state.RemoteID)
	}
	if provider.uploads != 1 || provider.reconciles != 2 {
		t.Fatalf("provider calls after reconciliation: uploads=%d reconciles=%d; want 1/2", provider.uploads, provider.reconciles)
	}
}
