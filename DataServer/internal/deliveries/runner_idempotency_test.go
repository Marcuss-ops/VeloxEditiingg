package deliveries

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/credentials"
	"velox-server/internal/store"
)

type publishedShortCircuitProvider struct {
	credentialCalls int
	providerCalls   int
}

func (p *publishedShortCircuitProvider) Name() string { return "youtube" }

func (p *publishedShortCircuitProvider) RequiredCredentialScopes() []string {
	return []string{"publish"}
}

func (p *publishedShortCircuitProvider) Deliver(_ context.Context, _ *store.Artifact, _ *Destination, _, _ string) (*Result, error) {
	p.providerCalls++
	return &Result{Success: true, RemoteID: "unexpected-provider-call"}, nil
}

func (p *publishedShortCircuitProvider) DeliverWithCredential(_ context.Context, _ *store.Artifact, _ *Destination, _, _ string, _ *credentials.AccessLease) (*Result, error) {
	p.credentialCalls++
	return &Result{Success: true, RemoteID: "unexpected-credential-call"}, nil
}

func TestDeliveryRunnerPublishedShortCircuitSkipsGoogleWork(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "published-short-circuit.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	const (
		jobID         = "job-published-short-circuit"
		publicationID = "publication-published-short-circuit"
		artifactID    = "artifact-published-short-circuit"
		destinationID = "destination-youtube-short-circuit"
		deliveryID    = "delivery-published-short-circuit"
	)
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.DB().Exec(`
		INSERT INTO jobs (job_id, status, revision, max_retries, created_at, updated_at, migrated_at)
		VALUES (?, 'DELIVERING', 0, 3, ?, ?, ?)`, jobID, now, now, now); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:         destinationID,
		Provider:              "youtube",
		ExternalDestinationID: "google-channel-opaque",
		Enabled:               true,
		ConfigurationJSON:     `{"credential_ref":"cred_0123456789abcdef0123456789abcdef0123"}`,
	}); err != nil {
		t.Fatalf("InsertDeliveryDestination: %v", err)
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID:              artifactID,
		JobID:           jobID,
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      filepath.Join(t.TempDir(), "video.mp4"),
		SHA256:          "published-short-circuit-sha",
		SizeBytes:       1,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("InsertArtifact: %v", err)
	}
	if _, err := db.DB().Exec(`
		INSERT INTO job_delivery_plans
			(job_id, destination_id, enabled, priority, retry_budget, metadata_json, created_at, updated_at)
		VALUES (?, ?, 1, 0, 3, ?, ?, ?)`,
		jobID, destinationID,
		`{"publication_id":"`+publicationID+`","credential_ref":"cred_0123456789abcdef0123456789abcdef0123"}`,
		now, now); err != nil {
		t.Fatalf("seed delivery plan: %v", err)
	}
	if err := db.InsertJobDelivery(&store.JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  destinationID,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    3,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("InsertJobDelivery: %v", err)
	}
	if err := db.CreatePublicationState(context.Background(), publicationID); err != nil {
		t.Fatalf("CreatePublicationState: %v", err)
	}
	if _, err := db.DB().Exec(`
		UPDATE publication_states
		SET state = 'PUBLISHED', remote_id = ?, remote_url = ?, updated_at = ?
		WHERE publication_id = ?`,
		"youtube-video-already-uploaded", "https://youtube.example/watch?v=already", now, publicationID); err != nil {
		t.Fatalf("mark publication published: %v", err)
	}

	provider := &publishedShortCircuitProvider{}
	registry := NewRegistry()
	registry.Register(provider)
	runner := NewDeliveryRunner(DefaultRunnerConfig(), registry, db, "published-short-circuit-runner")

	leases, err := db.ClaimDeliveries(context.Background(), "published-short-circuit-runner", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %d, want 1", len(leases))
	}
	if err := runner.processLease(context.Background(), leases[0]); err != nil {
		t.Fatalf("processLease: %v", err)
	}

	if provider.credentialCalls != 0 || provider.providerCalls != 0 {
		t.Fatalf("already-published target performed Google work: credential_calls=%d provider_calls=%d", provider.credentialCalls, provider.providerCalls)
	}
	row, err := db.GetJobDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if row.Status != "SUCCEEDED" || row.RemoteID != "youtube-video-already-uploaded" {
		t.Fatalf("delivery = status=%q remote_id=%q, want SUCCEEDED/youtube-video-already-uploaded", row.Status, row.RemoteID)
	}
}
