package deliveries

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/publicationstate"
	"velox-server/internal/store"
)

type crashResumePhaseProvider struct {
	uploads       int
	metadata      int
	verifications int
}

func (p *crashResumePhaseProvider) Name() string { return "phase-test" }
func (p *crashResumePhaseProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	return nil, ErrProviderNotConfigured
}
func (p *crashResumePhaseProvider) Capabilities() map[publicationstate.State]bool {
	return map[publicationstate.State]bool{
		publicationstate.Uploading:        true,
		publicationstate.MetadataApplying: true,
		publicationstate.Verifying:        true,
	}
}
func (p *crashResumePhaseProvider) ExecutePhase(_ context.Context, phase publicationstate.State, _ *PublicationPhaseContext) (*Result, error) {
	switch phase {
	case publicationstate.Uploading:
		p.uploads++
		return &Result{Success: true, RemoteID: "remote-video-1", RemoteURL: "https://provider/video/1"}, nil
	case publicationstate.MetadataApplying:
		p.metadata++
		if p.metadata == 1 {
			return nil, errors.Join(ErrProviderTransient, errors.New("metadata unavailable"))
		}
		return &Result{Success: true}, nil
	case publicationstate.Verifying:
		p.verifications++
		return &Result{Success: true}, nil
	default:
		return nil, ErrProviderPermanent
	}
}

func TestDeliveryRunnerResumesMetadataWithoutSecondUpload(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "phase-resume.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const artifactID = "artifact-phase-resume"
	const deliveryID = "delivery-phase-resume"
	const destinationID = "destination-phase-resume"
	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{DestinationID: destinationID, Provider: "phase-test", ExternalDestinationID: "external-phase", Enabled: true, ConfigurationJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{ID: artifactID, JobID: "job-phase-resume", Type: "video", StorageProvider: "local", StorageKey: filepath.Join(t.TempDir(), "video.mp4"), SHA256: "artifact-sha", SizeBytes: 1, Status: "READY", VerifiedAt: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertJobDelivery(&store.JobDelivery{DeliveryID: deliveryID, ArtifactID: artifactID, DestinationID: destinationID, Status: "PENDING", IdempotencyKey: deliveryID, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreatePublicationState(context.Background(), "publication-phase-resume"); err != nil {
		t.Fatal(err)
	}

	provider := &crashResumePhaseProvider{}
	registry := NewRegistry()
	registry.Register(provider)
	runner := NewDeliveryRunner(&RunnerConfig{LeaseDuration: time.Minute, MaxAttempts: 3, BackoffSchedule: []time.Duration{0}}, registry, db, "phase-runner")

	leases, err := db.ClaimDeliveries(context.Background(), "phase-runner", time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("first claim: %v leases=%d", err, len(leases))
	}
	if err := runner.processLease(context.Background(), leases[0]); err != nil {
		t.Fatalf("first process: %v", err)
	}
	state, err := db.GetPublicationState(context.Background(), "publication-phase-resume")
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.RetryWait || state.RetryFrom != publicationstate.MetadataApplying || state.RemoteID != "remote-video-1" {
		t.Fatalf("checkpoint after metadata failure: %+v", state)
	}

	leases, err = db.ClaimDeliveries(context.Background(), "phase-runner", time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("resume claim: %v leases=%d", err, len(leases))
	}
	if err := runner.processLease(context.Background(), leases[0]); err != nil {
		t.Fatalf("resume process: %v", err)
	}

	state, err = db.GetPublicationState(context.Background(), "publication-phase-resume")
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.Published {
		t.Fatalf("final publication state = %s", state.State)
	}
	if provider.uploads != 1 || provider.metadata != 2 || provider.verifications != 1 {
		t.Fatalf("phase calls upload=%d metadata=%d verify=%d; want 1/2/1", provider.uploads, provider.metadata, provider.verifications)
	}
	row, err := db.GetJobDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "SUCCEEDED" || row.RemoteID != "remote-video-1" {
		t.Fatalf("delivery = %+v", row)
	}
	events, err := db.ListAuditEvents(context.Background(), "publication-phase-resume", 20)
	if err != nil {
		t.Fatal(err)
	}
	var sawStarted, sawFailed, sawCompleted bool
	for _, event := range events {
		switch event.Action {
		case "PUBLICATION_PHASE_STARTED":
			sawStarted = true
		case "PUBLICATION_PHASE_FAILED":
			sawFailed = true
		case "PUBLICATION_COMPLETED":
			sawCompleted = true
		}
	}
	if !sawStarted || !sawFailed || !sawCompleted {
		t.Fatalf("publication audit incomplete: %+v", events)
	}
}
