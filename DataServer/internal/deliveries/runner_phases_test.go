package deliveries

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
	"velox-server/internal/deliverystore"

	"velox-server/internal/publicationstate"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
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
func (p *crashResumePhaseProvider) Reconcile(_ context.Context, _, remoteID string) (*Result, error) {
	p.verifications++
	return &Result{Success: true, Status: ResultStatusPublished, RemoteID: "remote-video-final-1"}, nil
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
		return &Result{Success: true, Status: ResultStatusPublished, RemoteID: "remote-video-final-1"}, nil
	default:
		return nil, ErrProviderPermanent
	}
}

type legacyAcceptedProvider struct{}

func (legacyAcceptedProvider) Name() string { return "legacy-accepted" }
func (legacyAcceptedProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	return &Result{Success: true, Status: "accepted", RemoteID: "remote-operation-1"}, nil
}

type syncPublishedProvider struct{}

type invalidResultClosingStoreProvider struct {
	db *store.SQLiteStore
}

func (p *invalidResultClosingStoreProvider) Name() string { return "drive" }
func (p *invalidResultClosingStoreProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	_ = p.db.Close()
	return &Result{Success: true}, nil
}

func (syncPublishedProvider) Name() string { return "sync-published" }
func (syncPublishedProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	return &Result{Success: true, Status: "published", RemoteID: "remote-operation-sync"}, nil
}

type unregisteredReconcilerProvider struct{}

func (unregisteredReconcilerProvider) Name() string { return "unregistered-reconciler" }
func (unregisteredReconcilerProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	return &Result{Success: true, Status: "accepted", RemoteID: "remote-operation-2"}, nil
}
func (unregisteredReconcilerProvider) Reconcile(context.Context, string, string) (*Result, error) {
	return &Result{Success: true, Status: "published", RemoteID: "remote-final-2"}, nil
}

func TestValidateProviderResultRejectsCanonicalAsyncStatusWithoutReconciler(t *testing.T) {
	for _, status := range []string{ResultStatusSubmittedToProvider, ResultStatusRemoteProcessing, ResultStatusReconciliation, ResultStatusPublished} {
		err := validateProviderResult(&Result{Success: true, Status: status, RemoteID: "operation-1"})
		if !errors.Is(err, ErrProviderPermanent) {
			t.Fatalf("status %q validation error = %v; want canonical permanent classification", status, err)
		}
	}
	if err := validateProviderResult(&Result{Success: true, Status: "published", RemoteID: "operation-1"}); !errors.Is(err, ErrProviderPermanent) {
		t.Fatalf("lowercase published validation error = %v; want permanent classification", err)
	}
}

func TestAdvanceSkippedPhaseRejectsRequiredMetadataPhase(t *testing.T) {
	runner := &DeliveryRunner{}
	phase, err := runner.advanceSkippedPhase(context.Background(), "publication-required-metadata", publicationstate.MetadataApplying)
	if err == nil || !errors.Is(err, ErrProviderPermanent) || phase != publicationstate.MetadataApplying {
		t.Fatalf("advanceSkippedPhase(metadata) = phase=%s err=%v; want permanent fail-closed error", phase, err)
	}
}

func TestProcessLeaseRejectsSynchronousPublishedStatus(t *testing.T) {
	ctx := context.Background()
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "sync-published.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		artifactID    = "artifact-sync-published"
		destinationID = "destination-sync-published"
		deliveryID    = "delivery-sync-published"
	)
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{DestinationID: destinationID, Provider: "sync-published", ExternalDestinationID: "external-sync", Enabled: true, ConfigurationJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertArtifact(&store.Artifact{ID: artifactID, JobID: "job-sync-published", Type: "video", StorageProvider: "local", StorageKey: filepath.Join(t.TempDir(), "video.mp4"), SHA256: "sync-published-sha", SizeBytes: 1, Status: "READY", VerifiedAt: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{DeliveryID: deliveryID, ArtifactID: artifactID, DestinationID: destinationID, Status: "PENDING", IdempotencyKey: deliveryID, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	registry := NewRegistry()
	registry.Register(syncPublishedProvider{})
	runner := NewDeliveryRunner(&RunnerConfig{LeaseDuration: time.Minute, MaxAttempts: 3}, registry, db.Delivery(), db, "sync-published-runner")
	leases, err := db.Delivery().ClaimDeliveries(ctx, runner.identity, time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v leases=%d", err, len(leases))
	}
	if err := runner.processLease(ctx, leases[0]); err == nil {
		t.Fatal("synchronous published status was allowed without reconciliation")
	}
	row, err := db.Delivery().GetJobDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "FAILED" || row.RemoteID != "" {
		t.Fatalf("delivery = %+v, want FAILED without operation ID persisted", row)
	}
}

func TestProcessLeaseRejectsUnregisteredReconciler(t *testing.T) {
	ctx := context.Background()
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "unregistered-reconciler.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		artifactID    = "artifact-unregistered-reconciler"
		destinationID = "destination-unregistered-reconciler"
		deliveryID    = "delivery-unregistered-reconciler"
	)
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{DestinationID: destinationID, Provider: "unregistered-reconciler", ExternalDestinationID: "external-reconciler", Enabled: true, ConfigurationJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertArtifact(&store.Artifact{ID: artifactID, JobID: "job-unregistered-reconciler", Type: "video", StorageProvider: "local", StorageKey: filepath.Join(t.TempDir(), "video.mp4"), SHA256: "unregistered-reconciler-sha", SizeBytes: 1, Status: "READY", VerifiedAt: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{DeliveryID: deliveryID, ArtifactID: artifactID, DestinationID: destinationID, Status: "PENDING", IdempotencyKey: deliveryID, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	registry := NewRegistry()
	registry.Register(unregisteredReconcilerProvider{})
	runner := NewDeliveryRunner(&RunnerConfig{LeaseDuration: time.Minute, MaxAttempts: 3}, registry, db.Delivery(), db, "unregistered-reconciler-runner")
	leases, err := db.Delivery().ClaimDeliveries(ctx, runner.identity, time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v leases=%d", err, len(leases))
	}
	if err := runner.processLease(ctx, leases[0]); err == nil {
		t.Fatal("unregistered reconciler was allowed to use monolithic success path")
	}
	row, err := db.Delivery().GetJobDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "FAILED" || row.RemoteID != "" {
		t.Fatalf("delivery = %+v, want FAILED without remote publication", row)
	}
}

func TestLegacyProviderCannotPromoteAcceptedOperationToPublished(t *testing.T) {
	ctx := context.Background()
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "legacy-accepted.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		publicationID = "publication-legacy-accepted"
		artifactID    = "artifact-legacy-accepted"
		destinationID = "destination-legacy-accepted"
		deliveryID    = "delivery-legacy-accepted"
	)
	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{DestinationID: destinationID, Provider: "legacy-accepted", ExternalDestinationID: "external-legacy", Enabled: true, ConfigurationJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{ID: artifactID, JobID: "job-legacy-accepted", Type: "video", StorageProvider: "local", StorageKey: filepath.Join(t.TempDir(), "video.mp4"), SHA256: "legacy-accepted-sha", SizeBytes: 1, Status: "READY", VerifiedAt: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{DeliveryID: deliveryID, ArtifactID: artifactID, DestinationID: destinationID, Status: "PENDING", IdempotencyKey: deliveryID, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreatePublicationState(ctx, publicationID); err != nil {
		t.Fatal(err)
	}

	if _, err := db.DB().Exec(`UPDATE publication_states SET job_id = ? WHERE publication_id = ?`, "job-legacy-accepted", publicationID); err != nil {
		t.Fatal(err)
	}
	provider := legacyAcceptedProvider{}
	registry := NewRegistry()
	registry.Register(provider)
	registry.RegisterLegacyPhaseProvider(provider)
	runner := NewDeliveryRunner(&RunnerConfig{LeaseDuration: time.Minute, MaxAttempts: 3}, registry, db.Delivery(), db, "legacy-phase-runner")
	leases, err := db.Delivery().ClaimDeliveries(ctx, runner.identity, time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v leases=%d", err, len(leases))
	}
	if err := runner.processLease(ctx, leases[0]); err == nil {
		t.Fatal("accepted operation was promoted without verification")
	}

	state, stateErr := db.GetPublicationState(ctx, publicationID)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if state.State == publicationstate.Published {
		t.Fatalf("publication reached PUBLISHED without remote evidence: %+v", state)
	}
	if state.State != publicationstate.Partial || state.RetryFrom != publicationstate.MetadataApplying {
		t.Fatalf("publication failure checkpoint = %+v, want PARTIAL/METADATA_APPLYING", state)
	}
	row, rowErr := db.Delivery().GetJobDelivery(ctx, deliveryID)
	if rowErr != nil {
		t.Fatal(rowErr)
	}
	if row.Status != "FAILED" || row.RemoteID != "" {
		t.Fatalf("delivery result = %+v, want FAILED without remote publication", row)
	}
}

func TestPublicationPhaseFailureExhaustsDeliveryRetryBudget(t *testing.T) {
	ctx := context.Background()
	db := openDeliveryTestDB(t)

	const (
		publicationID = "publication-phase-budget"
		artifactID    = "artifact-phase-budget"
		destinationID = "destination-phase-budget"
		deliveryID    = "delivery-phase-budget"
	)
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{
		DestinationID: destinationID, Provider: "phase-test", ExternalDestinationID: "external-phase-budget",
		Enabled: true, ConfigurationJSON: "{}",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID: artifactID, JobID: "job-phase-budget", Type: "video", StorageProvider: "local",
		StorageKey: filepath.Join(t.TempDir(), "video.mp4"), SHA256: "phase-budget-sha",
		SizeBytes: 1, Status: "READY", VerifiedAt: now, CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{
		DeliveryID: deliveryID, ArtifactID: artifactID, DestinationID: destinationID,
		Status: "PENDING", IdempotencyKey: deliveryID, MaxAttempts: 1,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreatePublicationState(ctx, publicationID); err != nil {
		t.Fatal(err)
	}

	provider := &crashResumePhaseProvider{}
	registry := NewRegistry()
	registry.Register(provider)
	runner := NewDeliveryRunner(&RunnerConfig{
		LeaseDuration: time.Minute, MaxAttempts: 3, BackoffSchedule: []time.Duration{0},
	}, registry, db.Delivery(), db, "phase-budget-runner")

	leases, err := db.Delivery().ClaimDeliveries(ctx, runner.identity, time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v leases=%d", err, len(leases))
	}
	if leases[0].AttemptNumber != 1 || leases[0].MaxAttempts != 1 {
		t.Fatalf("lease budget = attempt %d/max %d, want 1/1", leases[0].AttemptNumber, leases[0].MaxAttempts)
	}
	if err := runner.processLease(ctx, leases[0]); err == nil {
		t.Fatal("exhausted phase retry budget was reported as a retry")
	}

	state, err := db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.Partial || state.RetryFrom != publicationstate.MetadataApplying {
		t.Fatalf("publication checkpoint = %+v, want PARTIAL/METADATA_APPLYING", state)
	}
	row, err := db.Delivery().GetJobDelivery(ctx, deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "FAILED" {
		t.Fatalf("delivery status = %q, want FAILED after exhausted phase retry budget", row.Status)
	}
	again, err := db.Delivery().ClaimDeliveries(ctx, "phase-budget-retry", time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("exhausted delivery remained claimable: %+v", again)
	}
}

func TestInvalidProviderResultPropagatesPersistenceFailure(t *testing.T) {
	ctx := context.Background()
	db := openDeliveryTestDB(t)
	const (
		destinationID = "destination-invalid-result"
		artifactID    = "artifact-invalid-result"
		deliveryID    = "delivery-invalid-result"
	)
	seedDriveDeliveryTriple(t, db, destinationID, artifactID, deliveryID, "job-invalid-result")

	provider := &invalidResultClosingStoreProvider{db: db}
	registry := NewRegistry()
	registry.Register(provider)
	runner := NewDeliveryRunner(&RunnerConfig{LeaseDuration: time.Minute, MaxAttempts: 1}, registry, db.Delivery(), db, "invalid-result-runner")
	leases, err := db.Delivery().ClaimDeliveries(ctx, runner.identity, time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v leases=%d", err, len(leases))
	}

	err = runner.processLease(ctx, leases[0])
	if !errors.Is(err, errDeliveryStatePersistence) {
		t.Fatalf("invalid provider result error = %v; want persistence failure", err)
	}
	if !errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("invalid provider result error = %v; want infrastructure classification", err)
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
	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{DestinationID: destinationID, Provider: "phase-test", ExternalDestinationID: "external-phase", Enabled: true, ConfigurationJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertArtifact(&store.Artifact{ID: artifactID, JobID: "job-phase-resume", Type: "video", StorageProvider: "local", StorageKey: filepath.Join(t.TempDir(), "video.mp4"), SHA256: "artifact-sha", SizeBytes: 1, Status: "READY", VerifiedAt: now, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delivery().InsertJobDelivery(&deliverystore.JobDelivery{DeliveryID: deliveryID, ArtifactID: artifactID, DestinationID: destinationID, Status: "PENDING", IdempotencyKey: deliveryID, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreatePublicationState(context.Background(), "publication-phase-resume"); err != nil {
		t.Fatal(err)
	}

	provider := &crashResumePhaseProvider{}
	registry := NewRegistry()
	registry.Register(provider)
	runner := NewDeliveryRunner(&RunnerConfig{LeaseDuration: time.Minute, MaxAttempts: 3, BackoffSchedule: []time.Duration{0}}, registry, db.Delivery(), db, "phase-runner")

	leases, err := db.Delivery().ClaimDeliveries(context.Background(), "phase-runner", time.Minute, 1)
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

	leases, err = db.Delivery().ClaimDeliveries(context.Background(), "phase-runner", time.Minute, 1)
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
	row, err := db.Delivery().GetJobDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "SUCCEEDED" || row.RemoteID != "remote-video-final-1" {
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
