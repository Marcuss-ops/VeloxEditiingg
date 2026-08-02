package deliveries

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/credentials"
	"velox-server/internal/store"
)

type runnerCredentialRepository struct {
	record credentials.StoredCredential
	uses   []credentials.UsageEvent
}

func (r *runnerCredentialRepository) PutCredential(_ context.Context, record credentials.StoredCredential) error {
	r.record = record
	return nil
}
func (r *runnerCredentialRepository) GetCredential(_ context.Context, ref string) (*credentials.StoredCredential, error) {
	if ref != r.record.Ref {
		return nil, credentials.ErrNotFound
	}
	record := r.record
	return &record, nil
}
func (r *runnerCredentialRepository) UpdateCredential(_ context.Context, record credentials.StoredCredential) error {
	r.record = record
	return nil
}
func (r *runnerCredentialRepository) RevokeCredential(_ context.Context, _ string, _ time.Time) error {
	return nil
}
func (r *runnerCredentialRepository) RecordCredentialUse(_ context.Context, _ string, event credentials.UsageEvent) error {
	r.uses = append(r.uses, event)
	return nil
}

type credentialAwareTestProvider struct{}

func (credentialAwareTestProvider) Name() string { return "credential-test" }
func (credentialAwareTestProvider) Deliver(context.Context, *store.Artifact, *Destination, string, string) (*Result, error) {
	return nil, ErrProviderNotConfigured
}
func (credentialAwareTestProvider) RequiredCredentialScopes() []string { return []string{"publish"} }
func (credentialAwareTestProvider) DeliverWithCredential(context.Context, *store.Artifact, *Destination, string, string, *credentials.AccessLease) (*Result, error) {
	return &Result{Success: true, RemoteID: "remote"}, nil
}

func TestDeliveryRunnerIssuesShortLeaseAndAuditsResult(t *testing.T) {
	repo := &runnerCredentialRepository{}
	keys, err := credentials.NewKeyring(1, map[int][]byte{1: []byte("01234567890123456789012345678901")})
	if err != nil {
		t.Fatal(err)
	}
	vault, err := credentials.NewVault(repo, keys)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := vault.Put(context.Background(), "credential-test", "owner", []string{"publish"}, time.Now().Add(time.Hour), time.Time{}, credentials.Material{AccessToken: "short-access", RefreshToken: "never-in-lease"})
	if err != nil {
		t.Fatal(err)
	}
	runner := NewDeliveryRunner(nil, nil, nil, "worker-1").WithCredentialVault(vault)
	lease, err := runner.issueCredentialLease(context.Background(), credentialAwareTestProvider{}, &Destination{CredentialRef: ref, DeliveryMetadataJSON: `{"publication_id":"pub-1"}`}, store.DeliveryLease{DeliveryID: "delivery-1"})
	if err != nil {
		t.Fatal(err)
	}
	if lease.AccessToken != "short-access" || lease.PublicationID != "pub-1" || lease.WorkerID != "worker-1" {
		t.Fatalf("lease = %#v", lease)
	}
	if err := vault.RecordLeaseResult(context.Background(), lease, true, ""); err != nil {
		t.Fatal(err)
	}
	if len(repo.uses) != 2 || !repo.uses[1].Success || repo.uses[1].PublicationID != "pub-1" {
		t.Fatalf("credential audit uses = %#v", repo.uses)
	}
}
