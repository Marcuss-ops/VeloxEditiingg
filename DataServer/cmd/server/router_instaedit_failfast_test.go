package main

import (
	"context"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/instaeditauth"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

// mockMinimalJobsRepo satisfies jobs.Repository with no-op methods.
type mockMinimalJobsRepo struct{}

func (m *mockMinimalJobsRepo) Get(ctx context.Context, id string) (*jobs.Job, error) {
	return nil, nil
}

func (m *mockMinimalJobsRepo) List(ctx context.Context, filter jobs.Filter) ([]jobs.Job, error) {
	return nil, nil
}

func (m *mockMinimalJobsRepo) Counts(ctx context.Context) (jobs.Counts, error) {
	return nil, nil
}

func (m *mockMinimalJobsRepo) SetStatus(ctx context.Context, id string, from, to jobs.Status) error {
	return nil
}

func (m *mockMinimalJobsRepo) Fail(ctx context.Context, id string, reason string) error {
	return nil
}

func (m *mockMinimalJobsRepo) Cancel(ctx context.Context, id string, reason string, revision int) error {
	return nil
}

func (m *mockMinimalJobsRepo) Delete(ctx context.Context, id string) error {
	return nil
}

// mockMinimalAssetRepo satisfies store.AssetRepository with no-op methods.
type mockMinimalAssetRepo struct{}

func (m *mockMinimalAssetRepo) Insert(ctx context.Context, a store.AssetRecord) error {
	return nil
}

func (m *mockMinimalAssetRepo) GetByID(ctx context.Context, assetID string) (*store.AssetRecord, error) {
	return nil, nil
}

func (m *mockMinimalAssetRepo) GetByIDAndWorkspace(ctx context.Context, assetID string, workspaceID int64) (*store.AssetRecord, error) {
	return nil, nil
}

func (m *mockMinimalAssetRepo) GetBySHA256(ctx context.Context, sha256 string) (*store.AssetRecord, error) {
	return nil, nil
}

func (m *mockMinimalAssetRepo) UpdateStatus(ctx context.Context, assetID, from, to string) error {
	return nil
}

func (m *mockMinimalAssetRepo) SoftDelete(ctx context.Context, assetID string) error {
	return nil
}

func (m *mockMinimalAssetRepo) InsertSource(ctx context.Context, s store.AssetSourceRecord) error {
	return nil
}

func (m *mockMinimalAssetRepo) LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error {
	return nil
}

func (m *mockMinimalAssetRepo) ListByJob(ctx context.Context, jobID string) ([]store.AssetRecord, error) {
	return nil, nil
}

func TestRegisterInstaEditRoutes_DisabledWhenVerifierNil(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	deps := InstaEditRouteDeps{}

	if err := registerInstaEditRoutes(r, deps); err != nil {
		t.Fatalf("expected no error when feature is disabled, got %v", err)
	}

	instaeditPrefix := "/api/v1/instaedit"
	for _, route := range r.Routes() {
		if len(route.Path) >= len(instaeditPrefix) && route.Path[:len(instaeditPrefix)] == instaeditPrefix {
			t.Fatalf("InstaEdit routes should not be mounted when verifier is nil, got %s", route.Path)
		}
	}
}

func TestRegisterInstaEditRoutes_FailsWhenEnabledButDepsMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	deps := InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		// Enqueuer, Store, Jobs, Assets are intentionally nil.
	}

	if err := registerInstaEditRoutes(r, deps); err == nil {
		t.Fatal("expected error when InstaEdit is enabled but dependencies are missing")
	}
}

func TestRegisterInstaEditRoutes_SucceedsWithAllDeps(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	deps := InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		Enqueuer: &enqueue.Enqueuer{},
		Store:    &store.SQLiteStore{},
		Jobs:     &mockMinimalJobsRepo{},
		Assets:   &mockMinimalAssetRepo{},
	}

	if err := registerInstaEditRoutes(r, deps); err != nil {
		t.Fatalf("expected no error with all dependencies, got %v", err)
	}

	found := false
	for _, route := range r.Routes() {
		if route.Path == "/api/v1/instaedit/jobs" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected /api/v1/instaedit/jobs route to be mounted")
	}
}

func TestRegisterInstaEditRoutes_FailsWhenAnySingleDepMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	fullDeps := InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		Enqueuer: &enqueue.Enqueuer{},
		Store:    &store.SQLiteStore{},
		Jobs:     &mockMinimalJobsRepo{},
		Assets:   &mockMinimalAssetRepo{},
	}

	cases := []struct {
		name string
		mutate func(*InstaEditRouteDeps)
	}{
		{"missing enqueuer", func(d *InstaEditRouteDeps) { d.Enqueuer = nil }},
		{"missing store", func(d *InstaEditRouteDeps) { d.Store = nil }},
		{"missing jobs repo", func(d *InstaEditRouteDeps) { d.Jobs = nil }},
		{"missing assets repo", func(d *InstaEditRouteDeps) { d.Assets = nil }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			deps := fullDeps
			tc.mutate(&deps)
			if err := registerInstaEditRoutes(r, deps); err == nil {
				t.Fatal("expected error when a required dependency is missing")
			}
		})
	}
}
