package fleet

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/store"
)

func TestConfigureLevelDSmokeCapabilityProductionRejectsMissingAssetResolver(t *testing.T) {
	backend := LevelDSmokeBackend{}
	status, err := ConfigureLevelDSmokeCapability(NewExecutorRegistry(), NewLevelDSmokeExecutor(backend), false)
	if err == nil {
		t.Fatal("incomplete production smoke backend must fail registration")
	}
	if !errors.Is(err, ErrSmokeRunnerNotWired) {
		t.Fatalf("error = %v, want ErrSmokeRunnerNotWired", err)
	}
	if status.State != SmokeCapabilityMisconfigured {
		t.Fatalf("state = %q, want %q", status.State, SmokeCapabilityMisconfigured)
	}
}

func TestConfigureLevelDSmokeCapabilityRejectsStubInProduction(t *testing.T) {
	backend := LevelDSmokeBackend{
		Worker:    &stubDevelopmentSmokeWorker{},
		Drive:     &stubDevelopmentSmokeDrive{},
		Asset:     NewStubAssetResolver("asset://synthetic", 0),
		Lease:     &stubDevelopmentSmokeLease{},
		SmokeRuns: &stubDevelopmentSmokeRuns{},
		Verifier:  &stubDevelopmentSmokeVerifier{},
	}
	status, err := ConfigureLevelDSmokeCapability(NewExecutorRegistry(), NewLevelDSmokeExecutor(backend), false)
	if err == nil || !errors.Is(err, ErrSmokeRunnerNotWired) {
		t.Fatalf("stub production registration error = %v, want ErrSmokeRunnerNotWired", err)
	}
	if status.State != SmokeCapabilityMisconfigured {
		t.Fatalf("state = %q, want %q", status.State, SmokeCapabilityMisconfigured)
	}
}

// Minimal interfaces for the production stub-rejection test.
type stubDevelopmentSmokeWorker struct{}

func (*stubDevelopmentSmokeWorker) DownloadAsset(context.Context, string, string, string, string) error {
	return nil
}
func (*stubDevelopmentSmokeWorker) RunFFmpegRender(context.Context, string, string, string, string) (string, int64, error) {
	return "x", 1, nil
}
func (*stubDevelopmentSmokeWorker) CleanupWorkerTemp(context.Context, string, string) error {
	return nil
}

type stubDevelopmentSmokeDrive struct{}

func (*stubDevelopmentSmokeDrive) UploadArtifact(context.Context, string, string, int64, string) (string, error) {
	return "id", nil
}

type stubDevelopmentSmokeLease struct{}

func (*stubDevelopmentSmokeLease) AcquireSmokeLease(context.Context, string, string) error {
	return nil
}
func (*stubDevelopmentSmokeLease) ReleaseSmokeLease(context.Context, string) error { return nil }

type stubDevelopmentSmokeRuns struct{}

func (*stubDevelopmentSmokeRuns) InsertSmokeRun(context.Context, store.SmokeRun) error { return nil }
func (*stubDevelopmentSmokeRuns) MarkSmokeSucceeded(context.Context, string, time.Time, int64, string) error {
	return nil
}
func (*stubDevelopmentSmokeRuns) MarkSmokeFailed(context.Context, string, time.Time, int64, string) error {
	return nil
}
func (*stubDevelopmentSmokeRuns) GetLatestSmokeForWorker(context.Context, string) (*store.SmokeRun, error) {
	return nil, store.ErrSmokeRunNotFound
}
func (*stubDevelopmentSmokeRuns) ListRecentSmokesForWorker(context.Context, string, int) ([]store.SmokeRun, error) {
	return nil, nil
}

type stubDevelopmentSmokeVerifier struct{}

func (*stubDevelopmentSmokeVerifier) VerifyArtifact(context.Context, string, int64) (string, error) {
	return "sha", nil
}

func TestConfigureLevelDSmokeCapabilityDevelopmentIncompleteIsDisabled(t *testing.T) {
	status, err := ConfigureLevelDSmokeCapability(NewExecutorRegistry(), NewLevelDSmokeExecutor(LevelDSmokeBackend{}), true)
	if err != nil {
		t.Fatalf("incomplete development backend should disable, not fail bootstrap: %v", err)
	}
	if status.State != SmokeCapabilityDisabled {
		t.Fatalf("state = %q, want %q", status.State, SmokeCapabilityDisabled)
	}
}
