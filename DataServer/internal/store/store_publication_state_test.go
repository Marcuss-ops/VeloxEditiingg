package store

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/publicationstate"
)

func TestPublicationStatePersistsRetryCheckpointAndPhaseKey(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	if err := db.CreatePublicationState(ctx, "pub-store-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, "pub-store-1", publicationstate.WaitingForRender, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, "pub-store-1", publicationstate.ArtifactBound, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, "pub-store-1", publicationstate.Ready, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, "pub-store-1", publicationstate.Uploading, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, "pub-store-1", publicationstate.VideoCreated, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, "pub-store-1", publicationstate.MetadataApplying, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationPartial(ctx, "pub-store-1", publicationstate.LocalizationsApplying, "LOCALIZATIONS_FAILED"); err != nil {
		t.Fatal(err)
	}
	state, err := db.GetPublicationState(ctx, "pub-store-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.Partial || state.RetryFrom != publicationstate.LocalizationsApplying {
		t.Fatalf("persisted state = %+v", state)
	}

	key1, already, err := db.BeginPublicationPhaseEffect(ctx, "pub-store-1", publicationstate.LocalizationsApplying, "apply")
	if err != nil || already {
		t.Fatalf("first phase reservation key=%q already=%v err=%v", key1, already, err)
	}
	key2, already, err := db.BeginPublicationPhaseEffect(ctx, "pub-store-1", publicationstate.LocalizationsApplying, "apply")
	if err != nil || !already || key1 != key2 {
		t.Fatalf("replayed phase reservation key=%q already=%v err=%v", key2, already, err)
	}
	if err := db.CompletePublicationPhaseEffect(ctx, "pub-store-1", publicationstate.LocalizationsApplying, "apply", true, ""); err != nil {
		t.Fatal(err)
	}
	if err := db.CompletePublicationPhaseEffect(ctx, "pub-store-1", publicationstate.LocalizationsApplying, "missing", true, ""); !errors.Is(err, ErrPublicationPhaseConflict) {
		t.Fatalf("missing phase completion err=%v, want conflict", err)
	}
}

func TestPublicationStateTransitionReplayIsNoOp(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	if err := db.CreatePublicationState(ctx, "pub-store-2"); err != nil {
		t.Fatal(err)
	}
	first, err := db.TransitionPublicationState(ctx, "pub-store-2", publicationstate.WaitingForRender, "")
	if err != nil {
		t.Fatal(err)
	} else if first.Revision != 1 {
		t.Fatalf("first revision=%d, want 1", first.Revision)
	}
	second, err := db.TransitionPublicationState(ctx, "pub-store-2", publicationstate.WaitingForRender, "")
	if err != nil {
		t.Fatal(err)
	} else if second.Revision != 1 {
		t.Fatalf("replay revision=%d, want 1", second.Revision)
	}
}
