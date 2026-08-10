package store

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/publicationstate"
)

func TestRecordPublicationRemoteResultBeforePublishedIsReplaySafe(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	const publicationID = "pub-async-checkpoint"

	if err := db.CreatePublicationState(ctx, publicationID); err != nil {
		t.Fatal(err)
	}
	for _, state := range []publicationstate.State{
		publicationstate.WaitingForRender,
		publicationstate.ArtifactBound,
		publicationstate.Ready,
		publicationstate.Uploading,
	} {
		if _, err := db.TransitionPublicationState(ctx, publicationID, state, ""); err != nil {
			t.Fatalf("transition to %s: %v", state, err)
		}
	}
	if _, err := db.PersistPublicationVideoCreated(ctx, publicationID, "artifact-async", "social-operation-1", ""); err != nil {
		t.Fatalf("PersistPublicationVideoCreated: %v", err)
	}
	if _, err := db.TransitionPublicationState(ctx, publicationID, publicationstate.MetadataApplying, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, publicationID, publicationstate.Verifying, ""); err != nil {
		t.Fatal(err)
	}
	verifyingState, err := db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}

	// This is the durable checkpoint taken after remote verification but
	// before the terminal VERIFYING -> PUBLISHED transition. A crash after
	// this write must not leave the submission/operation ID as the final ID.
	if err := db.RecordPublicationRemoteResult(ctx, publicationID, verifyingState.Revision, verifyingState.RemoteID, "youtube-media-1", "https://youtube.example/watch/1"); err != nil {
		t.Fatalf("RecordPublicationRemoteResult: %v", err)
	}
	state, err := db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.Verifying || state.RemoteID != "youtube-media-1" {
		t.Fatalf("checkpoint state = %+v", state)
	}

	// Replay the terminal transition after the simulated crash.
	if _, err := db.TransitionPublicationState(ctx, publicationID, publicationstate.Published, ""); err != nil {
		t.Fatalf("replay Published transition: %v", err)
	}
	state, err = db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.Published || state.RemoteID != "youtube-media-1" {
		t.Fatalf("replayed final state = %+v", state)
	}
}

func TestRecordPublicationRemoteResultRejectsStaleCheckpoint(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	const publicationID = "pub-async-stale-checkpoint"

	if err := db.CreatePublicationState(ctx, publicationID); err != nil {
		t.Fatal(err)
	}
	for _, state := range []publicationstate.State{
		publicationstate.WaitingForRender,
		publicationstate.ArtifactBound,
		publicationstate.Ready,
		publicationstate.Uploading,
	} {
		if _, err := db.TransitionPublicationState(ctx, publicationID, state, ""); err != nil {
			t.Fatalf("transition to %s: %v", state, err)
		}
	}
	if _, err := db.PersistPublicationVideoCreated(ctx, publicationID, "artifact-stale", "social-operation-stale", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, publicationID, publicationstate.MetadataApplying, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, publicationID, publicationstate.Verifying, ""); err != nil {
		t.Fatal(err)
	}
	state, err := db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.RecordPublicationRemoteResult(ctx, publicationID, state.Revision-1, state.RemoteID, "youtube-stale", ""); !errors.Is(err, ErrPublicationPhaseConflict) {
		t.Fatalf("stale revision error = %v, want ErrPublicationPhaseConflict", err)
	}
	if err := db.RecordPublicationRemoteResult(ctx, publicationID, state.Revision, "wrong-operation-id", "youtube-stale", ""); !errors.Is(err, ErrPublicationPhaseConflict) {
		t.Fatalf("stale operation error = %v, want ErrPublicationPhaseConflict", err)
	}
	state, err = db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.RemoteID != "social-operation-stale" {
		t.Fatalf("stale checkpoint changed remote ID to %q", state.RemoteID)
	}
}
