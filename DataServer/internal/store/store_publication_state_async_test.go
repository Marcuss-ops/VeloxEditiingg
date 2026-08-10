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
	if state.State != publicationstate.Verifying || state.RemoteID != "youtube-media-1" || state.SubmittedRemoteID != "social-operation-1" {
		t.Fatalf("checkpoint state = %+v", state)
	}

	// Replay the terminal transition after the simulated crash. The
	// succeeded VERIFYING effect is the durable proof that reconciliation
	// was the authority for this promotion.
	if _, _, err := db.BeginPublicationPhaseEffect(ctx, publicationID, publicationstate.Verifying, "verify"); err != nil {
		t.Fatal(err)
	}
	if err := db.CompletePublicationReconciliationEffect(ctx, publicationID, "verify"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublicationState(ctx, publicationID, publicationstate.Published, ""); !errors.Is(err, ErrPublicationPhaseConflict) {
		t.Fatalf("generic Published transition error = %v, want ErrPublicationPhaseConflict", err)
	}
	if _, err := db.CompletePublicationAfterReconciliation(ctx, publicationID, "verify"); err != nil {
		t.Fatalf("replay Published transition: %v", err)
	}
	state, err = db.GetPublicationState(ctx, publicationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != publicationstate.Published || state.RemoteID != "youtube-media-1" || !state.ReconciliationVerified {
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

func TestCompletePublicationAfterReconciliationRequiresExactSucceededEffect(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	const publicationID = "pub-async-exact-effect"

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
	if _, err := db.PersistPublicationVideoCreated(ctx, publicationID, "artifact-exact", "operation-exact", ""); err != nil {
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
	if err := db.RecordPublicationRemoteResult(ctx, publicationID, state.Revision, state.RemoteID, "media-exact", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.BeginPublicationPhaseEffect(ctx, publicationID, publicationstate.Verifying, "canonical-verify"); err != nil {
		t.Fatal(err)
	}
	if err := db.CompletePublicationReconciliationEffect(ctx, publicationID, "canonical-verify"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompletePublicationAfterReconciliation(ctx, publicationID, "wrong-verify"); !errors.Is(err, ErrPublicationPhaseConflict) {
		t.Fatalf("wrong operation error = %v, want ErrPublicationPhaseConflict", err)
	}
	if _, err := db.CompletePublicationAfterReconciliation(ctx, publicationID, "canonical-verify"); err != nil {
		t.Fatalf("exact operation completion: %v", err)
	}
}

func TestCompletePublicationAfterReconciliationRejectsSubmissionAsFinalMedia(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	const publicationID = "pub-async-same-id"

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
	if _, err := db.PersistPublicationVideoCreated(ctx, publicationID, "artifact-same", "operation-same", ""); err != nil {
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
	if err := db.RecordPublicationRemoteResult(ctx, publicationID, state.Revision, state.RemoteID, "operation-same", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.BeginPublicationPhaseEffect(ctx, publicationID, publicationstate.Verifying, "canonical-verify"); err != nil {
		t.Fatal(err)
	}
	if err := db.CompletePublicationReconciliationEffect(ctx, publicationID, "canonical-verify"); !errors.Is(err, ErrPublicationPhaseConflict) {
		t.Fatalf("same operation/media ID reconciliation error = %v, want ErrPublicationPhaseConflict", err)
	}
	if _, err := db.CompletePublicationAfterReconciliation(ctx, publicationID, "canonical-verify"); !errors.Is(err, ErrPublicationPhaseConflict) {
		t.Fatalf("same operation/media ID completion error = %v, want ErrPublicationPhaseConflict", err)
	}
}
