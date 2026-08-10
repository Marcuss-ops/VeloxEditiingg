package publicationstate

import "testing"

// TestPublishedIsReachableOnlyAfterVerification protects the publication
// state machine's evidence boundary. publicationstate currently models phase
// authority, not statemachine.Actor ownership; therefore PUBLISHED is proven
// here by requiring VERIFYING rather than by claiming an actor registry that
// does not exist for this domain.
func TestPublishedIsReachableOnlyAfterVerification(t *testing.T) {
	if err := ValidateTransition(Uploading, Published); err == nil {
		t.Fatal("uploading emitted PUBLISHED without verification")
	}
	if err := ValidateTransition(MetadataApplying, Published); err == nil {
		t.Fatal("metadata application emitted PUBLISHED without verification")
	}
	if err := ValidateTransition(Verifying, Published); err != nil {
		t.Fatalf("verification rejected canonical publication completion: %v", err)
	}
	if PublicationStatus("completed").Valid() {
		t.Fatal("completed is an input-assembly status, not a publication status")
	}
	if PublicationStatus("SUCCEEDED").Valid() {
		t.Fatal("SUCCEEDED is a job/delivery status, not a publication status")
	}
}

func TestPublicationStateMachineAndPhaseKeys(t *testing.T) {
	for _, pair := range [][2]State{{Pending, WaitingForRender}, {WaitingForRender, ArtifactBound}, {ArtifactBound, Ready}, {Ready, Uploading}, {Uploading, VideoCreated}, {VideoCreated, MetadataApplying}, {MetadataApplying, LocalizationsApplying}, {LocalizationsApplying, Verifying}, {Verifying, Published}} {
		if err := ValidateTransition(pair[0], pair[1]); err != nil {
			t.Fatalf("%s -> %s: %v", pair[0], pair[1], err)
		}
	}
	if err := ValidateTransition(Uploading, Published); err == nil {
		t.Fatal("uploading could skip verification phases")
	}
	if ResumeAfterFailure(Partial) != LocalizationsApplying {
		t.Fatal("partial publication did not resume at localizations")
	}
	if IdempotencyKey("p", Uploading) == IdempotencyKey("p", MetadataApplying) {
		t.Fatal("phase keys must differ")
	}
}
