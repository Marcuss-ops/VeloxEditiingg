package publicationstate

import "testing"

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
