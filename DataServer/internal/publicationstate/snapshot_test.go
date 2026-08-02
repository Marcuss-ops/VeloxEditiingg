package publicationstate

import "testing"

func TestSnapshotRetryKeepsTheFailedPhase(t *testing.T) {
	snapshot, err := NewSnapshot("pub-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []State{WaitingForRender, ArtifactBound, Uploading, MetadataApplying, LocalizationsApplying, Verifying} {
		snapshot.State = state
		snapshot.RetryFrom = ""
		retry, err := snapshot.Transition(RetryWait)
		if err != nil {
			t.Fatalf("%s -> RETRY_WAIT: %v", state, err)
		}
		resumed, err := retry.Transition(retry.RetryFrom)
		if err != nil || resumed.State != state {
			t.Fatalf("retry resumed at %#v, want %s (err=%v)", resumed, state, err)
		}
	}
}

func TestSnapshotLocalizationsRetryNeverReuploads(t *testing.T) {
	snapshot := Snapshot{PublicationID: "pub-2", State: MetadataApplying, Revision: 4}
	partial, err := snapshot.TransitionPartial(LocalizationsApplying)
	if err != nil {
		t.Fatal(err)
	}
	if partial.RetryFrom != LocalizationsApplying {
		t.Fatalf("partial retry checkpoint = %s", partial.RetryFrom)
	}
	retry, err := partial.Transition(RetryWait)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := retry.Transition(retry.RetryFrom)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != LocalizationsApplying {
		t.Fatalf("resumed state = %s, want LOCALIZATIONS_APPLYING", resumed.State)
	}
	if _, err := retry.Transition(Uploading); err == nil {
		t.Fatal("retry checkpoint allowed video upload to run again")
	}
}

func TestSnapshotSameTransitionIsIdempotent(t *testing.T) {
	snapshot := Snapshot{PublicationID: "pub-3", State: Published, Revision: 9}
	replayed, err := snapshot.Transition(Published)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != snapshot {
		t.Fatalf("same-state replay changed snapshot: %#v -> %#v", snapshot, replayed)
	}
}
