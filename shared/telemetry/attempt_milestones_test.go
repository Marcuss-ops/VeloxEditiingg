package telemetry

import "testing"

// TestCanonicalAttemptMilestonesPinsMainWaterfallVocabulary pins the closed
// main-waterfall milestone list. The worker marks exactly these milestones on
// the attempt timeline and the master's WaterfallBuilder derives its
// non-overlapping buckets from this list; a change here is a wire-contract
// change and must be coordinated across both modules.
func TestCanonicalAttemptMilestonesPinsMainWaterfallVocabulary(t *testing.T) {
	want := []AttemptMilestone{
		MilestoneAttemptAccepted, MilestoneExecutionStarted, MilestoneAssetsRequested,
		MilestoneFirstAssetStarted, MilestoneAllAssetsReady, MilestonePlanStarted,
		MilestonePlanCompleted, MilestoneRenderStarted, MilestoneRenderCompleted,
		MilestoneFinalizeStarted, MilestoneFinalizeCompleted, MilestoneOutputDurable,
		MilestonePublishQueued, MilestonePublishStarted,
		MilestonePublishSlotWaitStarted, MilestonePublishSlotWaitCompleted,
		MilestonePublishDeclareStarted, MilestonePublishDeclareCompleted,
		MilestonePublishUploadStarted, MilestonePublishUploadCompleted,
		MilestonePublishRemoteFinalizeStarted, MilestonePublishRemoteFinalizeCompleted,
		MilestonePublishCommitWaitStarted, MilestonePublishCommitWaitCompleted,
		MilestonePublishSpoolCommitStarted, MilestonePublishSpoolCommitCompleted,
		MilestonePublishCompleted,
		MilestoneResultSending, MilestoneResultSent, MilestoneAttemptCompleted,
	}
	got := CanonicalAttemptMilestones()
	if len(got) != len(want) {
		t.Fatalf("CanonicalAttemptMilestones() length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("CanonicalAttemptMilestones()[%d] = %q, want %q", i, got[i], want[i])
		}
		if !IsCanonicalAttemptMilestone(want[i]) {
			t.Errorf("IsCanonicalAttemptMilestone(%q) = false, want true", want[i])
		}
	}
}

// TestRemoteMaterializationWaitIsSubSpanNotMainWaterfall locks the design
// intent that the remote materialization wait is a per-asset sub-span
// (tracked as RemoteWaitMS/RemoteWaitCount counters), never a main-waterfall
// milestone: the markers ARE canonical (the recorder accepts them) but they
// must not appear in CanonicalAttemptMilestones(), so the attempt waterfall
// stays sequential and non-overlapping.
func TestRemoteMaterializationWaitIsSubSpanNotMainWaterfall(t *testing.T) {
	for _, name := range []AttemptMilestone{
		MilestoneRemoteMaterializationWaitStarted,
		MilestoneRemoteMaterializationWaitCompleted,
	} {
		if !IsCanonicalAttemptMilestone(name) {
			t.Errorf("IsCanonicalAttemptMilestone(%q) = false, want true (sub-span markers stay canonical)", name)
		}
		for _, core := range CanonicalAttemptMilestones() {
			if core == name {
				t.Errorf("sub-span milestone %q leaked into the main waterfall vocabulary", name)
			}
		}
	}
}

// TestIsCanonicalAttemptMilestoneRejectsUnknown pins the fail-closed
// membership: empty and unknown names are never canonical, so the recorder
// refuses to fabricate timeline entries for typos.
func TestIsCanonicalAttemptMilestoneRejectsUnknown(t *testing.T) {
	for _, name := range []AttemptMilestone{"", "assets.all_ready ", "render.complete", "hypothetical.bogus"} {
		if IsCanonicalAttemptMilestone(name) {
			t.Errorf("IsCanonicalAttemptMilestone(%q) = true, want false", name)
		}
	}
}

// TestCanonicalAttemptMilestonesDefensive pins the read-only contract:
// mutating the returned slice must not affect later calls.
func TestCanonicalAttemptMilestonesDefensive(t *testing.T) {
	got := CanonicalAttemptMilestones()
	got[0] = "MUTATED"
	again := CanonicalAttemptMilestones()
	if again[0] == "MUTATED" {
		t.Fatal("CanonicalAttemptMilestones() returned a shared slice")
	}
}
