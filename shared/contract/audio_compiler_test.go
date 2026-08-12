package contract

import (
	"strings"
	"testing"
)

func TestAudioCompiler_UsesCanonicalTimelineWithoutReinterpretingTiming(t *testing.T) {
	timeline := testCanonicalTimeline()
	before, err := timeline.CanonicalJSON()
	if err != nil {
		t.Fatalf("before CanonicalJSON: %v", err)
	}

	compiled, err := NewAudioCompiler().Compile(timeline)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if compiled.TimelineRevision != timeline.Revision || compiled.DurationUS != timeline.DurationUS {
		t.Fatalf("identity/duration = %d/%d, want %d/%d", compiled.TimelineRevision, compiled.DurationUS, timeline.Revision, timeline.DurationUS)
	}
	wantSHA, err := timeline.TimelineSHA256()
	if err != nil {
		t.Fatalf("TimelineSHA256: %v", err)
	}
	if compiled.TimelineSHA256 != wantSHA {
		t.Fatalf("TimelineSHA256 = %q, want %q", compiled.TimelineSHA256, wantSHA)
	}
	if len(compiled.Tracks) != 3 {
		t.Fatalf("tracks = %d, want 3", len(compiled.Tracks))
	}
	want := []struct {
		id, role, asset                           string
		start, duration, sourceIn, sourceDuration int64
	}{
		{"vo-main", "voiceover", "voiceover-a", 0, 60_000_000, 0, 60_000_000},
		{"music-bed", "music", "music-a", 0, 60_000_000, 0, 60_000_000},
		{"sfx-hit", "sfx", "sfx-a", 35_000_000, 1_000_000, 2_000_000, 1_000_000},
	}
	for index, expected := range want {
		got := compiled.Tracks[index]
		if got.TrackID != expected.id || got.Role != expected.role || got.AssetID != expected.asset {
			t.Errorf("track[%d] identity = %q/%q/%q, want %q/%q/%q", index, got.TrackID, got.Role, got.AssetID, expected.id, expected.role, expected.asset)
		}
		if got.TimelineStartUS != expected.start || got.TimelineDurationUS != expected.duration || got.SourceInUS != expected.sourceIn || got.SourceDurationUS != expected.sourceDuration {
			t.Errorf("track[%d] timing = %+v, want start=%d duration=%d source=%d/%d", index, got, expected.start, expected.duration, expected.sourceIn, expected.sourceDuration)
		}
	}
	if len(compiled.Tracks) != len(timeline.Voiceovers)+len(timeline.Music)+len(timeline.SFX) {
		t.Fatalf("audio compiler included non-audio timeline segments: %d tracks", len(compiled.Tracks))
	}

	after, err := timeline.CanonicalJSON()
	if err != nil {
		t.Fatalf("after CanonicalJSON: %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("AudioCompiler mutated CanonicalTimeline")
	}
}

func TestAudioCompiler_OrderIsDeterministicAndHashBound(t *testing.T) {
	timeline := testCanonicalTimeline()
	first, err := CompileAudioTimeline(timeline)
	if err != nil {
		t.Fatalf("first CompileAudioTimeline: %v", err)
	}
	second, err := CompileAudioTimeline(timeline)
	if err != nil {
		t.Fatalf("second CompileAudioTimeline: %v", err)
	}
	if first.TimelineSHA256 != second.TimelineSHA256 {
		t.Fatalf("same timeline produced different hashes: %q != %q", first.TimelineSHA256, second.TimelineSHA256)
	}
	for i := range first.Tracks {
		if first.Tracks[i] != second.Tracks[i] {
			t.Fatalf("track[%d] changed between compiles: %+v != %+v", i, first.Tracks[i], second.Tracks[i])
		}
	}

	timeline.Segments[0].TimelineStartUS++
	changed, err := CompileAudioTimeline(timeline)
	if err != nil {
		t.Fatalf("changed CompileAudioTimeline: %v", err)
	}
	if changed.TimelineSHA256 == first.TimelineSHA256 {
		t.Fatal("audio plan retained old timeline hash after editorial change")
	}
}

func TestAudioCompiler_ValidateCompiledAudioPlanRejectsTimelineDrift(t *testing.T) {
	timeline := testCanonicalTimeline()
	compiled, err := CompileAudioTimeline(timeline)
	if err != nil {
		t.Fatalf("CompileAudioTimeline: %v", err)
	}
	if err := ValidateCompiledAudioPlan(timeline, compiled); err != nil {
		t.Fatalf("ValidateCompiledAudioPlan(same timeline): %v", err)
	}
	compiled.Tracks[0].TimelineStartUS++
	if err := ValidateCompiledAudioPlan(timeline, compiled); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("ValidateCompiledAudioPlan(drift) = %v, want timeline drift error", err)
	}
}

func TestAudioCompiler_RejectsInvalidTimeline(t *testing.T) {
	timeline := testCanonicalTimeline()
	timeline.Segments[0].TimelineStartUS = timeline.DurationUS
	if _, err := CompileAudioTimeline(timeline); err == nil || !strings.Contains(err.Error(), "timeline range exceeds duration_us") {
		t.Fatalf("Compile invalid timeline = %v, want fail-closed bounds error", err)
	}
	if _, err := CompileAudioTimeline(nil); err == nil {
		t.Fatal("Compile(nil) = nil error, want fail-closed error")
	}
}
