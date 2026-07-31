package taskattempts

import "testing"

func TestExecutionEventOriginClosedSet(t *testing.T) {
	valid := []ExecutionEventOrigin{
		ExecutionEventOriginMaster,
		ExecutionEventOriginWorker,
		ExecutionEventOriginEngine,
		ExecutionEventOriginFFmpeg,
		ExecutionEventOriginUpload,
		ExecutionEventOriginValidation,
	}
	for _, origin := range valid {
		if !origin.IsValid() {
			t.Errorf("origin %q should be valid", origin)
		}
	}
	if ExecutionEventOrigin("custom").IsValid() {
		t.Fatal("custom origin should be rejected")
	}
}

func TestExecutionEventScopeClosedSet(t *testing.T) {
	valid := []ExecutionEventScope{
		ExecutionEventScopeJob,
		ExecutionEventScopeTask,
		ExecutionEventScopeAttempt,
		ExecutionEventScopeSegment,
		ExecutionEventScopeAudioTrack,
		ExecutionEventScopeSubtitleTrack,
		ExecutionEventScopeArtifact,
	}
	for _, scope := range valid {
		if !scope.IsValid() {
			t.Errorf("scope %q should be valid", scope)
		}
	}
	if ExecutionEventScope("custom").IsValid() {
		t.Fatal("custom scope should be rejected")
	}
}

func TestExecutionEventValidateScopeRequirements(t *testing.T) {
	segment := ExecutionEvent{
		EventID: "segment-0", AttemptID: "attempt-1",
		Origin: ExecutionEventOriginEngine, Scope: ExecutionEventScopeSegment,
	}
	if err := segment.Validate(); err == nil {
		t.Fatal("segment event without segment_index should be rejected")
	}

	track := ExecutionEvent{
		EventID: "track-0", AttemptID: "attempt-1",
		Origin: ExecutionEventOriginEngine, Scope: ExecutionEventScopeAudioTrack,
	}
	if err := track.Validate(); err == nil {
		t.Fatal("audio-track event without track_index should be rejected")
	}

	artifact := ExecutionEvent{
		EventID: "artifact-0", AttemptID: "attempt-1",
		Origin: ExecutionEventOriginUpload, Scope: ExecutionEventScopeArtifact,
	}
	if err := artifact.Validate(); err == nil {
		t.Fatal("artifact event without artifact_id should be rejected")
	}

	segmentIndex := 0
	valid := segment
	valid.SegmentIndex = &segmentIndex
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid segment event rejected: %v", err)
	}
}
