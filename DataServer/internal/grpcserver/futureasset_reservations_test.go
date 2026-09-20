package grpcserver

import (
	"testing"

	"velox-server/internal/placement"
)

func TestPrioritizeCurrentJobCandidates(t *testing.T) {
	candidates := []placement.TaskCandidate{
		{TaskID: "old-1", JobID: "job-old-1"},
		{TaskID: "current", JobID: "job-current"},
		{TaskID: "old-2", JobID: "job-old-2"},
	}

	got := prioritizeCurrentJobCandidates(candidates, "job-current")
	if got[0].TaskID != "current" {
		t.Fatalf("current job candidate first = %q, want current", got[0].TaskID)
	}
	if got[1].TaskID != "old-1" || got[2].TaskID != "old-2" {
		t.Fatalf("non-current candidate order changed: %+v", got)
	}
}

func TestPrioritizeCurrentJobCandidatesWithoutCurrentJobPreservesInput(t *testing.T) {
	candidates := []placement.TaskCandidate{{TaskID: "old-1", JobID: "job-old-1"}}
	got := prioritizeCurrentJobCandidates(candidates, "job-missing")
	if len(got) != 1 || got[0].TaskID != "old-1" {
		t.Fatalf("candidate list changed without current job: %+v", got)
	}
}
