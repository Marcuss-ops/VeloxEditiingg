package controltransport

import (
	"sort"
	"testing"
)

// TestCapabilities_KnownConstants asserts the kebab-case string
// literals that the worker emits and the master inspects. Renaming a
// constant below is a wire-protocol break — workers compiled today
// will advertise the OLD string verbatim, and masters compiled today
// will check HasCapability against the NEW string. Always add a new
// constant alongside (Phase 6 drain handles the cutover).
func TestCapabilities_KnownConstants(t *testing.T) {
	want := []string{
		"artifact.commit.v1",
		"executor.hybrid.v1",
		"task.output.declared.v1",
		"artifact.upload.plan.v1",
		"artifact.upload.completed.v1",
		"task.commit.ack.v1",
	}
	if len(AllCapabilities) != len(want) {
		t.Fatalf("AllCapabilities length: got %d, want %d (orphaned literal?)",
			len(AllCapabilities), len(want))
	}
	sortedGot := append([]string{}, AllCapabilities...)
	sort.Strings(sortedGot)
	sortedWant := append([]string{}, want...)
	sort.Strings(sortedWant)
	for i := range sortedGot {
		if sortedGot[i] != sortedWant[i] {
			t.Errorf("AllCapabilities[%d]: got %q, want %q",
				i, sortedGot[i], sortedWant[i])
		}
	}
}

func TestCapabilities_IsKnownCapability(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{CapabilityArtifactCommitV1, true},
		{CapabilityExecutorHybridV1, true},
		{CapabilityTaskOutputDeclaredV1, true},
		{CapabilityArtifactUploadPlanV1, true},
		{CapabilityArtifactUploadCompletedV1, true},
		{CapabilityTaskCommitAckV1, true},
		{"", false},
		{"artifact.commit.v0", false},   // deprecated-by-naming
		{"artifact.commit.v2.beta", false}, // not yet a constant
		{"executor.legacy.v1", false},
		{"ARTIFACT.COMMIT.V1", false}, // case-sensitive (kebab norm)
	}
	for _, c := range cases {
		got := IsKnownCapability(c.in)
		if got != c.want {
			t.Errorf("IsKnownCapability(%q): got %v, want %v", c.in, got, c.want)
		}
	}
}

// TestCapabilities_AllCapabilitiesIsClosedSet asserts the
// AllCapabilities slice contains exactly the documented constants —
// not more, not fewer. A constant declared in this file but missing
// from AllCapabilities is a footgun: it would pass IsKnownCapability
// negative tests but the master would never iterate it for log
// inspection.
func TestCapabilities_AllCapabilitiesIsClosedSet(t *testing.T) {
	declared := map[string]bool{
		CapabilityArtifactCommitV1:            true,
		CapabilityExecutorHybridV1:            true,
		CapabilityTaskOutputDeclaredV1:        true,
		CapabilityArtifactUploadPlanV1:        true,
		CapabilityArtifactUploadCompletedV1:   true,
		CapabilityTaskCommitAckV1:             true,
	}
	allAsSet := map[string]bool{}
	for _, c := range AllCapabilities {
		if allAsSet[c] {
			t.Errorf("AllCapabilities contains duplicate entry %q", c)
		}
		allAsSet[c] = true
	}
	if len(declared) != len(allAsSet) {
		t.Fatalf("AllCapabilities length (%d) != declared constants (%d);"+
			" either add the constant to AllCapabilities or remove the orphan",
			len(allAsSet), len(declared))
	}
	for c := range declared {
		if !allAsSet[c] {
			t.Errorf("declared constant %q missing from AllCapabilities", c)
		}
	}
	for c := range allAsSet {
		if !declared[c] {
			t.Errorf("AllCapabilities contains undeclared string %q", c)
		}
	}
}
