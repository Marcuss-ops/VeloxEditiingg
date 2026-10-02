package metrics

import (
	"strings"
	"testing"
)

// TestIntakeSourceFamily_RegisteredOnCollector verifies that the
// package-level intake families are registered on every Collector so
// /metrics exposes them. A boot regression that drops the registration
// would silently hide alias-usage telemetry.
func TestIntakeSourceFamily_RegisteredOnCollector(t *testing.T) {
	reg := NewRegistry()
	_ = NewCollector(reg)
	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		"# TYPE pipeline_intake_source_accepted_total counter",
		"# TYPE pipeline_creator_intake_accepted_total counter",
		"# TYPE pipeline_intake_scene_warnings_total counter",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in collector exposition:\n%s", want, out)
		}
	}
}

// TestIntakeSourceSink_IncAccepted verifies the sink records the intake
// source on the bounded family.
func TestIntakeSourceSink_IncAccepted(t *testing.T) {
	reg := NewRegistry()
	_ = NewCollector(reg)
	sink := NewIntakeSourceSink()
	sink.IncAccepted("canonical")
	sink.IncAccepted("creator")
	sink.IncAccepted("canonical")

	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		`pipeline_intake_source_accepted_total{intake_source="canonical"} 2`,
		`pipeline_intake_source_accepted_total{intake_source="creator"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestRecordIntakeSource_PackageLevel verifies the package-level
// convenience used by direct-enqueue producers (script, pipeline-run).
func TestRecordIntakeSource_PackageLevel(t *testing.T) {
	reg := NewRegistry()
	_ = NewCollector(reg)
	RecordIntakeSource("script_generate")
	RecordIntakeSource("pipeline_run")

	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		`pipeline_intake_source_accepted_total{intake_source="script_generate"} 1`,
		`pipeline_intake_source_accepted_total{intake_source="pipeline_run"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestIntakeSourceSink_SatisfiesRecorderContract pins the structural
// contract the canonical submitter depends on (creatorflow.IntakeSourceRecorder
// is satisfied by IntakeSourceSink via IncAccepted(string)).
func TestIntakeSourceSink_SatisfiesRecorderContract(t *testing.T) {
	var _ interface{ IncAccepted(string) } = NewIntakeSourceSink()
}

// TestRecordIntakeSceneWarning_BoundedLabels verifies the soft-deprecation
// warning counter records per intake path with the anomaly reason, keeping
// high-cardinality scene/job identifiers out of the label set.
func TestRecordIntakeSceneWarning_BoundedLabels(t *testing.T) {
	reg := NewRegistry()
	_ = NewCollector(reg)
	RecordIntakeSceneWarning("creator_push", "kind_clip_without_clip_asset")
	RecordIntakeSceneWarning("api_v1_jobs", "kind_clip_without_clip_asset")

	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		`pipeline_intake_scene_warnings_total{path="creator_push",reason="kind_clip_without_clip_asset"} 1`,
		`pipeline_intake_scene_warnings_total{path="api_v1_jobs",reason="kind_clip_without_clip_asset"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
