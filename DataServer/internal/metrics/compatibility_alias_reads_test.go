package metrics

import (
	"bytes"
	"strings"
	"testing"

	"velox-shared/compatibility"
)

func TestCompatibilityAliasReadObserverExportsMetric(t *testing.T) {
	reg := NewRegistry()
	collector := NewCollector(reg)
	compatibility.SetAliasReadObserver(collector.NewCompatibilityAliasObserver())
	t.Cleanup(func() { compatibility.SetAliasReadObserver(nil) })

	_ = compatibility.ReadStringList(map[string]interface{}{
		"voiceover_path": "legacy.mp3",
	}, compatibility.VoiceoverPathsKey)

	var output bytes.Buffer
	if err := reg.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	for _, want := range []string{
		"# TYPE velox_compatibility_alias_reads_total counter",
		`velox_compatibility_alias_reads_total{alias="voiceover_path",canonical="voiceover_paths"} 1`,
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, output.String())
		}
	}
}

func TestCompatibilityAliasRejectionObserverExportsMetric(t *testing.T) {
	reg := NewRegistry()
	collector := NewCollector(reg)
	compatibility.SetAliasRejectedObserver(collector.NewCompatibilityAliasRejectionObserver())
	t.Cleanup(func() { compatibility.SetAliasRejectedObserver(nil) })
	compatibility.SetMode(compatibility.ModeStrict)
	t.Cleanup(func() { compatibility.SetMode(compatibility.ModeCompat) })
	_ = compatibility.ReadStringList(map[string]interface{}{"voiceover_path": "legacy.mp3"}, compatibility.VoiceoverPathsKey)

	var output bytes.Buffer
	if err := reg.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	want := `velox_compatibility_rejections_total{alias="voiceover_path",canonical="voiceover_paths"} 1`
	if !strings.Contains(output.String(), want) {
		t.Fatalf("missing %q in metrics output:\n%s", want, output.String())
	}
}
