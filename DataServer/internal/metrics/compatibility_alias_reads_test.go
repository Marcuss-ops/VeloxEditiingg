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
		"# TYPE velox_compat_alias_reads_total counter",
		`velox_compat_alias_reads_total{alias="voiceover_path",canonical="voiceover_paths"} 1`,
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q in metrics output:\n%s", want, output.String())
		}
	}
}
