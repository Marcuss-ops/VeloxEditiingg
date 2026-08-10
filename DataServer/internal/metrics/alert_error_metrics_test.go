package metrics

import (
	"strings"
	"testing"
)

func TestAlertEvaluationErrorsMetricExposesEngineAndCategory(t *testing.T) {
	reg := NewRegistry()
	collector := NewCollector(reg)
	collector.RecordAlertEvaluationError("compute", "infrastructure", 2)
	collector.RecordAlertEvaluationError("fleet", "inventory", 1)

	out := dumpRegistryAll(t, reg)
	for _, want := range []string{
		"# TYPE velox_alert_evaluation_errors_total counter",
		`velox_alert_evaluation_errors_total{engine="compute",category="infrastructure"} 2`,
		`velox_alert_evaluation_errors_total{engine="fleet",category="inventory"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
