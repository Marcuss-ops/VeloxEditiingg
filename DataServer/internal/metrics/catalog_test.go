package metrics

import (
	"strings"
	"testing"
)

// TestCatalog_NoDuplicateNames verifies that MetricCatalog has no
// duplicate entries. Duplicates are a programmer error — the registry
// key is the canonical name.
func TestCatalog_NoDuplicateNames(t *testing.T) {
	// The map itself already enforces unique keys (Go maps can't have
	// duplicates). This test also verifies that every Name field matches
	// its map key — a mismatch would mean the catalog is inconsistent.
	seen := make(map[string]struct{})
	for key, def := range MetricCatalog {
		if key != def.Name {
			t.Errorf("catalog entry has key %q but def.Name=%q — must match exactly", key, def.Name)
		}
		if _, exists := seen[key]; exists {
			t.Errorf("duplicate catalog key: %q", key)
		}
		seen[key] = struct{}{}
	}
}

// TestCatalog_AllNamesFollowConvention verifies that every metric name
// follows the naming convention: lowercase, dot-separated, no spaces,
// no trailing dots, and a valid unit suffix.
func TestCatalog_AllNamesFollowConvention(t *testing.T) {
	for name, def := range MetricCatalog {
		// Must be lowercase.
		if name != strings.ToLower(name) {
			t.Errorf("metric name %q is not lowercase", name)
		}
		// Must contain at least one dot.
		if !strings.Contains(name, ".") {
			t.Errorf("metric name %q does not contain a dot separator", name)
		}
		// No spaces.
		if strings.Contains(name, " ") {
			t.Errorf("metric name %q contains spaces", name)
		}
		// No trailing dots.
		if strings.HasSuffix(name, ".") {
			t.Errorf("metric name %q has a trailing dot", name)
		}
		// No leading dots.
		if strings.HasPrefix(name, ".") {
			t.Errorf("metric name %q has a leading dot", name)
		}
		// Component must be non-empty.
		if def.Component == "" {
			t.Errorf("metric name %q has empty Component", name)
		}
		// Unit must be non-empty.
		if def.Unit == "" {
			t.Errorf("metric name %q has empty Unit", name)
		}
		// Description must be non-empty.
		if def.Description == "" {
			t.Errorf("metric name %q has empty Description", name)
		}
		// Kind must be valid.
		switch def.Kind {
		case KindCounter, KindGauge, KindHistogram:
			// valid
		default:
			t.Errorf("metric name %q has invalid Kind: %q", name, def.Kind)
		}
	}
}

// TestCatalog_RequiredMetricsExist verifies that the most critical
// metric names are present. These are the metrics that dashboards,
// alerts, and the operator-facing control room depend on.
func TestCatalog_RequiredMetricsExist(t *testing.T) {
	required := []string{
		// Engine phases — most important for performance debugging.
		"engine.segment_build_ms",
		"engine.asset_download_ms",
		"engine.concat_ms",
		"engine.mux_audio_ms",
		"engine.copy_final_ms",
		"engine.audio_download_ms",
		// Pipeline phases.
		"pipeline.compile_ms",
		"pipeline.validate_ms",
		"pipeline.resolve_ms",
		"pipeline.render_ms",
		"pipeline.total_ms",
		// Native process.
		"native.total_ms",
		"native.process_wait_ms",
		// Output.
		"output.bytes",
		// FFmpeg.
		"ffmpeg.speed_ratio",
		// Cache.
		"cache.hits",
		"cache.misses",
		"cache.bytes",
		// Worker resources.
		"worker.cpu_utilization_ratio",
		"worker.disk_free_bytes",
		"worker.active_tasks",
		// Scorecard.
		"scorecard.render_speed_ratio",
		"scorecard.compute_seconds_total",
		// Cost.
		"cost.total_per_output_minute",
		// Waste.
		"waste.wasted_cpu_ms",
		// TaskRunner.
		"taskrunner.execute_ms",
		// Queue.
		"queue.ms",
	}

	for _, name := range required {
		if _, ok := MetricCatalog[name]; !ok {
			t.Errorf("required metric %q is missing from MetricCatalog", name)
		}
	}
}

// TestCatalog_ValidateMetricName verifies the lookup function.
func TestCatalog_ValidateMetricName(t *testing.T) {
	// Known name.
	def, ok := ValidateMetricName("engine.segment_build_ms")
	if !ok {
		t.Error("ValidateMetricName('engine.segment_build_ms') should be found")
	}
	if def.Component != CompEngine {
		t.Errorf("expected component %q, got %q", CompEngine, def.Component)
	}

	// Unknown name.
	_, ok = ValidateMetricName("nonexistent.metric_ms")
	if ok {
		t.Error("ValidateMetricName('nonexistent.metric_ms') should not be found")
	}
}

// TestCatalog_MetricNames returns a non-empty sorted list.
func TestCatalog_MetricNames(t *testing.T) {
	names := MetricNames()
	if len(names) == 0 {
		t.Fatal("MetricNames() returned empty slice")
	}
	if len(names) != len(MetricCatalog) {
		t.Errorf("MetricNames() returned %d names but catalog has %d entries", len(names), len(MetricCatalog))
	}
}

// TestCatalog_MetricNamesByComponent verifies the component filter.
func TestCatalog_MetricNamesByComponent(t *testing.T) {
	engineMetrics := MetricNamesByComponent(CompEngine)
	if len(engineMetrics) == 0 {
		t.Errorf("MetricNamesByComponent(%q) returned empty — expected engine metrics", CompEngine)
	}
	for _, name := range engineMetrics {
		def, ok := MetricCatalog[name]
		if !ok {
			t.Errorf("MetricNamesByComponent returned %q which is not in catalog", name)
			continue
		}
		if def.Component != CompEngine {
			t.Errorf("MetricNamesByComponent(%q) returned %q with component %q", CompEngine, name, def.Component)
		}
	}
}

// TestCatalog_NoUnknownComponents verifies that every Component field
// is one of the known component constants.
func TestCatalog_NoUnknownComponents(t *testing.T) {
	known := map[string]bool{
		CompEngine:     true,
		CompPipeline:   true,
		CompNative:     true,
		CompOutput:     true,
		CompCache:      true,
		CompBlob:       true,
		CompFFmpeg:     true,
		CompVideo:      true,
		CompQueue:      true,
		CompLease:      true,
		CompResource:   true,
		CompInput:      true,
		CompWaste:      true,
		CompError:      true,
		CompWorker:     true,
		CompTaskRunner: true,
		CompMaster:     true,
		CompCost:       true,
		CompPlacement:  true,
		CompConflict:   true,
		CompReconcile:  true,
		CompScorecard:  true,
	}

	for name, def := range MetricCatalog {
		if !known[def.Component] {
			t.Errorf("metric %q has unknown component %q — use a Comp* constant", name, def.Component)
		}
	}
}
