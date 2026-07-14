// Package metrics / catalog.go
//
// MetricCatalog is the central, single-source-of-truth registry for every
// canonical metric name Velox emits. No other package may invent metric
// names — all producers (workers, pipeline runners, engine sidecar, master
// supervisors) MUST use names from this catalog. The registry is enforced
// at registration time: unknown names are rejected.
//
// Adding a new metric:
//  1. Add an entry to the appropriate catalog_<family>.go file with a unique key.
//  2. Run tests — TestCatalog_NoDuplicateNames catches collisions.
//  3. Add the validation test in TestCatalog_RequiredMetricsExist if the metric
//     is part of the critical-path surface (dashboards, alerts, control room).
//  4. Update docs/metrics-catalog.md.
//
// Naming convention:
//   - Lowercase, dot-separated: <component>.<metric>[_unit]
//   - Unit suffix is part of the name (e.g. _ms, _bytes, _ratio)
//   - Components: engine, pipeline, native, output, cache, blob, ffmpeg,
//     video, queue, lease, resource, input, waste, error, worker, taskrunner
//
// The catalog is split by logical family across several files
// (catalog_engine.go, catalog_pipeline.go, catalog_media.go, etc.) but is
// always assembled into the single MetricCatalog map by registerMetricFamily
// at package-init time. Do not bypass the assembler — every entry MUST
// flow through this central registry so that TestCatalog_* invariants hold.
package metrics

import "sort"

// MetricDefinition is the canonical descriptor for one metric name.
// Every metric Velox emits MUST have a corresponding entry in MetricCatalog.
type MetricDefinition struct {
	// Name is the canonical dotted-key name (e.g. "engine.segment_build_ms").
	Name string
	// Unit is the SI-suffixed unit (ms, bytes, ratio, count, fps, seconds, items, tracks).
	Unit string
	// Component is the subsystem that produces this metric (engine, pipeline, native, etc.).
	Component string
	// Description is a human-readable explanation of what this metric measures.
	Description string
	// Kind indicates the metric type for the catalog consumer (counter, gauge, histogram).
	Kind CatalogMetricKind
}

// CatalogMetricKind mirrors the Prometheus family type for catalog
// consumers that need to know if a metric is cumulative or instantaneous.
type CatalogMetricKind string

const (
	KindCounter   CatalogMetricKind = "counter"
	KindGauge     CatalogMetricKind = "gauge"
	KindHistogram CatalogMetricKind = "histogram"
)

// Component constants for the MetricCatalog entries.
const (
	CompEngine     = "engine"
	CompPipeline   = "pipeline"
	CompNative     = "native"
	CompOutput     = "output"
	CompCache      = "cache"
	CompBlob       = "blob"
	CompFFmpeg     = "ffmpeg"
	CompVideo      = "video"
	CompQueue      = "queue"
	CompLease      = "lease"
	CompResource   = "resource"
	CompInput      = "input"
	CompWaste      = "waste"
	CompError      = "error"
	CompWorker     = "worker"
	CompTaskRunner = "taskrunner"
	CompMaster     = "master"
	CompCost       = "cost"
	CompPlacement  = "placement"
	CompConflict   = "conflict"
	CompReconcile  = "reconcile"
	CompScorecard  = "scorecard"
)

// MetricCatalog is the central registry of every canonical metric name.
// The key is the dotted metric name; the value is its full definition.
// Tests verify no duplicates and that required families are present.
//
// The map is declared empty here and populated by the init() assembler
// from per-family *MetricDefinition() functions. Do NOT populate this
// map directly from per-family files — that breaks the single-assembly
// invariant that TestCatalog_NoDuplicateNames and the other catalog
// tests rely on.
var MetricCatalog = map[string]MetricDefinition{}

// init assembles the central MetricCatalog by calling every per-family
// assembler in dependency order. The order is not semantically meaningful
// (the map is a set) but is kept grouped: engine/pipeline/media first
// (the rendering hot path), then storage/output, then system resources,
// then scheduling/outcomes/economics.
//
// To add a new metric family, create a new catalog_<family>.go file
// exporting a `func <family>MetricDefinitions() []MetricDefinition`
// and add it to the call list below.
func init() {
	registerMetricFamily(
		engineMetricDefinitions(),
		pipelineMetricDefinitions(),
		mediaMetricDefinitions(),
		outputMetricDefinitions(),
		cacheMetricDefinitions(),
		inputMetricDefinitions(),
		resourcesMetricDefinitions(),
		schedulingMetricDefinitions(),
		errorMetricDefinitions(),
		outcomesMetricDefinitions(),
		economicsMetricDefinitions(),
	)
}

// registerMetricFamily folds every MetricDefinition returned by the
// provided assemblers into the central MetricCatalog map. If two
// families declare the same metric name, the later family wins and
// TestCatalog_NoDuplicateNames catches the regression.
func registerMetricFamily(families ...[]MetricDefinition) {
	for _, family := range families {
		for _, def := range family {
			MetricCatalog[def.Name] = def
		}
	}
}

// ValidateMetricName reports whether name is a valid entry in the catalog.
// Returns the definition and true if found, zero-value and false otherwise.
func ValidateMetricName(name string) (MetricDefinition, bool) {
	def, ok := MetricCatalog[name]
	return def, ok
}

// MetricNames returns all registered metric names in sorted order.
// Useful for iteration, documentation generation, and validation.
func MetricNames() []string {
	names := make([]string, 0, len(MetricCatalog))
	for name := range MetricCatalog {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// MetricNamesByComponent returns all metric names belonging to the given component.
// Results are sorted for deterministic iteration.
func MetricNamesByComponent(component string) []string {
	var names []string
	for name, def := range MetricCatalog {
		if def.Component == component {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}
