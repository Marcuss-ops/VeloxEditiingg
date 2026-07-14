// Package metrics / catalog_error.go
//
// Error family — error classification metrics. Every error emitted by
// any component is bucketed by:
//   - error.component : the component that produced the error.
//   - error.phase      : the canonical phase the error occurred in.
//   - error.retryable  : whether the error is classified as retryable.
//   - error.message_hash: stable hash of the message for dedup.
package metrics

// errorMetricDefinitions returns error.* definitions in stable order:
// where/when (component, phase), then classification (retryable,
// message_hash).
func errorMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		{
			Name: "error.component", Unit: "string", Component: CompError, Kind: KindCounter,
			Description: "Component where the error originated (engine, pipeline, cache, etc.)",
		},
		{
			Name: "error.phase", Unit: "string", Component: CompError, Kind: KindCounter,
			Description: "Canonical phase where the error occurred",
		},
		{
			Name: "error.retryable", Unit: "boolean", Component: CompError, Kind: KindGauge,
			Description: "Whether the error is classified as retryable",
		},
		{
			Name: "error.message_hash", Unit: "hash", Component: CompError, Kind: KindCounter,
			Description: "Stable hash of the error message for deduplication",
		},
	}
}
