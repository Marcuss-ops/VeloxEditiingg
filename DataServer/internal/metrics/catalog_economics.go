// Package metrics / catalog_economics.go
//
// Economics family — cost attribution and waste accounting:
//   - cost.*  : cost per output minute by worker class (CPU, network,
//     storage, total).
//   - waste.* : wasted compute / download / cost from retries and
//     failed attempts.
package metrics

// economicsMetricDefinitions returns cost.* + waste.* definitions.
// Cost first (per-minute attribution), then waste (aggregate waste
// from retries).
func economicsMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Cost metrics ─────────────────────────────────────────────────
		{
			Name: "cost.cpu_core_seconds_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
			Description: "CPU cost per output minute by worker class",
		},
		{
			Name: "cost.network_gb_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
			Description: "Network egress cost per output minute by worker class",
		},
		{
			Name: "cost.storage_gb_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
			Description: "Storage cost per output minute by worker class",
		},
		{
			Name: "cost.total_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
			Description: "Total cost per output minute by worker class",
		},
		// ── Waste / cost attribution ─────────────────────────────────────
		{
			Name: "waste.retry_count", Unit: "count", Component: CompWaste, Kind: KindCounter,
			Description: "Number of retries for this task (wasted attempts)",
		},
		{
			Name: "waste.wasted_cpu_ms", Unit: "ms", Component: CompWaste, Kind: KindCounter,
			Description: "CPU time wasted on failed/retried attempts",
		},
		{
			Name: "waste.wasted_download_bytes", Unit: "bytes", Component: CompWaste, Kind: KindCounter,
			Description: "Download bytes wasted on failed/retried attempts",
		},
		{
			Name: "waste.wasted_cost_estimate", Unit: "eur", Component: CompWaste, Kind: KindCounter,
			Description: "Estimated cost in EUR of wasted compute resources",
		},
	}
}
