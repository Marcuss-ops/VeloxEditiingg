// Package metrics / collector_costs.go
//
// Cost-per-output-minute (spec §14 follow-up) stamping + waste
// counters, sliced out of collector.go so the Collector struct
// definition stays focused on registration. Micro-EUR encoding +
// the Scorecard v2 / Step 17 waste counters live here.
//
// Cost-per-output-minute is a per-tick aggregation stamped under
// `worker_class`. The supervisor (supervisor.go) calls
// RecordAggregateCost once per tick after summing
// (cpuSeconds, networkGB, storageGB, outputMinutes) for newly-
// terminal attempts.
//
// RecordWaste (Scorecard v2 / Step 17) increments velox_waste_total
// for a single waste_type label. Caller-supplied; the schema
// (retry_count | wasted_cpu_ms | wasted_download_bytes |
// wasted_cost_estimate) lives in the catalog and is enforced by
// tests.
package metrics

// RecordAggregateCost stamps the 4 cost-per-output-minute gauges for
// one worker class. Called by the supervisor once per tick after the
// per-class aggregation (sum of cost components / sum of output
// minutes for newly-terminal attempts on that class) is computed.
//
// Micro-EUR encoding (×1_000_000) so a fraction fits inside the int64
// gauge — exposition is plain decimals. Pass output_minutes < 0.001
// to skip all 4 stamps (zero safety matches the typed AttemptCostBasis
// guards in taskattempts/report.go).
//
// The supervisor aggregates per tick, NOT incrementally, so this is
// a GaugeSet (last-write-wins per tick) — see cost_factors.go for
// the math caveat on averaging these gauges across time.
func (c *Collector) RecordAggregateCost(
	workerClass string,
	cpuSeconds, networkGB, storageGB, outputMinutes float64,
	f CostFactors,
) {
	if workerClass == "" {
		workerClass = "default"
	}
	if outputMinutes < 0.001 {
		return
	}
	wl := []string{workerClass}
	c.costCpuPerMin.GaugeSet(wl, encodeMicroEUR(f.CPUPerOutputMinute(cpuSeconds, outputMinutes)))
	c.costNetworkPerMin.GaugeSet(wl, encodeMicroEUR(f.NetworkPerOutputMinute(networkGB, outputMinutes)))
	c.costStoragePerMin.GaugeSet(wl, encodeMicroEUR(f.StoragePerOutputMinute(storageGB, outputMinutes)))
	c.costTotalPerMin.GaugeSet(wl, encodeMicroEUR(f.CostPerOutputMinute(cpuSeconds, networkGB, storageGB, outputMinutes)))
}

// encodeMicroEUR encodes a float64 EUR value as int64 micro-EUR.
// Negative values clamp to zero so a misconfigured env var (or a
// future cost-model bug) cannot emit negative gauge readings.
func encodeMicroEUR(eur float64) int64 {
	if eur <= 0 {
		return 0
	}
	return int64(eur * 1_000_000)
}

// ── Waste/cost metrics (Scorecard v2 / Step 17) ──────────────────────────

// RecordWaste increments velox_waste_total for a single waste type.
// wasteType is one of: "retry_count", "wasted_cpu_ms",
// "wasted_download_bytes", "wasted_cost_estimate".
// value is the absolute value to increment (counter).
func (c *Collector) RecordWaste(wasteType string, value uint64) {
	c.wasteTotal.Inc([]string{wasteType}, value)
}

// initCostFamilies creates the cost-per-output-minute gauges + the
// waste counter recorded by RecordAggregateCost / RecordWaste. Called
// once from NewCollector at boot.
func (c *Collector) initCostFamilies() {
	costLabels := []string{"worker_class"}
	c.costCpuPerMin = NewGaugeFamily("velox_cost_cpu_core_seconds_per_output_minute",
		"CPU cost per output minute (€ × 1e6) by worker class",
		costLabels)
	c.costNetworkPerMin = NewGaugeFamily("velox_cost_network_gb_per_output_minute",
		"Network egress cost per output minute (€ × 1e6) by worker class",
		costLabels)
	c.costStoragePerMin = NewGaugeFamily("velox_cost_storage_gb_written_per_output_minute",
		"Storage cost per output minute (€ × 1e6) by worker class",
		costLabels)
	c.costTotalPerMin = NewGaugeFamily("velox_cost_total_per_output_minute",
		"Total cost per output minute (€ × 1e6) by worker class",
		costLabels)
	c.wasteTotal = NewCounterFamily(
		"velox_waste_total",
		"Waste/cost totals by type (retry_count, wasted_cpu_ms, wasted_download_bytes, wasted_cost_estimate)",
		[]string{"waste_type"},
	)
}

// costFamilies returns the cost/waste subset registered by
// NewCollector via allFamilies.
func (c *Collector) costFamilies() []*Family {
	return []*Family{
		c.costCpuPerMin, c.costNetworkPerMin, c.costStoragePerMin, c.costTotalPerMin,
		c.wasteTotal,
	}
}
