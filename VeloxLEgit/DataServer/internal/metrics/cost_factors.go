// Package metrics / cost_factors.go
//
// SPEC §14 follow-up (cost metrics wiring). Loads the per-EU cost
// factors from VELOX_* env vars and exposes a typed `CostFactors`
// struct with a default constructor `LoadCostFactorsFromEnv` used by
// the supervisor on each tick.
//
// Defaults calibrated to Hetzner Cloud CCX pricing snapshot:
//   - €0.000005 per CPU·core·second
//   - €0.01 per GB of network egress
//   - €0.00012 per GB of object-storage written (per-GB-per-month
//     amortised — operators MUST tune for their actual storage
//     provider; the raw rate is intentionally small so a fleet
//     running 24/7 doesn't surface absurd cost figures in tests)
//
// Cardinality contract: only `worker_class` is a label — the 4
// per-output-minute gauges MUST stay single-label so the on-master
// aggregator does NOT explode. `project_id` was rejected (UNSAFE; see
// metrics.go/cardinality discipline).
//
// Mathematical caveat (operational discipline, NOT a bug):
//
//	`velox_cost_*_per_output_minute` is emitted as a Gauge — a
//	ratio of cost ÷ output_minutes. PromQL aggregates such gauges
//	by averaging averages (which mathematically double-counts the
//	weight of attempts with low output_minutes). The operational
//	guidance is:
//
//	  * alert on `velox_cost_total_per_output_minute` per worker_class
//	    using `last()` or `max_over_time(window)` — NOT `avg_over_time`.
//	  * for fleet-wide cost panels, divide sum(cost) by sum(output_minutes)
//	    in a recording rule — do not rely on aggregating the per-minute
//	    gauge.
//
//	This trade-off was explicit in the spec §14 follow-up: maintainers
//	need to be able to read a single instant "what is /min on this
//	worker class" without joining queries. A future spec iteration
//	might migrate to a Counter+rate() pair if the panel math gets
//	painful.
package metrics

import (
	"os"
	"strconv"
)

// CostFactors is the typed container for the per-EU env-loaded cost
// rates. The collector + supervisor consume it through methods (NOT
// field reads) so callers don't accidentally forget the zero-output
// guard.
type CostFactors struct {
	// CPUCoreSecondEUR is the cost of running 1 CPU·core for 1
	// second. Worker workers cost this much CPU-time in a render
	// session.
	CPUCoreSecondEUR float64
	// NetworkGBEUR is the cost of egressing 1 GB of data from
	// the worker's hosting provider to the master (or downstream
	// CDN). The spec wires this to `attempt.NetworkGBEgressed`
	// once the worker surfaces it on the typed TaskExecutionMetrics
	// (currently 0; pre-PR-3 follow-up — see F2 file-header
	// comment).
	NetworkGBEUR float64
	// StorageGBEUR is the per-GB cost of storing the temp + output
	// artefacts the render produced. Hooked to attempt storage
	// written (TempBytesWritten) for the temp component; the
	// output component is operator-configurable in a later PR.
	StorageGBEUR float64
}

// DefaultCostFactors returns the Hetzner CCX defaults. A exported
// const-style constructor so tests can reference a stable baseline.
func DefaultCostFactors() CostFactors {
	return CostFactors{
		CPUCoreSecondEUR: 5e-6,    // €0.000005 per core·s
		NetworkGBEUR:     0.01,    // €0.01 per GB egress
		StorageGBEUR:     0.00012, // €0.00012 per GB·month amortised
	}
}

// LoadCostFactorsFromEnv reads VELOX_CPU_CORE_SECOND_COST,
// VELOX_NETWORK_GB_COST, VELOX_STORAGE_GB_COST and falls back to
// DefaultCostFactors on absence OR on parse failure (parse failure
// MUST never crash the master — operators can introduce a typo in
// deployment and silently fall back to defaults is preferable to
// hard-fail at boot).
//
// Negative numbers are clamped to 0 so a misconfigured env var
// can't produce a negative cost-per-minute gauge (Prometheus rate()
// math on negative values is undefined and dashboards break).
func LoadCostFactorsFromEnv() CostFactors {
	out := DefaultCostFactors()
	if v := envFloatOrZero("VELOX_CPU_CORE_SECOND_COST"); v > 0 {
		out.CPUCoreSecondEUR = v
	}
	if v := envFloatOrZero("VELOX_NETWORK_GB_COST"); v > 0 {
		out.NetworkGBEUR = v
	}
	if v := envFloatOrZero("VELOX_STORAGE_GB_COST"); v > 0 {
		out.StorageGBEUR = v
	}
	return out
}

// envFloatOrZero is a tiny env-read primitive. Returns 0 (NOT the
// default) so callers can detect absence vs the floor of env=0.
func envFloatOrZero(name string) float64 {
	v := os.Getenv(name)
	if v == "" {
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	if f < 0 {
		return 0
	}
	return f
}

// CostPerOutputMinute computes the cost per output minute for a
// single attempt's totals. Returns 0 if output_minutes <= 0.001.
//
// The output_minutes floor (0.001 minute ≈ 60ms) prevents div-by-0
// on attempts that report wall_clock but no media duration (FAIL
// paths emit wall_clock_seconds but not media_duration_seconds
// yet; pending the typed-metrics cutover follow-up).
//
// This is the SAME formula as taskattempts.AttemptCostBasis.Compute()
// but applied to env-loaded rates instead of per-attempt rates, so
// the supervisor can aggregate across attempts by class without
// carrying the per-attempt price snapshot.
func (f CostFactors) CostPerOutputMinute(
	cpuSecondsTotal, networkGB, storageGB, outputMinutesTotal float64,
) float64 {
	if outputMinutesTotal <= 0.001 {
		return 0
	}
	cpu := cpuSecondsTotal * f.CPUCoreSecondEUR
	net := networkGB * f.NetworkGBEUR
	sto := storageGB * f.StorageGBEUR
	return (cpu + net + sto) / outputMinutesTotal
}

// CPUPerOutputMinute is the CPU-only sub-component so dashboards
// can isolate which dimension is driving a cost spike.
func (f CostFactors) CPUPerOutputMinute(cpuSecondsTotal, outputMinutesTotal float64) float64 {
	if outputMinutesTotal <= 0.001 {
		return 0
	}
	return (cpuSecondsTotal * f.CPUCoreSecondEUR) / outputMinutesTotal
}

// NetworkPerOutputMinute is the network egress sub-component.
func (f CostFactors) NetworkPerOutputMinute(networkGB, outputMinutesTotal float64) float64 {
	if outputMinutesTotal <= 0.001 {
		return 0
	}
	return (networkGB * f.NetworkGBEUR) / outputMinutesTotal
}

// StoragePerOutputMinute is the storage sub-component (temp +
// output — currently collapsed to temp_bytes_written as a 1st
// order proxy).
func (f CostFactors) StoragePerOutputMinute(storageGB, outputMinutesTotal float64) float64 {
	if outputMinutesTotal <= 0.001 {
		return 0
	}
	return (storageGB * f.StorageGBEUR) / outputMinutesTotal
}
