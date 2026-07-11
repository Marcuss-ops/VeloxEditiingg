package metrics

import (
	"os"
	"testing"
)

// TestDefaultCostFactors asserts the Hetzner CCX defaults.
// Operators tune these via env vars; deviating from the documented
// baselines should be an EXPLICIT decision (PR comment + alert).
func TestDefaultCostFactors(t *testing.T) {
	f := DefaultCostFactors()
	if f.CPUCoreSecondEUR != 5e-6 {
		t.Errorf("CPUCoreSecondEUR default = %v; want 5e-6 (Hetzner CCX snapshot)", f.CPUCoreSecondEUR)
	}
	if f.NetworkGBEUR != 0.01 {
		t.Errorf("NetworkGBEUR default = %v; want 0.01 (Hetzner egress rate)", f.NetworkGBEUR)
	}
	if f.StorageGBEUR != 0.00012 {
		t.Errorf("StorageGBEUR default = %v; want 0.00012 (per-GB-per-month amortised)", f.StorageGBEUR)
	}
}

// TestLoadCostFactorsFromEnv_DefaultsWhenAbsent: no env vars set
// → defaults hit. This is the canonical boot path on a fresh
// deploy.
func TestLoadCostFactorsFromEnv_DefaultsWhenAbsent(t *testing.T) {
	t.Setenv("VELOX_CPU_CORE_SECOND_COST", "")
	t.Setenv("VELOX_NETWORK_GB_COST", "")
	t.Setenv("VELOX_STORAGE_GB_COST", "")
	f := LoadCostFactorsFromEnv()
	if f.CPUCoreSecondEUR != 5e-6 || f.NetworkGBEUR != 0.01 || f.StorageGBEUR != 0.00012 {
		t.Errorf("LoadCostFactorsFromEnv with no env vars: cpu=%v net=%v sto=%v; want defaults",
			f.CPUCoreSecondEUR, f.NetworkGBEUR, f.StorageGBEUR)
	}
}

// TestLoadCostFactorsFromEnv_Overrides: env vars override defaults.
// Critical for live tuning during incidents ("landlord, my egress
// went up, bump VELOX_NETWORK_GB_COST").
func TestLoadCostFactorsFromEnv_Overrides(t *testing.T) {
	t.Setenv("VELOX_CPU_CORE_SECOND_COST", "0.000007")
	t.Setenv("VELOX_NETWORK_GB_COST", "0.012")
	t.Setenv("VELOX_STORAGE_GB_COST", "0.0005")
	f := LoadCostFactorsFromEnv()
	if f.CPUCoreSecondEUR != 0.000007 {
		t.Errorf("CPUCoreSecondEUR override = %v; want 0.000007", f.CPUCoreSecondEUR)
	}
	if f.NetworkGBEUR != 0.012 {
		t.Errorf("NetworkGBEUR override = %v; want 0.012", f.NetworkGBEUR)
	}
	if f.StorageGBEUR != 0.0005 {
		t.Errorf("StorageGBEUR override = %v; want 0.0005", f.StorageGBEUR)
	}
}

// TestLoadCostFactorsFromEnv_NegativeClampedToDefault: a typo'd
// negative env value MUST fall back to defaults, NOT emit a
// negative cost-per-minute gauge (negative Prometheus values
// break dashboard math).
func TestLoadCostFactorsFromEnv_NegativeClampedToDefault(t *testing.T) {
	t.Setenv("VELOX_CPU_CORE_SECOND_COST", "-0.5")
	t.Setenv("VELOX_NETWORK_GB_COST", "")
	t.Setenv("VELOX_STORAGE_GB_COST", "")
	f := LoadCostFactorsFromEnv()
	if f.CPUCoreSecondEUR != 5e-6 {
		t.Errorf("Negative CPU override should fall back to default; got %v", f.CPUCoreSecondEUR)
	}
}

// TestLoadCostFactorsFromEnv_GarbageFallsBack: garbage env values
// must NOT crash the master at boot — a typo in deployment is a
// recoverable mistake, not a hard fail.
func TestLoadCostFactorsFromEnv_GarbageFallsBack(t *testing.T) {
	os.Setenv("VELOX_CPU_CORE_SECOND_COST", "not-a-number")
	defer os.Unsetenv("VELOX_CPU_CORE_SECOND_COST")
	os.Setenv("VELOX_NETWORK_GB_COST", "")
	os.Setenv("VELOX_STORAGE_GB_COST", "")
	f := LoadCostFactorsFromEnv()
	if f.CPUCoreSecondEUR != 5e-6 {
		t.Errorf("Garbage CPU override should fall back to default; got %v", f.CPUCoreSecondEUR)
	}
}

// TestCostPerOutputMinute_ZeroFloor: a 0.001-minute output floor
// prevents div-by-0 on attempts that emit wall_clock but no
// media_duration (FAIL paths).
func TestCostPerOutputMinute_ZeroFloor(t *testing.T) {
	f := DefaultCostFactors()
	if got := f.CostPerOutputMinute(120, 0.5, 0.1, 0); got != 0 {
		t.Errorf("0 output_minutes should yield 0; got %v", got)
	}
	if got := f.CostPerOutputMinute(120, 0.5, 0.1, 0.0005); got != 0 {
		t.Errorf("sub-0.001 output_minutes should yield 0 (zero floor); got %v", got)
	}
	// Realistic: 120s CPU, 0.5GB network, 0.1GB storage, 60s output.
	// Expected = (120*5e-6 + 0.5*0.01 + 0.1*0.00012) / 1
	//         = 0.0006 + 0.005 + 0.000012 = 0.005612
	want := 0.005612
	if got := f.CostPerOutputMinute(120, 0.5, 0.1, 1.0); absDiff(got, want) > 1e-6 {
		t.Errorf("CostPerOutputMinute(120, 0.5, 0.1, 1) = %v; want %v", got, want)
	}
}

// TestCostPerOutputMinute_Components: the sum of three component
// helpers equals the total. Float math so we use absDiff with
// ULP-level tolerance (1e-9) — bit-exact equality fails on
// subtraction then re-addition across multiple terms.
func TestCostPerOutputMinute_Components(t *testing.T) {
	f := DefaultCostFactors()
	cpu, net, sto, mins := 30.0, 0.2, 0.05, 0.5
	gotTotal := f.CostPerOutputMinute(cpu, net, sto, mins)
	cpuPart := f.CPUPerOutputMinute(cpu, mins)
	netPart := f.NetworkPerOutputMinute(net, mins)
	stoPart := f.StoragePerOutputMinute(sto, mins)
	sumParts := cpuPart + netPart + stoPart
	if absDiff(gotTotal, sumParts) > 1e-9 {
		t.Errorf("component breakdown: total=%v sum(parts)=%v (CPU=%.9f Network=%.9f Storage=%.9f)", gotTotal, sumParts, cpuPart, netPart, stoPart)
	}
}

// absDiff is a tiny ε helper for float comparison (no imports).
func absDiff(a, b float64) float64 {
	d := a - b
	if d < 0 {
		return -d
	}
	return d
}
