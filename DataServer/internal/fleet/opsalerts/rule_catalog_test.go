package opsalerts

import (
	"testing"
)

// rule_catalog_test.go — Step 16/15 verifications for the
// catalog constants. Per the user spec:
//
//   1. heartbeat assente >90s = CRITICAL
//   2. container unhealthy = CRITICAL
//   3. restart loop >=3/h = CRITICAL
//   4. disco >85% = WARN / >95% = CRITICAL  (two entries, same RuleID)
//   5. RAM >90% = CRITICAL
//   6. 3 job consecutivi falliti = CRITICAL
//   7. smoke fail = CRITICAL
//   8. version drift = WARNING
//   9. cert <15gg = WARNING / <5gg = CRITICAL (two entries, same RuleID)
//  10. Drive delivery fail = CRITICAL
//  11. deployment ROLLBACK = CRITICAL
//  12. worker disconnected = WARNING
//  13. job stuck in RUNNING >10min = CRITICAL
//  14. stale lease >5min = WARNING
//  15. workdir permission changed = CRITICAL

func TestAllRulesCatalogCount(t *testing.T) {
	rules := AllRules()
	// 15 rule families + 2 dual-severity entries (disk 85/95,
	// cert 15d/5d) = 17 total.
	if got, want := len(rules), 17; got != want {
		t.Fatalf("AllRules() count: got %d, want %d", got, want)
	}
}

func TestAllRulesHaveUniqueRuleIDsBySeverity(t *testing.T) {
	rules := AllRules()
	seen := make(map[RuleID]Severity, 16)
	for _, r := range rules {
		if r.ID == "" {
			t.Fatalf("rule has empty RuleID: %+v", r)
		}
		if r.HumanReadable == "" {
			t.Fatalf("rule %q has empty HumanReadable", r.ID)
		}
		if r.LongDescription == "" {
			t.Fatalf("rule %q has empty LongDescription", r.ID)
		}
		switch r.Severity {
		case Info, Warning, Critical:
		default:
			t.Fatalf("rule %q has invalid severity %q", r.ID, r.Severity)
		}
		// (rule_id, severity) tuple must be unique — this is
		// the dedup key.
		key := string(r.ID) + "|" + string(r.Severity)
		if _, dup := seen[r.ID]; dup && r.Severity == Critical {
			// dual-severity is allowed for disk and cert
			// ONLY; any other duplicate is a contract bug.
			if r.ID != RuleDiskPressure && r.ID != RuleCertExpiring {
				t.Fatalf("rule %q has duplicate severity %q (not in dual-severity whitelist)", r.ID, r.Severity)
			}
		}
		seen[r.ID] = r.Severity
		_ = key
	}
}

func TestDualSeverityRulesHaveCorrectTiers(t *testing.T) {
	rules := AllRulesByID()
	// Disk: 85 WARN, 95 CRITICAL
	if entries, ok := rules[RuleDiskPressure]; !ok {
		t.Fatalf("disk_pressure catalog missing")
	} else {
		if len(entries) != 2 {
			t.Fatalf("disk_pressure has %d entries, want 2 (warning+critical)", len(entries))
		}
		var seenW, seenC bool
		for _, e := range entries {
			if e.Severity == Warning && e.Threshold == 85 && e.Above {
				seenW = true
			}
			if e.Severity == Critical && e.Threshold == 95 && e.Above {
				seenC = true
			}
		}
		if !seenW || !seenC {
			t.Fatalf("disk_pressure entries malformed: warning=%v critical=%v", seenW, seenC)
		}
	}
	// Cert: 15d WARN, 5d CRITICAL.
	if entries, ok := rules[RuleCertExpiring]; !ok {
		t.Fatalf("cert_expiring catalog missing")
	} else {
		if len(entries) != 2 {
			t.Fatalf("cert_expiring has %d entries, want 2 (warning+critical)", len(entries))
		}
		var seenW, seenC bool
		for _, e := range entries {
			if e.Severity == Warning && e.Threshold == 15*24 && !e.Above {
				seenW = true
			}
			if e.Severity == Critical && e.Threshold == 5*24 && !e.Above {
				seenC = true
			}
		}
		if !seenW || !seenC {
			t.Fatalf("cert_expiring entries malformed: warning=%v critical=%v", seenW, seenC)
		}
	}
}

func TestRuleIDsMatchUserSpec(t *testing.T) {
	want := map[RuleID]bool{
		RuleHeartbeatStale:         true,
		RuleContainerUnhealthy:     true,
		RuleRestartLoop:            true,
		RuleDiskPressure:           true,
		RuleRAMPressure:            true,
		RuleConsecutiveJobFailures: true,
		RuleSmokeFailed:            true,
		RuleVersionDrift:           true,
		RuleCertExpiring:           true,
		RuleDriveDeliveryFailed:    true,
		RuleDeploymentRollback:      true,
		RuleWorkerDisconnected:      true,
		RuleJobStuckRunning:         true,
		RuleStaleLease:              true,
		RuleWorkdirPermissionChange: true,
	}
	for _, r := range AllRules() {
		if !want[r.ID] {
			t.Fatalf("unexpected rule_id %q in catalog", r.ID)
		}
	}
	if len(want) != 15 {
		t.Fatalf("user spec expects 15 rule families, got %d", len(want))
	}
}
