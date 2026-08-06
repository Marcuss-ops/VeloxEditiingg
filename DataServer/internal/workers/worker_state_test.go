package workers

import (
	"testing"
	"time"
)

func TestDeriveConnectionState(t *testing.T) {
	now := canonicalNow()
	cases := []struct {
		name          string
		sessionActive bool
		lastHB        string
		want          ConnectionState
	}{
		{"fresh session + fresh HB", true, freshHB(now, 30*time.Second), ConnectionConnected},
		{"session dead", false, freshHB(now, 30*time.Second), ConnectionOffline},
		{"empty heartbeat", true, "", ConnectionOffline},
		{"unparseable heartbeat", true, "nope", ConnectionOffline},
		{"heartbeat beyond 5min", true, freshHB(now, 6*time.Minute), ConnectionOffline},
		{"heartbeat in stale window", true, freshHB(now, 3*time.Minute), ConnectionStale},
		{"stale edge 150s", true, freshHB(now, ConnectionStaleThreshold), ConnectionStale},
		{"just under stale edge", true, freshHB(now, ConnectionStaleThreshold-time.Second), ConnectionConnected},
		{"future heartbeat is offline", true, now.Add(time.Minute).Format(time.RFC3339), ConnectionOffline},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeriveConnectionState(tc.sessionActive, tc.lastHB, now); got != tc.want {
				t.Errorf("DeriveConnectionState = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeriveSchedulingState(t *testing.T) {
	cases := []struct {
		name        string
		drain       bool
		quarantined bool
		resuming    bool
		activeTasks int
		want        SchedulingState
	}{
		{"idle", false, false, false, 0, SchedulingAvailable},
		{"busy from tasks", false, false, false, 2, SchedulingBusy},
		{"draining", true, false, false, 0, SchedulingDraining},
		{"resuming wire status remains exclusion", false, false, true, 0, SchedulingResuming},
		{"draining even when busy", true, false, false, 5, SchedulingDraining},
		{"resuming blocks placement", false, false, true, 0, SchedulingResuming},
		{"quarantined beats resuming", false, true, true, 5, SchedulingQuarantined},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeriveSchedulingState(tc.drain, tc.quarantined, tc.resuming, tc.activeTasks); got != tc.want {
				t.Errorf("DeriveSchedulingState = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeriveDeploymentState(t *testing.T) {
	cases := []struct {
		status     string
		isRollback bool
		want       DeploymentState
	}{
		{"PENDING", false, DeploymentUpdating},
		{"PENDING", true, DeploymentRollback},
		{"SUCCEEDED", false, DeploymentCurrent},
		{"FAILED", false, DeploymentFailed},
		{"ROLLED_BACK", false, DeploymentNone},
		{"", false, DeploymentNone},
	}
	for _, tc := range cases {
		t.Run(tc.status+"/"+boolStr(tc.isRollback), func(t *testing.T) {
			if got := DeriveDeploymentState(tc.status, tc.isRollback); got != tc.want {
				t.Errorf("DeriveDeploymentState = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeriveHealthState(t *testing.T) {
	now := canonicalNow()
	cases := []struct {
		name  string
		cs    ConnectionState
		ss    SchedulingState
		ds    DeploymentState
		smoke time.Time
		want  HealthState
	}{
		{"healthy", ConnectionConnected, SchedulingAvailable, DeploymentNone, time.Time{}, HealthHealthy},
		{"busy is healthy", ConnectionConnected, SchedulingBusy, DeploymentNone, time.Time{}, HealthHealthy},
		{"offline is down", ConnectionOffline, SchedulingAvailable, DeploymentNone, time.Time{}, HealthDown},
		{"quarantined is down", ConnectionConnected, SchedulingQuarantined, DeploymentNone, time.Time{}, HealthDown},
		{"failed deploy is degraded", ConnectionConnected, SchedulingAvailable, DeploymentFailed, time.Time{}, HealthDegraded},
		{"updating is degraded", ConnectionConnected, SchedulingAvailable, DeploymentUpdating, time.Time{}, HealthDegraded},
		{"stale is degraded", ConnectionStale, SchedulingAvailable, DeploymentNone, time.Time{}, HealthDegraded},
		{"rollback is degraded", ConnectionConnected, SchedulingAvailable, DeploymentRollback, time.Time{}, HealthDegraded},
		{"recent smoke fail is degraded", ConnectionConnected, SchedulingAvailable, DeploymentNone, now.Add(-30 * time.Minute), HealthDegraded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeriveHealthState(tc.cs, tc.ss, tc.ds, tc.smoke, now); got != tc.want {
				t.Errorf("DeriveHealthState = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsHeartbeatOfflineRejectsFutureHeartbeat(t *testing.T) {
	now := canonicalNow()
	future := now.Add(time.Minute).Format(time.RFC3339)
	if !IsHeartbeatOffline(future, now) {
		t.Fatal("future heartbeat must be treated as offline for eligibility")
	}
	if IsHeartbeatOffline(freshHB(now, 30*time.Second), now) {
		t.Fatal("fresh heartbeat must remain eligible")
	}
}

func TestWireStatusProjection(t *testing.T) {
	cases := []struct {
		cs   ConnectionState
		ss   SchedulingState
		want string
	}{
		{ConnectionConnected, SchedulingAvailable, StatusConnected},
		{ConnectionConnected, SchedulingBusy, StatusConnected},
		{ConnectionStale, SchedulingAvailable, StatusStale},
		{ConnectionOffline, SchedulingAvailable, StatusDisconnected},
		{ConnectionOffline, SchedulingDraining, StatusDraining}, // wire back-compat: drain wins
		{ConnectionStale, SchedulingDraining, StatusDraining},
		{ConnectionConnected, SchedulingResuming, StatusDraining},
	}
	for _, tc := range cases {
		if got := tc.cs.WireStatus(tc.ss); got != tc.want {
			t.Errorf("WireStatus(%q, %q) = %q, want %q", tc.cs, tc.ss, got, tc.want)
		}
	}
}
