package workers

import (
	"testing"
	"time"
)

func TestDeriveWorkerCapacity(t *testing.T) {
	cases := []struct {
		name          string
		max, active   int
		wantAvailable int
	}{
		{"free", 4, 1, 3},
		{"full", 4, 4, 0},
		{"overcommitted clamps", 2, 4, 0},
		{"negative inputs clamp", -1, -2, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveWorkerCapacity(tc.max, tc.active)
			if got.MaxSlots != maxInt(tc.max, 0) || got.ActiveSlots != maxInt(tc.active, 0) || got.AvailableSlots != tc.wantAvailable {
				t.Fatalf("capacity = %#v", got)
			}
			if !got.Authoritative {
				t.Fatal("capacity must be marked authoritative")
			}
		})
	}
}

func TestNonAuthoritativeCapacityNeverAdvertisesAvailability(t *testing.T) {
	got := deriveWorkerCapacityWithAuthority(4, 0, false)
	if got.MaxSlots != 4 || got.ActiveSlots != 0 || got.AvailableSlots != 0 || got.Authoritative {
		t.Fatalf("non-authoritative capacity = %#v, want declared max only and zero availability", got)
	}
}

func TestApplyLeaseCapacityStateUsesLeaseSlots(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	info := &Worker{
		LastHB:           now.Add(-30 * time.Second).Format(time.RFC3339),
		ConnectionState:  ConnectionConnected,
		DeploymentState:  DeploymentNone,
		Metrics:          map[string]interface{}{"active_tasks": float64(0)},
		DeclaredMaxSlots: 4,
	}

	applyLeaseCapacityState(info, 2, now)

	if info.SchedulingState != SchedulingBusy {
		t.Fatalf("SchedulingState = %q, want %q from active lease count", info.SchedulingState, SchedulingBusy)
	}
	if info.Health != WorkerHealthBusy {
		t.Fatalf("Health = %q, want %q from active lease count", info.Health, WorkerHealthBusy)
	}
	if info.HealthState != HealthHealthy {
		t.Fatalf("HealthState = %q, want %q; busy is healthy", info.HealthState, HealthHealthy)
	}
}

func maxInt(v, min int) int {
	if v < min {
		return min
	}
	return v
}
