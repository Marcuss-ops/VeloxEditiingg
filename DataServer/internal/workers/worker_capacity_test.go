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

func TestDeriveWorkerCapacityWithPhaseScores(t *testing.T) {
	cap := deriveWorkerCapacityWithPhaseScores(
		4, 2, // flat: max=4, active=2
		3, 1, // render: slots=3, active=1
		4, 2, // prefetch: slots=4, active=2
		2, 2, // publisher: slots=2, active=2 (full)
		"NETWORK",
	)
	if cap.MaxSlots != 4 || cap.ActiveSlots != 2 || cap.AvailableSlots != 2 {
		t.Fatalf("flat capacity = %v, want max=4 active=2 available=2", cap)
	}
	if cap.RenderSlots != 3 || cap.ActiveRender != 1 {
		t.Fatalf("render = %d/%d, want 3/1", cap.RenderSlots, cap.ActiveRender)
	}
	if cap.PrefetchSlots != 4 || cap.ActivePrefetch != 2 {
		t.Fatalf("prefetch = %d/%d, want 4/2", cap.PrefetchSlots, cap.ActivePrefetch)
	}
	if cap.PublisherSlots != 2 || cap.ActivePublisher != 2 {
		t.Fatalf("publisher = %d/%d, want 2/2", cap.PublisherSlots, cap.ActivePublisher)
	}
	if cap.LimitingResource != "NETWORK" {
		t.Fatalf("limiting = %q, want NETWORK", cap.LimitingResource)
	}
}

func TestAvailableRenderSlots_WithPerPhaseLimits(t *testing.T) {
	cap := WorkerCapacity{
		MaxSlots: 10, ActiveSlots: 2, AvailableSlots: 8,
		RenderSlots: 3, ActiveRender: 1,
	}
	if got := cap.AvailableRenderSlots(); got != 2 {
		t.Fatalf("AvailableRenderSlots = %d, want 2", got)
	}
}

func TestAvailableRenderSlots_FlatFallback(t *testing.T) {
	cap := WorkerCapacity{
		MaxSlots: 4, ActiveSlots: 1, AvailableSlots: 3,
	}
	if got := cap.AvailableRenderSlots(); got != 3 {
		t.Fatalf("AvailableRenderSlots = %d, want 3 (flat fallback)", got)
	}
}

func TestAvailablePrefetchSlots_WithPerPhaseLimits(t *testing.T) {
	cap := WorkerCapacity{
		MaxSlots: 10, ActiveSlots: 2, AvailableSlots: 8,
		PrefetchSlots: 5, ActivePrefetch: 4,
	}
	if got := cap.AvailablePrefetchSlots(); got != 1 {
		t.Fatalf("AvailablePrefetchSlots = %d, want 1", got)
	}
}

func TestAvailablePublisherSlots_Full(t *testing.T) {
	cap := WorkerCapacity{
		MaxSlots: 10, ActiveSlots: 2, AvailableSlots: 8,
		PublisherSlots: 2, ActivePublisher: 2,
	}
	if got := cap.AvailablePublisherSlots(); got != 0 {
		t.Fatalf("AvailablePublisherSlots = %d, want 0", got)
	}
}

func TestAvailablePublisherSlots_NegativeClamps(t *testing.T) {
	cap := WorkerCapacity{
		MaxSlots: 10, ActiveSlots: 2, AvailableSlots: 8,
		PublisherSlots: 2, ActivePublisher: 3, // over-committed
	}
	if got := cap.AvailablePublisherSlots(); got != 0 {
		t.Fatalf("AvailablePublisherSlots = %d, want 0 (clamped)", got)
	}
}
