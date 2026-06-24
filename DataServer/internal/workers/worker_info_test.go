package workers

import (
	"testing"
	"time"
)

func TestExtractStringSlice_Nil(t *testing.T) {
	if got := ExtractStringSlice(nil); got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}

func TestExtractStringSlice_StringSlice(t *testing.T) {
	input := []interface{}{"a", "b", "c"}
	got := ExtractStringSlice(input)
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("want [a b c], got %v", got)
	}
}

func TestExtractStringSlice_Empty(t *testing.T) {
	got := ExtractStringSlice([]interface{}{})
	if got == nil || len(got) != 0 {
		t.Fatalf("want empty slice, got %v", got)
	}
}

func TestExtractStringSlice_MixedTypes(t *testing.T) {
	input := []interface{}{"hello", 42, true}
	got := ExtractStringSlice(input)
	if len(got) != 1 {
		t.Fatalf("want 1 string item (non-strings skipped), got %d: %v", len(got), got)
	}
	if got[0] != "hello" {
		t.Fatalf("want 'hello', got %q", got[0])
	}
}

func TestNormalizeCapabilities(t *testing.T) {
	input := map[string]interface{}{
		"ffmpeg":              true,
		"supported_job_types": []string{"health_check"},
	}
	got := normalizeCapabilities(input)
	if got["ffmpeg"] != true {
		t.Fatalf("want ffmpeg=true, got %v", got["ffmpeg"])
	}
}

func TestNormalizeCapabilities_Nil(t *testing.T) {
	got := normalizeCapabilities(nil)
	if got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}

// ─────────────────────────────────────────────────────────────────────
// ConnectionStatus — pure-function state derivation (no DB / Registry)
// ─────────────────────────────────────────────────────────────────────
//
// Rules (canonical, in evaluation order):
//   1. drain=true           → DRAINING     (overrides freshness)
//   2. !sessionActive       → DISCONNECTED (no valid auth session)
//   3. lastHB empty/unpars. → DISCONNECTED
//   4. age ≥ 5min           → DISCONNECTED
//   5. age ≥ 30s            → STALE
//   6. age <  30s           → CONNECTED
func hbOffset(now time.Time, age time.Duration) string {
	return now.UTC().Add(-age).Format(time.RFC3339)
}

func TestConnectionStatus_DrainOverridesEverything(t *testing.T) {
	now := time.Now().UTC()
	// Drain overrides stale heartbeat too.
	got := ConnectionStatus(true, hbOffset(now, 10*time.Minute), true, now)
	if got != StatusDraining {
		t.Errorf("drain=true should win regardless of staleness; got %s", got)
	}
}

func TestConnectionStatus_NoSession_IsDisconnected(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(false, hbOffset(now, 5*time.Second), false, now)
	if got != StatusDisconnected {
		t.Errorf("sessionInactive + fresh heartbeat should be DISCONNECTED; got %s", got)
	}
}

func TestConnectionStatus_FreshSession_FreshHeartbeat_IsConnected(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(true, hbOffset(now, 5*time.Second), false, now)
	if got != StatusConnected {
		t.Errorf("sessionActive + 5s-old heartbeat should be CONNECTED; got %s", got)
	}
}

func TestConnectionStatus_FreshSession_45sHeartbeat_IsStale(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(true, hbOffset(now, 45*time.Second), false, now)
	if got != StatusStale {
		t.Errorf("sessionActive + 45s-old heartbeat should be STALE (boundary 30s); got %s", got)
	}
}

func TestConnectionStatus_Boundary30s_IsStale(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(true, hbOffset(now, ConnectionStaleThreshold), false, now)
	if got != StatusStale {
		t.Errorf("age == staleThreshold should be STALE; got %s", got)
	}
}

func TestConnectionStatus_BoundaryJustUnder30s_IsConnected(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(true, hbOffset(now, ConnectionStaleThreshold-time.Second), false, now)
	if got != StatusConnected {
		t.Errorf("age just below staleThreshold should be CONNECTED; got %s", got)
	}
}

func TestConnectionStatus_FreshSession_6minHeartbeat_IsDisconnected(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(true, hbOffset(now, 6*time.Minute), false, now)
	if got != StatusDisconnected {
		t.Errorf("sessionActive + 6min-old heartbeat should be DISCONNECTED (boundary 5min); got %s", got)
	}
}

func TestConnectionStatus_EmptyHeartbeat_IsDisconnected(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(true, "", false, now)
	if got != StatusDisconnected {
		t.Errorf("empty heartbeat should be DISCONNECTED; got %s", got)
	}
}

func TestConnectionStatus_UnparseableHeartbeat_IsDisconnected(t *testing.T) {
	now := time.Now().UTC()
	got := ConnectionStatus(true, "not-a-rfc3339-timestamp", false, now)
	if got != StatusDisconnected {
		t.Errorf("unparseable heartbeat should be DISCONNECTED; got %s", got)
	}
}

func TestConnectionStatusForInfo_MutatesFields(t *testing.T) {
	now := time.Now().UTC()
	info := &WorkerInfo{
		WorkerID: "w1",
		LastHB:   hbOffset(now, 5*time.Second),
	}
	ConnectionStatusForInfo(info, true, now)
	if !info.SessionActive {
		t.Errorf("SessionActive not set on info: %+v", info)
	}
	if info.ConnectionStatus != StatusConnected {
		t.Errorf("ConnectionStatus not CONNECTED: %+v", info)
	}
}
