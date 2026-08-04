package workercache

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCleanerAuditEvent_JSONIncludesOperationalFields(t *testing.T) {
	event := CleanerAuditEvent{
		Event:                cleanerAuditEventName,
		AssetKey:             "SHARED-STOCK",
		Role:                 "shared_stock",
		Decision:             "kept",
		Reason:               "active_lease",
		Lease:                "job-42",
		ActiveLeaseCount:     2,
		FutureReferenceCount: 5,
		Timestamp:            time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
		LastUsedAt:           time.Date(2026, 8, 4, 11, 55, 0, 0, time.UTC),
		SizeBytes:            123456,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, key := range []string{
		"event", "asset_key", "role", "decision", "reason", "lease",
		"active_lease_count", "future_reference_count", "timestamp", "last_used_at", "size_bytes",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("JSON missing %q: %s", key, payload)
		}
	}
	if got["active_lease_count"] != float64(2) || got["future_reference_count"] != float64(5) || got["size_bytes"] != float64(123456) {
		t.Errorf("numeric fields=%v, want lease=2 future=5 size=123456", got)
	}
}
