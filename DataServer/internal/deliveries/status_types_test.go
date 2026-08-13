package deliveries

import (
	"encoding/json"
	"testing"
)

func TestDeliveryStatusValidity(t *testing.T) {
	for _, status := range []DeliveryStatus{DeliveryPending, DeliveryRunning, DeliveryRetryWait, DeliverySucceeded, DeliveryFailed, DeliveryBlockedAuth, DeliveryCancelled} {
		if !status.Valid() {
			t.Fatalf("status %q should be valid", status)
		}
	}
	if DeliveryStatus("PUBLISHED").Valid() {
		t.Fatal("PUBLISHED is a publication alias, not a delivery status")
	}
	if !DeliverySucceeded.IsTerminal() || DeliveryRunning.IsTerminal() {
		t.Fatal("delivery terminal semantics are incorrect")
	}
}

func TestReconciliationStatusMapsToTypedDeliveryStatus(t *testing.T) {
	cases := []struct {
		provider string
		want     DeliveryStatus
	}{
		{"published", DeliverySucceeded},
		{"COMPLETED", DeliverySucceeded},
		{"failed", DeliveryFailed},
		{"dead_letter", DeliveryFailed},
		{"blocked_auth", DeliveryBlockedAuth},
		{"retry_wait", DeliveryRetryWait},
		{"rate_limited", DeliveryRetryWait},
		{"  processing  ", DeliveryRunning},
		{"", DeliveryRunning},
	}
	for _, tc := range cases {
		if got := reconciliationStatus(tc.provider); got != tc.want {
			t.Errorf("reconciliationStatus(%q) = %q, want %q", tc.provider, got, tc.want)
		}
	}
}

func TestDeliveryStatusKeepsWireSpelling(t *testing.T) {
	data, err := json.Marshal(struct {
		Status DeliveryStatus `json:"status"`
	}{Status: DeliverySucceeded})
	if err != nil {
		t.Fatalf("marshal delivery status: %v", err)
	}
	if string(data) != `{"status":"SUCCEEDED"}` {
		t.Fatalf("wire delivery status = %s", data)
	}
}
