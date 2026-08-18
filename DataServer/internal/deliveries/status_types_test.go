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
