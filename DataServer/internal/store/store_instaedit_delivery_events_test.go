package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestApplyInstaEditDeliveryEvent_DeduplicatesAndRejectsOlderSequence(t *testing.T) {
	db, err := NewSQLiteStore(t.TempDir() + "/events.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	event := InstaEditDeliveryEvent{
		EventID: "evt-2", DeliveryID: "delivery-1", Sequence: 2,
		Status: "published", OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"status":"published"}`),
	}
	if applied, err := db.ApplyInstaEditDeliveryEvent(context.Background(), event); err != nil || !applied {
		t.Fatalf("first callback applied=%v err=%v", applied, err)
	}
	if applied, err := db.ApplyInstaEditDeliveryEvent(context.Background(), event); err != nil || applied {
		t.Fatalf("duplicate callback applied=%v err=%v", applied, err)
	}
	older := event
	older.EventID = "evt-1"
	older.Sequence = 1
	if applied, err := db.ApplyInstaEditDeliveryEvent(context.Background(), older); err != nil || applied {
		t.Fatalf("older callback applied=%v err=%v", applied, err)
	}
}
