package outbox_test

import (
	"context"
	"testing"
)

func TestDrainLegacyEventsFailsClosedWhenStoreIsUnavailable(t *testing.T) {
	store := newTestStore(t)
	if err := store.DB.Close(); err != nil {
		t.Fatalf("close test store: %v", err)
	}
	if _, err := store.DrainLegacyEvents(context.Background(), []string{"LEGACY_EVENT"}); err == nil {
		t.Fatal("DrainLegacyEvents returned nil after store close")
	}
}
