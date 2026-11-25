package pipeline

import (
	"testing"

	"velox-server/internal/store"
)

func TestPrefetchAssetStatesDoesNotCallFailedAssetReady(t *testing.T) {
	events := []store.JobEvent{
		{Event: "prefetch.asset_ready", RawJSON: `{"asset_key":"legacy","error_reason":"asset not found"}`},
		{Event: "prefetch.asset_failed", RawJSON: `{"asset_key":"current","error_reason":"asset not found"}`},
		{Event: "prefetch.asset_ready", RawJSON: `{"asset_key":"healthy","cache_hit":true}`},
	}

	states := prefetchAssetStates(events)
	for key, want := range map[string]string{
		"legacy":  "FAILED",
		"current": "FAILED",
		"healthy": "READY",
	} {
		if got := states[key]; got != want {
			t.Errorf("state for %q = %q, want %q", key, got, want)
		}
	}
}
