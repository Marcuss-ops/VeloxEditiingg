package worker

import "testing"

func TestRememberRuntimeAssetLocator(t *testing.T) {
	const locator = "https://77.93.152.122/objects/hash?expires=1&sig=opaque"
	w := &Worker{}
	w.rememberRuntimeAssetLocator(map[string]interface{}{
		"asset_id":   "final-fragment-1",
		"url":        "velox-asset://final-fragment-1",
		"source_uri": locator,
	})
	if got := w.sourceLocator("final-fragment-1"); got != locator {
		t.Fatalf("source locator = %q, want runtime HTTPS source_uri", got)
	}
}
