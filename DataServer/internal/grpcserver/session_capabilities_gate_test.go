package grpcserver

import (
	"testing"

	"velox-shared/controltransport"
)

// session_capabilities_gate_test.go — master-side half of the A2-3 gate
// (see shared/controltransport/capability_gate_test.go for the producer
// half). Parity contract: every key the master's capabilitiesBoolMap
// filter classifies as non-admission MUST be a transport flag that the
// shared AsMap actually emits, and vice versa. Drift in either direction
// means a flag either leaks into the placement gate or a real capability
// gets filtered out.

// TestCapabilityBoolKeyFilterMatchesSharedRegistry pins the filter to the
// shared flag registry.
func TestCapabilityBoolKeyFilterMatchesSharedRegistry(t *testing.T) {
	// The filter must reject the shared transport flag...
	if !isNonAdmissionCapabilityKey(controltransport.CapabilityAssetCacheKeysTruncated) {
		t.Fatalf("shared transport flag %q must be classified non-admission",
			controltransport.CapabilityAssetCacheKeysTruncated)
	}

	// ...and must NOT reject any registered admission capability (that
	// would silently drop a real placement gate).
	for _, c := range controltransport.KnownCapabilities() {
		if isNonAdmissionCapabilityKey(c) {
			t.Fatalf("known admission capability %q must not be classified non-admission", c)
		}
	}
}

// TestCapabilitiesBoolMapExcludesTransportFlag exercises the actual filter
// path end-to-end: a transport flag present in a raw capability map must
// not appear in the resulting CapabilitySet, while a real capability in
// the same map does.
func TestCapabilitiesBoolMapExcludesTransportFlag(t *testing.T) {
	raw := map[string]interface{}{
		controltransport.CapabilityAssetCacheKeysTruncated: true, // transport flag
		controltransport.CapabilityCanonicalPayloadV2:      true, // admission capability
		controltransport.CapabilityTaskCommitAckV1:         false,
		"unknown_forward_only_feature":                     true, // forward-only passthrough
	}

	got := capabilitiesBoolMap(raw)

	if got.Has(controltransport.CapabilityAssetCacheKeysTruncated) {
		t.Fatal("transport flag leaked into the CapabilitySet")
	}
	if !got.Has(controltransport.CapabilityCanonicalPayloadV2) {
		t.Fatal("admission capability was filtered out")
	}
	if got.Has(controltransport.CapabilityTaskCommitAckV1) {
		t.Fatal("false-valued capability must not be admitted")
	}
	if !got.Has("unknown_forward_only_feature") {
		t.Fatal("forward-only unknown capability must pass through (IsKnownCapability contract)")
	}
}
