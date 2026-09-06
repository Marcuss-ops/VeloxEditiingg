package controltransport

import "testing"

// capability_gate_test.go — A2-3 close-out gate.
//
// The master's grpcserver.capabilitiesBoolMap turns every top-level
// boolean key of the capability map into a typed admission CapabilitySet
// entry that placement matches against. The namespace is therefore
// load-bearing: a bool field casually added to CapabilityReport.AsMap
// would silently become a placement gate. These tests pin the contract:
//
//  1. every bool-valued key AsMap emits must be either a KNOWN admission
//     capability (allCapabilities) or a registered non-admission
//     transport flag (the CapabilityAssetCacheKeysTruncated class);
//  2. non-admission flags must never be part of allCapabilities (they
//     must not be satisfiable as admission features);
//  3. Features entries must stay inside the known registry (they are the
//     intended source of admission capabilities).

// capabilityBoolKeysFromAsMap extracts the top-level bool-valued keys the
// canonical AsMap emits for a fully-populated report.
func capabilityBoolKeysFromAsMap(t *testing.T) map[string]struct{} {
	t.Helper()
	report := CapabilityReport{
		SchemaVersion: CapabilitySchemaVersion,
		Host: HostInfo{
			WorkerID:        "w",
			CPUCount:        1,
			MaxParallelJobs: 1,
			RAMBytes:        1,
			DiskFreeBytes:   1,
			HasGPU:          true,
			NVENCAvailable:  true,
			NVDECAvailable:  true,
			QSVAvailable:    true,
		},
		Features:            allCapabilities[:],
		AssetCacheKeys:      []string{"k"},
		AssetCacheTruncated: true,
	}
	boolKeys := map[string]struct{}{}
	for key, val := range report.AsMap() {
		if _, ok := val.(bool); ok {
			boolKeys[key] = struct{}{}
		}
	}
	if len(boolKeys) == 0 {
		t.Fatal("AsMap emitted no bool keys — the gate lost its subject")
	}
	return boolKeys
}

// TestAsMapBoolKeysAreClassified fails when a new bool key appears in
// AsMap without being classified as admission capability (registry) or
// transport flag (grpcserver filter). The master-side filter
// (isNonAdmissionCapabilityKey in DataServer/internal/grpcserver/
// session_capabilities.go) must list every transport flag mirrored here.
func TestAsMapBoolKeysAreClassified(t *testing.T) {
	boolKeys := capabilityBoolKeysFromAsMap(t)

	known := map[string]struct{}{}
	for _, c := range allCapabilities {
		known[c] = struct{}{}
	}

	// Registered transport flags: the single source is the same constant
	// the master's capabilitiesBoolMap filter consumes. Keep this set in
	// lockstep with isNonAdmissionCapabilityKey — the master-side test
	// TestCapabilityBoolKeyFilterMatchesSharedRegistry enforces parity.
	transportFlags := map[string]struct{}{
		CapabilityAssetCacheKeysTruncated: {},
	}

	for key := range boolKeys {
		_, isKnown := known[key]
		_, isFlag := transportFlags[key]
		if !isKnown && !isFlag {
			t.Fatalf("unclassified bool key %q emitted by CapabilityReport.AsMap: "+
				"add it to allCapabilities (admission) or to the non-admission "+
				"transport-flag registry (and the master-side filter)", key)
		}
	}
}

// TestTransportFlagsAreNotAdmissionCapabilities ensures a transport
// bookkeeping flag can never be satisfied as an admission feature via the
// registry.
func TestTransportFlagsAreNotAdmissionCapabilities(t *testing.T) {
	if IsKnownCapability(CapabilityAssetCacheKeysTruncated) {
		t.Fatalf("transport flag %q must not be in the capability registry", CapabilityAssetCacheKeysTruncated)
	}
}

// TestKnownCapabilitiesAreValidKeys sanity: every registry entry is a
// non-empty, lowercase, dotted/underscored protocol key (no spaces, no
// upper-case drift).
func TestKnownCapabilitiesAreValidKeys(t *testing.T) {
	for _, c := range KnownCapabilities() {
		if c == "" {
			t.Fatal("empty capability key in registry")
		}
		for _, r := range c {
			switch {
			case r >= 'a' && r <= 'z':
			case r >= '0' && r <= '9':
			case r == '.' || r == '_' || r == '-':
			default:
				t.Fatalf("capability key %q contains invalid rune %q", c, string(r))
			}
		}
	}
}
