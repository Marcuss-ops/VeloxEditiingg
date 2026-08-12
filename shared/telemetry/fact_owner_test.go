package telemetry

import "testing"

// TestFactOwnerTablePinned pins the Fact Owner rule for the receipt-level
// facts: every fact has EXACTLY one authoritative producer, matching the
// architectural table (clip count → CompiledRenderPlan, downloaded bytes →
// Downloader, packets read → Media backend, …). Changing an owner here is a
// deliberate architectural decision, not a refactor.
func TestFactOwnerTablePinned(t *testing.T) {
	expected := map[string]ComponentOwner{
		FactClipCount:        OwnerRenderPlan,
		FactExpectedDuration: OwnerRenderPlan,
		FactAssetSHA:         OwnerAssetManifest,
		FactCacheHitMiss:     OwnerCacheResolver,
		FactDownloadedBytes:  OwnerDownloader,
		FactProcessSpawn:     OwnerProcessRunner,
		FactPacketsRead:      OwnerMediaEngine,
		FactFramesDecoded:    OwnerDecoder,
		FactFramesEncoded:    OwnerEncoder,
		FactMuxBytes:         OwnerMuxer,
		FactArtifactSHA:      OwnerPublisher,
		FactCPURamDisk:       OwnerAttemptTelemetry,
		FactTaskStatus:       OwnerTaskRunner,
		FactWorkerStatus:     OwnerWorkerState,
	}
	if len(expected) < 10 {
		t.Fatalf("fact owner table unexpectedly small: %d entries", len(expected))
	}
	if FactOwnerCount() != len(expected) {
		t.Fatalf("FactOwnerCount()=%d != pinned table %d (parallel fact registry detected)", FactOwnerCount(), len(expected))
	}

	for fact, wantOwner := range expected {
		if !IsCanonicalFactName(fact) {
			t.Fatalf("pinned fact %q not canonical", fact)
		}
		got, ok := FactOwner(fact)
		if !ok {
			t.Fatalf("FactOwner(%q) not found", fact)
		}
		if got != wantOwner {
			t.Errorf("owner of %s = %q, want %q", fact, got, wantOwner)
		}
		if !IsCanonicalComponentOwner(string(got)) {
			t.Errorf("owner %q of %s is not a closed-vocabulary member", got, fact)
		}
	}

	// No extra facts beyond the pinned table: an unknown fact must be added
	// explicitly, never assigned an owner ad hoc.
	if got := len(AllFactOwners()); got != len(expected) {
		t.Errorf("AllFactOwners() size=%d != pinned table %d", got, len(expected))
	}
}

// TestFactOwnerUnknownFactRejected pins the fail-closed behavior: facts
// outside the closed table have no owner, so a producer cannot silently
// claim ownership of an unregistered fact.
func TestFactOwnerUnknownFactRejected(t *testing.T) {
	if _, ok := FactOwner("invented_fact"); ok {
		t.Error("unknown fact must not resolve an owner")
	}
	if IsCanonicalFactName("") {
		t.Error("empty fact name must not be canonical")
	}
}

// TestFactOwnerDefensiveCopy verifies the exposed table cannot be mutated
// into the canonical registry.
func TestFactOwnerDefensiveCopy(t *testing.T) {
	owners := AllFactOwners()
	owners[FactClipCount] = OwnerDownloader
	got, ok := FactOwner(FactClipCount)
	if !ok || got != OwnerRenderPlan {
		t.Fatalf("mutating AllFactOwners result must not mutate the canonical table: got %q", got)
	}
}
