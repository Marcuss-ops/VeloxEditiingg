package telemetry

import (
	"sort"
	"strings"
	"testing"
)

func TestCatalogIsSharedClosedAndDefensive(t *testing.T) {
	if Catalog.Count() < 100 {
		t.Fatalf("catalog count=%d, want a complete closed catalog", Catalog.Count())
	}
	got, ok := Catalog.Lookup("engine.encode", "setup")
	if !ok {
		t.Fatal("engine.encode.setup must be registered")
	}
	if got.Origin != OriginEngine || got.Scope != ScopeSegment || got.SchemaVersion != SchemaVersion {
		t.Fatalf("registration=%+v, want engine/segment/schema=%d", got, SchemaVersion)
	}

	entries := Catalog.Entries()
	delete(entries, "engine.encode.setup")
	if _, ok := Catalog.Lookup("engine.encode", "setup"); !ok {
		t.Fatal("mutating Entries result must not mutate catalog")
	}
}

// TestCanonicalDescriptorsAreSingleSource pins the single-registry rule:
// catalog.json is the ONLY taxonomy source. Go's canonicalEventDescriptors is
// loaded from it, every entry must be complete and unique by key, and the
// runtime catalog must expose exactly that source. Adding an event means
// editing catalog.json — no parallel canonicalEventKeys/canonicalOriginScope
// registry may re-emerge.
func TestCanonicalDescriptorsAreSingleSource(t *testing.T) {
	if len(canonicalEventDescriptors) < 100 {
		t.Fatalf("canonical descriptors count=%d, want a complete closed catalog", len(canonicalEventDescriptors))
	}
	seen := make(map[string]bool, len(canonicalEventDescriptors))
	for _, d := range canonicalEventDescriptors {
		if d.Component == "" || d.Action == "" {
			t.Fatalf("descriptor with empty component/action: %+v", d)
		}
		if d.Origin == "" || d.Scope == "" {
			t.Fatalf("descriptor %s missing origin/scope", d.Key())
		}
		if seen[d.Key()] {
			t.Fatalf("duplicate canonical descriptor key %s", d.Key())
		}
		seen[d.Key()] = true
		if _, ok := Catalog.Lookup(d.Component, d.Action); !ok {
			t.Fatalf("descriptor %s not exposed by Catalog", d.Key())
		}
	}
	if Catalog.Count() != len(canonicalEventDescriptors) {
		t.Fatalf("Catalog.Count()=%d != canonicalEventDescriptors=%d (runtime registry diverged from the literal)", Catalog.Count(), len(canonicalEventDescriptors))
	}

	// Keys sorted for reviewability.
	keys := make([]string, 0, len(canonicalEventDescriptors))
	for _, d := range canonicalEventDescriptors {
		keys = append(keys, d.Key())
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("canonicalEventDescriptors must be sorted by key (reviewability); first unsorted pair %v", keys)
	}

	// Origin/scope values are closed vocabulary members.
	origins := map[string]bool{OriginMaster: true, OriginWorker: true, OriginEngine: true, OriginFFmpeg: true, OriginUpload: true, OriginValidation: true}
	scopes := map[string]bool{ScopeJob: true, ScopeTask: true, ScopeAttempt: true, ScopeSegment: true, ScopeAudioTrack: true, ScopeSubtitleTrack: true, ScopeArtifact: true}
	for _, d := range canonicalEventDescriptors {
		if !origins[d.Origin] {
			t.Fatalf("descriptor %s uses non-canonical origin %q", d.Key(), d.Origin)
		}
		if !scopes[d.Scope] {
			t.Fatalf("descriptor %s uses non-canonical scope %q", d.Key(), d.Scope)
		}
		if strings.ContainsAny(d.EventType, "\n\r") {
			t.Fatalf("descriptor %s has malformed event type", d.Key())
		}
	}
}

// TestCanonicalDescriptorsCarryCompleteSemantics pins the Fact Owner +
// TelemetryDescriptor contract: every canonical entry resolves the six
// semantic attributes (Kind, Unit, TimingMode, Aggregation, Cardinality, Owner) at
// declaration time, they are closed-vocabulary members, and Kind/TimingMode
// are mutually consistent. A descriptor missing a semantic attribute is a
// taxonomy hole, not a legacy entry.
func TestCanonicalDescriptorsCarryCompleteSemantics(t *testing.T) {
	for _, d := range canonicalEventDescriptors {
		if err := ValidateEventDescriptorSemantics(d); err != nil {
			t.Fatalf("semantic validation failed for %s: %v", d.Key(), err)
		}
		spec, ok := Catalog.Lookup(d.Component, d.Action)
		if !ok {
			t.Fatalf("descriptor %s not exposed by Catalog", d.Key())
		}
		if spec.Kind != d.Kind || spec.TimingMode != d.TimingMode ||
			spec.Aggregation != d.Aggregation || spec.Cardinality != d.Cardinality ||
			spec.Owner != d.Owner {
			t.Fatalf("catalog spec for %s lost semantic attributes: spec=%+v literal=%+v", d.Key(), spec, d)
		}
	}

	// The catalog must be dominated by exclusive timing facts today: the
	// accounted_ratio KPI sums exactly the exclusive top-level phases, so a
	// catalog without them would leave the ratio undefined.
	hasExclusive := false
	hasCounter := false
	for _, d := range canonicalEventDescriptors {
		hasExclusive = hasExclusive || d.TimingMode == TimingExclusive
		hasCounter = hasCounter || d.Kind == KindCounter
	}
	if !hasExclusive {
		t.Fatal("catalog has no exclusive timing phase; accounted_ratio has no denominator")
	}
	if !hasCounter {
		t.Fatal("catalog has no counter kind; discrete facts (spawn, miss, mismatch) missing")
	}
}

// TestCanonicalDescriptorsTimingRule pins the accounted_ratio guard from the
// catalog's accounted_ratio_rule: only per_attempt duration events are
// exclusive top-level phases (the ONLY facts summed into accounted_ratio);
// finer-grained durations are span_child so parallel instances (segments,
// asset transfers) can never double-count against wall clock; non-timed facts
// (counters/gauges) use timing none.
func TestCanonicalDescriptorsTimingRule(t *testing.T) {
	for _, d := range canonicalEventDescriptors {
		switch d.TimingMode {
		case TimingExclusive:
			if d.Kind != KindDuration || d.Cardinality != CardPerAttempt {
				t.Errorf("descriptor %s exclusive requires duration+per_attempt, got kind=%s cardinality=%s", d.Key(), d.Kind, d.Cardinality)
			}
		case TimingSpanChild:
			if d.Kind != KindDuration {
				t.Errorf("descriptor %s span_child requires kind=duration, got %s", d.Key(), d.Kind)
			}
			if d.Cardinality == CardPerAttempt {
				t.Errorf("descriptor %s span_child on a per_attempt fact (not a child span)", d.Key())
			}
		case TimingNone:
			if d.Kind == KindDuration || d.Kind == KindSpan {
				t.Errorf("descriptor %s timing none on a timed kind %s", d.Key(), d.Kind)
			}
		}
	}
}

// TestCanonicalDescriptorsCarryFactOwner pins representative Fact Owner
// assignments: each fact is owned by exactly the authoritative producer, and
// no other component may reconstruct it. Mirrors the fact_owner.go table for
// the event-level facts.
func TestCanonicalDescriptorsCarryFactOwner(t *testing.T) {
	assertions := []struct {
		component string
		action    string
		owner     ComponentOwner
	}{
		// Process facts → process runner.
		{component: "worker.engine", action: "spawn", owner: OwnerProcessRunner},
		{component: "ffmpeg", action: "progress", owner: OwnerProcessRunner},
		// Download facts → downloader.
		{component: "worker.asset", action: "transfer", owner: OwnerDownloader},
		{component: "worker.asset", action: "final_hash", owner: OwnerDownloader},
		// Cache facts → cache resolver.
		{component: "worker.cache", action: "miss", owner: OwnerCacheResolver},
		{component: "runner", action: "cache_lookup", owner: OwnerCacheResolver},
		// Media facts → media backend family.
		{component: "engine.input", action: "open", owner: OwnerMediaEngine},
		{component: "engine.video", action: "decode", owner: OwnerDecoder},
		{component: "engine.encode", action: "setup", owner: OwnerEncoder},
		{component: "engine.mux", action: "packet_write", owner: OwnerMuxer},
		{component: "engine.mux", action: "trailer", owner: OwnerMuxer},
		// Plan / manifest facts → render plan / asset manifest.
		{component: "master.plan", action: "compile", owner: OwnerRenderPlan},
		{component: "worker.plan", action: "resolve_assets", owner: OwnerAssetManifest},
		// Artifact facts → publisher / uploader.
		{component: "master.upload", action: "verify", owner: OwnerPublisher},
		{component: "worker.upload", action: "transfer", owner: OwnerUploader},
		// Resource facts → attempt telemetry.
		{component: "worker.temp", action: "read", owner: OwnerAttemptTelemetry},
		{component: "worker.disk", action: "wait", owner: OwnerAttemptTelemetry},
		// Task lifecycle → task runner.
		{component: "runner", action: "execute", owner: OwnerTaskRunner},
	}
	for _, assertion := range assertions {
		got, ok := Catalog.Lookup(assertion.component, assertion.action)
		if !ok {
			t.Fatalf("descriptor %s.%s not registered", assertion.component, assertion.action)
		}
		if got.Owner != assertion.owner {
			t.Errorf("owner of %s.%s = %q, want %q", assertion.component, assertion.action, got.Owner, assertion.owner)
		}
	}
}

func TestCatalogValidateAndNormalize(t *testing.T) {
	spec := TelemetryEventSpec{
		Origin: OriginEngine, Scope: ScopeSegment,
		Component: "engine.encode", Action: "setup",
	}
	if err := Catalog.Normalize(&spec); err != nil {
		t.Fatalf("normalize valid event: %v", err)
	}
	if spec.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version=%d, want %d", spec.SchemaVersion, SchemaVersion)
	}

	cases := []TelemetryEventSpec{
		{Origin: OriginWorker, Scope: ScopeAttempt, Component: "engine.encode", Action: "setup"},
		{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.unknown", Action: "invented"},
		{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.encode", Action: "setup", SchemaVersion: SchemaVersion + 1},
	}
	for _, invalid := range cases {
		if err := Catalog.Validate(invalid); err == nil {
			t.Errorf("invalid event %+v unexpectedly validated", invalid)
		}
	}
}
