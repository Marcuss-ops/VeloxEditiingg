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
// canonicalEventDescriptors is the ONLY taxonomy literal. Every entry must be
// complete (origin+scope+phase when phase applies), unique by key, and the
// runtime catalog must expose exactly that list. Adding an event means
// editing this one list — no parallel canonicalEventKeys/canonicalOriginScope
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
