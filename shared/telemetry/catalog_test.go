package telemetry

import "testing"

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
