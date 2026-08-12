package telemetry

import "testing"

func TestGeneratedGoBindingMatchesCatalog(t *testing.T) {
	if GeneratedCatalogSchemaVersion != SchemaVersion {
		t.Fatalf("generated schema version=%d, want %d", GeneratedCatalogSchemaVersion, SchemaVersion)
	}

	generated := GeneratedCatalogEvents()
	if len(generated) != Catalog.Count() {
		t.Fatalf("generated events=%d, catalog events=%d; generated Go binding is stale", len(generated), Catalog.Count())
	}
	for _, descriptor := range generated {
		want, ok := Catalog.Lookup(descriptor.Component, descriptor.Action)
		if !ok {
			t.Fatalf("generated binding contains unknown event %s", descriptor.Key())
		}
		expected := EventDescriptor{
			Component:   want.Component,
			Action:      want.Action,
			Origin:      want.Origin,
			Scope:       want.Scope,
			Phase:       want.Phase,
			EventType:   want.EventType,
			Kind:        want.Kind,
			Unit:        want.Unit,
			TimingMode:  want.TimingMode,
			Aggregation: want.Aggregation,
			Cardinality: want.Cardinality,
			Owner:       want.Owner,
		}
		if descriptor != expected {
			t.Fatalf("generated binding diverges for %s: generated=%+v catalog=%+v", descriptor.Key(), descriptor, want)
		}
	}

	generatedFacts := GeneratedFactOwners()
	if len(generatedFacts) != FactOwnerCount() {
		t.Fatalf("generated facts=%d, catalog facts=%d; generated Go fact binding is stale", len(generatedFacts), FactOwnerCount())
	}
	for _, fact := range generatedFacts {
		owner, ok := FactOwner(fact.Name)
		if !ok {
			t.Fatalf("generated binding contains unknown fact %q", fact.Name)
		}
		if fact.Owner != owner {
			t.Fatalf("generated fact owner for %q=%q, want %q", fact.Name, fact.Owner, owner)
		}
	}
}

func TestGeneratedGoBindingRejectsUnknownEventKeys(t *testing.T) {
	if !IsGeneratedCatalogEvent("engine.encode", "setup") {
		t.Fatal("generated binding rejected registered event engine.encode.setup")
	}
	if IsGeneratedCatalogEvent("engine.encode", "invented") {
		t.Fatal("generated binding accepted unknown event engine.encode.invented")
	}
	if IsGeneratedCatalogEvent("", "") {
		t.Fatal("generated binding accepted empty event key")
	}
}
