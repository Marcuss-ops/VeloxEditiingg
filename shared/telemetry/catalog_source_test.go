package telemetry

import "testing"

func TestLanguageNeutralCatalogIsTheGoSourceOfTruth(t *testing.T) {
	doc, err := parseLanguageNeutralCatalog()
	if err != nil {
		t.Fatalf("parse language-neutral catalog: %v", err)
	}
	if len(doc.Events) != Catalog.Count() {
		t.Fatalf("JSON events=%d, Go catalog=%d; parallel taxonomy detected", len(doc.Events), Catalog.Count())
	}
	if len(doc.Facts) != FactOwnerCount() {
		t.Fatalf("JSON facts=%d, Go fact owners=%d; parallel fact table detected", len(doc.Facts), FactOwnerCount())
	}
	if doc.Schema.AccountedRatioRule == "" {
		t.Fatal("accounted_ratio rule must be explicit in the shared schema")
	}
	for _, event := range doc.Events {
		spec, ok := Catalog.Lookup(event.Component, event.Action)
		if !ok {
			t.Fatalf("JSON event %q missing from Go catalog", event.Key)
		}
		if string(spec.Unit) != event.Unit || string(spec.Owner) != event.Owner ||
			string(spec.TimingMode) != event.TimingMode || string(spec.Cardinality) != event.Cardinality {
			t.Fatalf("JSON semantic fields not projected for %q: spec=%+v JSON=%+v", event.Key, spec, event)
		}
	}
}
