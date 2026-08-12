package telemetry

import (
	"sort"
	"strings"
	"testing"
)

// TestPhaseTaxonomyModelsAllTwelvePhases pins the canonical 12-phase model:
// every phase in the flat vocabulary is modeled exactly once, and the roles
// match the accounted_ratio rule (exclusive = top-level and summed;
// span_parent = nested under a top-level phase, never summed).
func TestPhaseTaxonomyModelsAllTwelvePhases(t *testing.T) {
	taxa := AllPhaseTaxa()
	if len(taxa) != 12 {
		t.Fatalf("taxonomy phases = %d, want the 12 canonical phases", len(taxa))
	}
	roles := make(map[string]TimingMode, len(taxa))
	for _, taxon := range taxa {
		if taxon.Name == "" || taxon.Role == "" {
			t.Fatalf("taxon with empty name/role: %+v", taxon)
		}
		roles[taxon.Name] = taxon.Role
	}
	wantExclusive := []string{"queue", "asset_wait", "cache_lookup", "download", "compile", "simulate", "render", "upload", "finalize"}
	for _, phase := range wantExclusive {
		if roles[phase] != TimingExclusive {
			t.Errorf("phase %q role = %q, want exclusive", phase, roles[phase])
		}
	}
	wantSpanParent := map[string]string{
		"decode":    "render",
		"composite": "render",
		"encode":    "render",
	}
	for phase, parent := range wantSpanParent {
		taxon, ok := canonicalPhaseTaxonomy[phase]
		if !ok {
			t.Fatalf("phase %q missing from taxonomy", phase)
		}
		if taxon.Role != TimingSpanParent {
			t.Errorf("phase %q role = %q, want span_parent", phase, taxon.Role)
		}
		if taxon.Parent != parent {
			t.Errorf("phase %q parent = %q, want %q", phase, taxon.Parent, parent)
		}
	}
}

// TestIsAccountedTopLevelPhase pins the accounted_ratio denominator: ONLY
// the exclusive-role phases are accounted; span parents and unknown phases
// are never (the fail-closed quarantine).
func TestIsAccountedTopLevelPhase(t *testing.T) {
	for _, phase := range []string{"render", "queue", "finalize", "upload", "download"} {
		if !IsAccountedTopLevelPhase(phase) {
			t.Errorf("IsAccountedTopLevelPhase(%q) = false, want true", phase)
		}
	}
	for _, phase := range []string{"decode", "composite", "encode", "invented", ""} {
		if IsAccountedTopLevelPhase(phase) {
			t.Errorf("IsAccountedTopLevelPhase(%q) = true, want false", phase)
		}
	}
	role, ok := PhaseRoleOf("render")
	if !ok || role != TimingExclusive {
		t.Fatalf("PhaseRoleOf(render) = %q/%v", role, ok)
	}
	if _, ok := PhaseRoleOf("invented"); ok {
		t.Fatal("PhaseRoleOf(invented) must fail closed")
	}
}

// TestTaxonomyDefensiveCopy pins the read-only contract: mutating the
// returned map must never mutate the canonical taxonomy (same pattern as
// the events catalog).
func TestTaxonomyDefensiveCopy(t *testing.T) {
	got := Taxonomy()
	got["render"] = PhaseTaxon{Name: "render", Role: TimingSpanChild}
	role, ok := PhaseRoleOf("render")
	if !ok || role != TimingExclusive {
		t.Fatalf("Taxonomy() mutation leaked into the canonical model: role=%q ok=%v", role, ok)
	}
}

// TestPhaseTaxonomyKeysMatchVocabulary pins the bijection between the flat
// phases vocabulary and the taxonomy: adding a phase means modeling it in
// BOTH places or validation fails at parse time.
func TestPhaseTaxonomyKeysMatchVocabulary(t *testing.T) {
	doc := mustLanguageNeutralCatalog()
	keys := make([]string, 0, len(doc.Schema.PhaseTaxonomy))
	for phase := range doc.Schema.PhaseTaxonomy {
		keys = append(keys, phase)
	}
	sort.Strings(keys)
	if !sameStringSet(keys, doc.Schema.Phases) {
		t.Fatalf("taxonomy keys %v diverge from phases vocabulary %v", keys, doc.Schema.Phases)
	}
}

// TestValidatePhaseTaxonomyRejectsBrokenModels pins the fail-closed
// validation: a phase declared exclusive with a parent, a span phase without
// a parent, or an exclusive event under a span phase must all be rejected —
// accounted_ratio semantics must never ride on a half-modeled taxonomy.
func TestValidatePhaseTaxonomyRejectsBrokenModels(t *testing.T) {
	good := func() languageNeutralSchema {
		doc := mustLanguageNeutralCatalog()
		return doc.Schema
	}
	events := func() []languageNeutralEvent {
		doc := mustLanguageNeutralCatalog()
		return doc.Events
	}
	cloneTaxonomy := func(schema languageNeutralSchema) map[string]languageNeutralPhaseTaxon {
		out := make(map[string]languageNeutralPhaseTaxon, len(schema.PhaseTaxonomy))
		for phase, taxon := range schema.PhaseTaxonomy {
			out[phase] = taxon
		}
		return out
	}
	cases := []struct {
		name    string
		mutate  func(schema languageNeutralSchema) languageNeutralSchema
		wantErr string
	}{
		{
			name: "exclusive with parent",
			mutate: func(schema languageNeutralSchema) languageNeutralSchema {
				schema.PhaseTaxonomy = cloneTaxonomy(schema)
				taxon := schema.PhaseTaxonomy["render"]
				taxon.Parent = "queue"
				schema.PhaseTaxonomy["render"] = taxon
				return schema
			},
			wantErr: "exclusive role must not declare a parent",
		},
		{
			name: "span parent missing parent",
			mutate: func(schema languageNeutralSchema) languageNeutralSchema {
				schema.PhaseTaxonomy = cloneTaxonomy(schema)
				taxon := schema.PhaseTaxonomy["decode"]
				taxon.Parent = ""
				schema.PhaseTaxonomy["decode"] = taxon
				return schema
			},
			wantErr: "span role \"span_parent\" must declare a parent",
		},
		{
			name: "unknown parent",
			mutate: func(schema languageNeutralSchema) languageNeutralSchema {
				schema.PhaseTaxonomy = cloneTaxonomy(schema)
				taxon := schema.PhaseTaxonomy["encode"]
				taxon.Parent = "invented"
				schema.PhaseTaxonomy["encode"] = taxon
				return schema
			},
			wantErr: "unknown parent",
		},
		{
			name: "invalid role",
			mutate: func(schema languageNeutralSchema) languageNeutralSchema {
				schema.PhaseTaxonomy = cloneTaxonomy(schema)
				taxon := schema.PhaseTaxonomy["queue"]
				taxon.Role = "none"
				schema.PhaseTaxonomy["queue"] = taxon
				return schema
			},
			wantErr: "invalid role",
		},
		{
			name: "exclusive event under span phase",
			mutate: func(schema languageNeutralSchema) languageNeutralSchema {
				schema.PhaseTaxonomy = cloneTaxonomy(schema)
				taxon := schema.PhaseTaxonomy["render"]
				taxon.Role = "span_parent"
				taxon.Parent = "queue"
				schema.PhaseTaxonomy["render"] = taxon
				return schema
			},
			wantErr: "exclusive timing under non-exclusive phase",
		},
		{
			name: "missing phase in taxonomy",
			mutate: func(schema languageNeutralSchema) languageNeutralSchema {
				delete(schema.PhaseTaxonomy, "upload")
				return schema
			},
			wantErr: "phase_taxonomy keys diverge",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := tc.mutate(good())
			err := validatePhaseTaxonomy(schema, events())
			if err == nil {
				t.Fatal("validatePhaseTaxonomy accepted a broken taxonomy")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantErr)
			}
		})
	}
}
