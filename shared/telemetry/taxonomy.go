package telemetry

// taxonomy.go binds the canonical 12-phase model declared in the
// language-neutral schema/catalog.json (schema.phase_taxonomy).
//
// The taxonomy is the phase-level view of the accounted_ratio rule:
//
//	exclusive    — top-level, non-overlapping phase; the ONLY phases whose
//	               durations are summed into accounted_ratio.
//	span_parent  — contains child spans; its own duration overlaps its
//	               children and is NEVER summed (summing parent + children
//	               would double-count parallel work against wall clock).
//	span_child   — nested under a top-level phase; NEVER summed.
//
// Roles reuse the closed TimingMode vocabulary; the taxonomy adds the
// parent/child nesting that the flat event vocabulary cannot express. Both
// levels derive from the same single schema/catalog.json source.

import "sort"

// ── Canonical phase name constants ─────────────────────────────────────────
//
// These are the stable labels of the 12-phase model declared in
// schema/catalog.json (schema.phases / schema.phase_order). The literals live
// here ONCE so worker/master consumers can reference compile-time constants
// instead of re-declaring the vocabulary; validateCanonicalPhaseConstants
// (init) pins them to the catalog-loaded order so a catalog edit that renames
// a phase fails startup rather than silently drifting consumers.
const (
	PhaseQueue       = "queue"
	PhaseAssetWait   = "asset_wait"
	PhaseCacheLookup = "cache_lookup"
	PhaseDownload    = "download"
	PhaseDecode      = "decode"
	PhaseCompile     = "compile"
	PhaseSimulate    = "simulate"
	PhaseRender      = "render"
	PhaseComposite   = "composite"
	PhaseEncode      = "encode"
	PhaseUpload      = "upload"
	PhaseFinalize    = "finalize"
)

// canonicalPhaseConstants mirrors CanonicalPhaseOrder() in the exact catalog
// order; validateCanonicalPhaseConstants enforces the 1:1 correspondence.
var canonicalPhaseConstants = [...]string{
	PhaseQueue, PhaseAssetWait, PhaseCacheLookup, PhaseDownload,
	PhaseDecode, PhaseCompile, PhaseSimulate, PhaseRender,
	PhaseComposite, PhaseEncode, PhaseUpload, PhaseFinalize,
}

func validateCanonicalPhaseConstants() {
	if len(canonicalPhaseOrder) != len(canonicalPhaseConstants) {
		panic("telemetry: canonical phase constant count diverges from catalog phase_order")
	}
	for i := range canonicalPhaseOrder {
		if canonicalPhaseOrder[i] != canonicalPhaseConstants[i] {
			panic("telemetry: canonical phase constant " + canonicalPhaseConstants[i] +
				" diverges from catalog phase_order " + canonicalPhaseOrder[i])
		}
	}
}

func init() { validateCanonicalPhaseConstants() }

// PhaseTaxon is one canonical phase's role in the taxonomy. Name is the
// canonical phase label; Role is its accounted role (a TimingMode value:
// exclusive | span_parent | span_child); Parent is the containing phase,
// empty for top-level phases.
type PhaseTaxon struct {
	Name   string
	Role   TimingMode
	Parent string
}

// canonicalPhaseTaxonomy is loaded once from the language-neutral JSON
// source (validated by validatePhaseTaxonomy at parse time).
var canonicalPhaseTaxonomy = loadCanonicalPhaseTaxonomy()

// canonicalPhaseOrder is the stable order declared by the shared schema.
// Worker and Master callers must project this value rather than maintaining
// independent phase lists.
var canonicalPhaseOrder = loadCanonicalPhaseOrder()

// CanonicalPhaseOrder returns the catalog-defined order of canonical phases.
// The returned slice is independent and may be mutated by the caller.
func CanonicalPhaseOrder() []string {
	return append([]string(nil), canonicalPhaseOrder...)
}

// Taxonomy returns a defensive copy of the canonical phase taxonomy keyed
// by phase name.
func Taxonomy() map[string]PhaseTaxon {
	out := make(map[string]PhaseTaxon, len(canonicalPhaseTaxonomy))
	for phase, taxon := range canonicalPhaseTaxonomy {
		out[phase] = taxon
	}
	return out
}

// PhaseRoleOf returns the canonical accounted role of a phase
// (exclusive | span_parent | span_child). The second value is false for
// unknown phases — callers must fail closed (never treat an unknown phase
// as exclusive).
func PhaseRoleOf(phase string) (TimingMode, bool) {
	taxon, ok := canonicalPhaseTaxonomy[phase]
	if !ok {
		return "", false
	}
	return taxon.Role, true
}

// IsAccountedTopLevelPhase reports whether a canonical phase is a
// top-level exclusive phase — the only phases whose durations may feed
// accounted_ratio (catalog accounted_ratio_rule). Unknown phases are
// never accounted: a false result is the fail-closed quarantine.
func IsAccountedTopLevelPhase(phase string) bool {
	role, ok := PhaseRoleOf(phase)
	return ok && role == TimingExclusive
}

// AllPhaseTaxa returns the canonical taxonomy sorted by phase name — the
// stable shape consumed by the binding generator (cataloggen) and by
// tests that pin the model.
func AllPhaseTaxa() []PhaseTaxon {
	out := make([]PhaseTaxon, 0, len(canonicalPhaseTaxonomy))
	for phase, taxon := range canonicalPhaseTaxonomy {
		out = append(out, PhaseTaxon{Name: phase, Role: taxon.Role, Parent: taxon.Parent})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func loadCanonicalPhaseTaxonomy() map[string]PhaseTaxon {
	doc := mustLanguageNeutralCatalog()
	out := make(map[string]PhaseTaxon, len(doc.Schema.PhaseTaxonomy))
	for phase, taxon := range doc.Schema.PhaseTaxonomy {
		out[phase] = PhaseTaxon{Name: phase, Role: TimingMode(taxon.Role), Parent: taxon.Parent}
	}
	return out
}

func loadCanonicalPhaseOrder() []string {
	doc := mustLanguageNeutralCatalog()
	return append([]string(nil), doc.Schema.PhaseOrder...)
}
