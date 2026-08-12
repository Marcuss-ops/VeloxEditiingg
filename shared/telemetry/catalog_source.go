package telemetry

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
)

// catalogJSON is the language-neutral source consumed by Go and the C++
// binding generator. Keep the source in JSON so both consumers can use it
// without an additional YAML/protobuf parser dependency.
//
//go:embed schema/catalog.json
var catalogJSON []byte

//go:generate go run ./cmd/cataloggen -input schema/catalog.json -output ../../RemoteCodex/native/video-engine-cpp/include/velox/telemetry/catalog_generated.hpp -go-output generated/catalog_gen.go

// CatalogJSON returns a defensive copy of the language-neutral source. The
// C++ generator uses it to fail closed when its input path is not the exact
// catalog embedded by the Go package.
func CatalogJSON() []byte { return append([]byte(nil), catalogJSON...) }

type languageNeutralCatalog struct {
	SchemaVersion int                    `json:"schema_version"`
	Schema        languageNeutralSchema  `json:"schema"`
	Facts         []languageNeutralFact  `json:"facts"`
	Events        []languageNeutralEvent `json:"events"`
}

type languageNeutralSchema struct {
	EventKey            string   `json:"event_key"`
	RequiredEventFields []string `json:"required_event_fields"`
	Origins             []string `json:"origins"`
	Scopes              []string `json:"scopes"`
	Phases              []string `json:"phases"`
	PhaseOrder          []string `json:"phase_order"`
	// PhaseTaxonomy models the 12 canonical lifecycle phases as a tree:
	// each phase declares its accounted role (exclusive | span_parent |
	// span_child) and, when nested, its parent phase. Only phases with
	// role=exclusive are summed into accounted_ratio — span parents and
	// children overlap by construction and would double-count against the
	// wall clock.
	PhaseTaxonomy       map[string]languageNeutralPhaseTaxon `json:"phase_taxonomy"`
	Units               []string                             `json:"units"`
	Kinds               []string                             `json:"kinds"`
	TimingModes         []string                             `json:"timing_modes"`
	Aggregations        []string                             `json:"aggregations"`
	CardinalityPolicies []string                             `json:"cardinality_policies"`
	Owners              []string                             `json:"owners"`
	AccountedRatioRule  string                               `json:"accounted_ratio_rule"`
}

type languageNeutralPhaseTaxon struct {
	// Role is the phase's accounted role in the canonical taxonomy.
	// Values reuse the closed timing_mode vocabulary: exclusive
	// (top-level, summed into accounted_ratio), span_parent / span_child
	// (nested, NEVER summed).
	Role string `json:"role"`
	// Parent is the containing phase; empty for top-level phases. A phase
	// with a span role MUST declare a parent; an exclusive phase MUST NOT.
	Parent string `json:"parent,omitempty"`
}

type languageNeutralFact struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

type languageNeutralEvent struct {
	Key         string `json:"key"`
	Component   string `json:"component"`
	Action      string `json:"action"`
	Origin      string `json:"origin"`
	Scope       string `json:"scope"`
	Phase       string `json:"phase"`
	EventType   string `json:"event_type"`
	Kind        string `json:"kind"`
	Unit        string `json:"unit"`
	TimingMode  string `json:"timing_mode"`
	Aggregation string `json:"aggregation"`
	Cardinality string `json:"cardinality"`
	Owner       string `json:"owner"`
}

func parseLanguageNeutralCatalog() (languageNeutralCatalog, error) {
	var doc languageNeutralCatalog
	if err := json.Unmarshal(catalogJSON, &doc); err != nil {
		return doc, fmt.Errorf("telemetry catalog JSON: %w", err)
	}
	if err := validateLanguageNeutralCatalog(doc); err != nil {
		return doc, err
	}
	return doc, nil
}

func mustLanguageNeutralCatalog() languageNeutralCatalog {
	doc, err := parseLanguageNeutralCatalog()
	if err != nil {
		panic(err)
	}
	return doc
}

func validateLanguageNeutralCatalog(doc languageNeutralCatalog) error {
	if doc.SchemaVersion != int(SchemaVersion) {
		return fmt.Errorf("telemetry catalog schema_version=%d, want %d", doc.SchemaVersion, SchemaVersion)
	}
	if doc.Schema.EventKey != "component.action" {
		return fmt.Errorf("telemetry catalog event_key=%q, want component.action", doc.Schema.EventKey)
	}
	if doc.Schema.AccountedRatioRule == "" {
		return fmt.Errorf("telemetry catalog accounted_ratio_rule is required")
	}
	if err := validateSchemaVocabularies(doc.Schema); err != nil {
		return err
	}
	if err := validatePhaseTaxonomy(doc.Schema, doc.Events); err != nil {
		return err
	}
	if len(doc.Events) == 0 {
		return fmt.Errorf("telemetry catalog contains no events")
	}
	seenEvents := make(map[string]struct{}, len(doc.Events))
	for _, event := range doc.Events {
		if event.Component == "" || event.Action == "" {
			return fmt.Errorf("telemetry catalog event has empty component/action")
		}
		wantKey := event.Component + "." + event.Action
		if event.Key != wantKey {
			return fmt.Errorf("telemetry catalog event key=%q, want %q", event.Key, wantKey)
		}
		if _, exists := seenEvents[event.Key]; exists {
			return fmt.Errorf("telemetry catalog duplicate event %q", event.Key)
		}
		seenEvents[event.Key] = struct{}{}
		if !containsString(doc.Schema.Origins, event.Origin) ||
			!containsString(doc.Schema.Scopes, event.Scope) ||
			!containsString(doc.Schema.Phases, event.Phase) ||
			!containsString(doc.Schema.Kinds, event.Kind) ||
			!containsString(doc.Schema.Units, event.Unit) ||
			!containsString(doc.Schema.TimingModes, event.TimingMode) ||
			!containsString(doc.Schema.Aggregations, event.Aggregation) ||
			!containsString(doc.Schema.CardinalityPolicies, event.Cardinality) ||
			!containsString(doc.Schema.Owners, event.Owner) {
			return fmt.Errorf("telemetry catalog event %q uses a value absent from the declared schema vocabulary", event.Key)
		}
		descriptor := EventDescriptor{
			Component:   event.Component,
			Action:      event.Action,
			Origin:      event.Origin,
			Scope:       event.Scope,
			Phase:       event.Phase,
			EventType:   event.EventType,
			Kind:        EventKind(event.Kind),
			Unit:        MetricUnit(event.Unit),
			TimingMode:  TimingMode(event.TimingMode),
			Aggregation: AggregationMode(event.Aggregation),
			Cardinality: CardinalityPolicy(event.Cardinality),
			Owner:       ComponentOwner(event.Owner),
		}
		if err := ValidateEventDescriptorSemantics(descriptor); err != nil {
			return err
		}
	}
	seenFacts := make(map[string]struct{}, len(doc.Facts))
	for _, fact := range doc.Facts {
		if fact.Name == "" || !containsString(doc.Schema.Owners, fact.Owner) || !IsCanonicalComponentOwner(fact.Owner) {
			return fmt.Errorf("telemetry catalog fact %q has invalid owner %q", fact.Name, fact.Owner)
		}
		if _, exists := seenFacts[fact.Name]; exists {
			return fmt.Errorf("telemetry catalog duplicate fact %q", fact.Name)
		}
		seenFacts[fact.Name] = struct{}{}
	}
	return nil
}

func validateSchemaVocabularies(schema languageNeutralSchema) error {
	requiredFields := []string{"component", "action", "origin", "scope", "phase", "event_type", "kind", "unit", "timing_mode", "aggregation", "cardinality", "owner"}
	if !sameStringSet(schema.RequiredEventFields, requiredFields) {
		return fmt.Errorf("telemetry catalog required_event_fields diverge from the descriptor contract: got=%v want=%v", schema.RequiredEventFields, requiredFields)
	}
	vocabularies := []struct {
		name string
		got  []string
		want []string
	}{
		{name: "origins", got: schema.Origins, want: []string{string(OriginEngine), string(OriginFFmpeg), string(OriginMaster), string(OriginUpload), string(OriginValidation), string(OriginWorker)}},
		{name: "scopes", got: schema.Scopes, want: []string{string(ScopeArtifact), string(ScopeAttempt), string(ScopeAudioTrack), string(ScopeJob), string(ScopeSegment), string(ScopeSubtitleTrack), string(ScopeTask)}},
		{name: "units", got: schema.Units, want: []string{string(UnitCount), string(UnitMilliseconds), string(UnitBytes), string(UnitRatio), string(UnitItems), string(UnitFrames)}},
		{name: "kinds", got: schema.Kinds, want: []string{string(KindCounter), string(KindGauge), string(KindDuration), string(KindHistogram), string(KindSpan)}},
		{name: "timing_modes", got: schema.TimingModes, want: []string{string(TimingNone), string(TimingExclusive), string(TimingSpanParent), string(TimingSpanChild)}},
		{name: "aggregations", got: schema.Aggregations, want: []string{string(AggSum), string(AggMax), string(AggMin), string(AggAvg), string(AggLast), string(AggCount)}},
		{name: "cardinality_policies", got: schema.CardinalityPolicies, want: []string{string(CardPerJob), string(CardPerTask), string(CardPerAttempt), string(CardPerSegment), string(CardPerAsset), string(CardPerArtifact), string(CardPerTrack)}},
		{name: "owners", got: schema.Owners, want: []string{string(OwnerMaster), string(OwnerWorker), string(OwnerTaskRunner), string(OwnerAttemptTelemetry), string(OwnerDownloader), string(OwnerCacheResolver), string(OwnerProcessRunner), string(OwnerMediaEngine), string(OwnerDecoder), string(OwnerEncoder), string(OwnerMuxer), string(OwnerPublisher), string(OwnerUploader), string(OwnerValidation), string(OwnerRenderPlan), string(OwnerAssetManifest), string(OwnerWorkerState)}},
	}
	for _, vocabulary := range vocabularies {
		if !sameStringSet(vocabulary.got, vocabulary.want) {
			return fmt.Errorf("telemetry catalog schema vocabulary %s diverges from Go bindings: got=%v want=%v", vocabulary.name, vocabulary.got, vocabulary.want)
		}
	}
	if len(schema.Phases) == 0 {
		return fmt.Errorf("telemetry catalog schema vocabulary phases must not be empty")
	}
	if !sameStringSet(schema.PhaseOrder, schema.Phases) {
		return fmt.Errorf("telemetry catalog phase_order must contain each canonical phase exactly once: got=%v want=%v", schema.PhaseOrder, schema.Phases)
	}
	return nil
}

// validatePhaseTaxonomy enforces the canonical 12-phase model declared in
// catalog.json:
//
//   - the taxonomy keys are EXACTLY the schema.phases vocabulary (every
//     canonical phase is modeled, no phase is invented);
//   - each phase role is a member of the closed timing vocabulary
//     (exclusive | span_parent | span_child — never none);
//   - span roles declare a parent (a known phase), exclusive roles never
//     do, and the parent chain is acyclic;
//   - event-level consistency: a timing_mode=exclusive event can only live
//     under an exclusive-role phase (the accounted_ratio denominator is
//     always a top-level phase).
//
// A taxonomy violation is a startup failure: accounted_ratio semantics must
// never depend on a half-modeled phase list.
func validatePhaseTaxonomy(schema languageNeutralSchema, events []languageNeutralEvent) error {
	if len(schema.PhaseTaxonomy) == 0 {
		return fmt.Errorf("telemetry catalog phase_taxonomy is required")
	}
	if !sameStringSet(keysOf(schema.PhaseTaxonomy), schema.Phases) {
		return fmt.Errorf("telemetry catalog phase_taxonomy keys diverge from the phases vocabulary: got=%v want=%v", keysOf(schema.PhaseTaxonomy), schema.Phases)
	}
	phaseSet := make(map[string]struct{}, len(schema.Phases))
	for _, phase := range schema.Phases {
		phaseSet[phase] = struct{}{}
	}
	for phase, taxon := range schema.PhaseTaxonomy {
		switch TimingMode(taxon.Role) {
		case TimingExclusive:
			if taxon.Parent != "" {
				return fmt.Errorf("telemetry catalog phase %q: exclusive role must not declare a parent, got %q", phase, taxon.Parent)
			}
		case TimingSpanParent, TimingSpanChild:
			if taxon.Parent == "" {
				return fmt.Errorf("telemetry catalog phase %q: span role %q must declare a parent", phase, taxon.Role)
			}
			if taxon.Parent == phase {
				return fmt.Errorf("telemetry catalog phase %q: parent must not be the phase itself", phase)
			}
			if _, ok := phaseSet[taxon.Parent]; !ok {
				return fmt.Errorf("telemetry catalog phase %q: unknown parent %q", phase, taxon.Parent)
			}
			// Single-level check is enough for the canonical model, but keep
			// a generic cycle guard so a deeper edit cannot deadlock the
			// taxonomy walk.
			for cursor := taxon.Parent; cursor != ""; cursor = schema.PhaseTaxonomy[cursor].Parent {
				if cursor == phase {
					return fmt.Errorf("telemetry catalog phase taxonomy cycle involving %q", phase)
				}
			}
		default:
			return fmt.Errorf("telemetry catalog phase %q has invalid role %q (exclusive|span_parent|span_child)", phase, taxon.Role)
		}
	}
	// Event-level cross-check: an exclusive (per_attempt) event is the
	// top-level fact OF an exclusive-role phase. A catalog that stamps
	// exclusive events inside span phases would double-count against the
	// wall clock and break accounted_ratio.
	for _, event := range events {
		if event.TimingMode != string(TimingExclusive) || event.Phase == "" {
			continue
		}
		taxon, ok := schema.PhaseTaxonomy[event.Phase]
		if !ok {
			return fmt.Errorf("telemetry catalog event %q: exclusive timing under unknown phase %q", event.Key, event.Phase)
		}
		if taxon.Role != string(TimingExclusive) {
			return fmt.Errorf("telemetry catalog event %q: exclusive timing under non-exclusive phase %q (role=%s)", event.Key, event.Phase, taxon.Role)
		}
	}
	return nil
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, value := range want {
		if !containsString(got, value) {
			return false
		}
	}
	return true
}

func loadCanonicalEventDescriptors() []EventDescriptor {
	doc := mustLanguageNeutralCatalog()
	out := make([]EventDescriptor, 0, len(doc.Events))
	for _, event := range doc.Events {
		out = append(out, EventDescriptor{
			Component:   event.Component,
			Action:      event.Action,
			Origin:      event.Origin,
			Scope:       event.Scope,
			Phase:       event.Phase,
			EventType:   event.EventType,
			Kind:        EventKind(event.Kind),
			Unit:        MetricUnit(event.Unit),
			TimingMode:  TimingMode(event.TimingMode),
			Aggregation: AggregationMode(event.Aggregation),
			Cardinality: CardinalityPolicy(event.Cardinality),
			Owner:       ComponentOwner(event.Owner),
		})
	}
	return out
}

func loadCanonicalFactOwners() map[string]ComponentOwner {
	doc := mustLanguageNeutralCatalog()
	out := make(map[string]ComponentOwner, len(doc.Facts))
	for _, fact := range doc.Facts {
		out[fact.Name] = ComponentOwner(fact.Owner)
	}
	return out
}
