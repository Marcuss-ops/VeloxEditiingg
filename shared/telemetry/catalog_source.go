package telemetry

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

// catalogJSON is the language-neutral source consumed by Go and the C++
// binding generator. Keep the source in JSON so both consumers can use it
// without an additional YAML/protobuf parser dependency.
//
//go:embed catalog.json
var catalogJSON []byte

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
	Units               []string `json:"units"`
	Kinds               []string `json:"kinds"`
	TimingModes         []string `json:"timing_modes"`
	Aggregations        []string `json:"aggregations"`
	CardinalityPolicies []string `json:"cardinality_policies"`
	Owners              []string `json:"owners"`
	AccountedRatioRule  string   `json:"accounted_ratio_rule"`
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
		if fact.Name == "" || !IsCanonicalComponentOwner(fact.Owner) {
			return fmt.Errorf("telemetry catalog fact %q has invalid owner %q", fact.Name, fact.Owner)
		}
		if _, exists := seenFacts[fact.Name]; exists {
			return fmt.Errorf("telemetry catalog duplicate fact %q", fact.Name)
		}
		seenFacts[fact.Name] = struct{}{}
	}
	return nil
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
