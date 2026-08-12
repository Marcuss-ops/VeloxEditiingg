// Package telemetry contains the immutable wire contract shared by workers
// and the master. Producers and consumers must validate event taxonomy through
// this catalog instead of maintaining local origin/scope lists.
//
// Single-source rule: the canonical taxonomy is EXACTLY ONE language-neutral
// source — schema/catalog.json. Go loads it into canonicalEventDescriptors; the C++
// binding generator emits its header from the same source. Every consumer
// (master validation, worker phase registry, SQL CHECK projections) derives
// from that catalog; adding an event means editing schema/catalog.json, never a
// parallel registry.
package telemetry

import (
	"fmt"
)

// SchemaVersion is the version of the event taxonomy carried by TaskResult.
const SchemaVersion int32 = 1

// EventDescriptor is the single canonical taxonomy entry. Key() is
// "component.action"; the remaining fields are the authoritative wire
// attributes every consumer derives:
//
//	Origin       — closed producer vocabulary (master | worker | engine | ...)
//	Scope        — closed resource vocabulary (job | task | attempt | ...)
//	Phase        — canonical worker-side lifecycle phase (queue | render | ...)
//	              or "" when the event has no phase projection.
//	EventType    — default event_type stamped by the worker when the producer
//	              omits it ("" = leave producer default).
//
// The semantic attributes (descriptor.go) complete the entry at declaration
// time so no consumer needs a second registry or a derivation switch:
//
//	Kind         — what the fact measures (counter | gauge | duration | ...)
//	TimingMode   — how its timing is accounted (none | exclusive | span_*)
//	Aggregation  — how observations combine (sum | max | last | ...)
//	Cardinality  — the resource dimension bounding occurrence (per_segment | ...)
//	Owner        — the single authoritative producer (Fact Owner rule)
type EventDescriptor struct {
	Component string
	Action    string
	Origin    string
	Scope     string
	Phase     string
	EventType string

	Kind        EventKind
	Unit        MetricUnit
	TimingMode  TimingMode
	Aggregation AggregationMode
	Cardinality CardinalityPolicy
	Owner       ComponentOwner
}

func (d EventDescriptor) Key() string { return d.Component + "." + d.Action }

// TelemetryEventSpec is the catalog entry as exposed to validation callers.
// It is the EventDescriptor plus the immutable schema version. Kept as a
// distinct name for the wire-facing API; the fields are populated from the
// single canonicalEventDescriptors list.
type TelemetryEventSpec struct {
	Origin        string
	Scope         string
	Component     string
	Action        string
	SchemaVersion int32
	// Phase and EventType are derived attributes of the canonical event.
	// They are carried on the spec so master-side validation and the
	// worker phase registry share one source.
	Phase     string
	EventType string

	// Semantic attributes resolved from the canonical descriptor. They are
	// informational attributes OF the registered entry — validation of a
	// producer event never requires them, projections read them from here.
	Kind        EventKind
	Unit        MetricUnit
	TimingMode  TimingMode
	Aggregation AggregationMode
	Cardinality CardinalityPolicy
	Owner       ComponentOwner
}

func (s TelemetryEventSpec) Key() string { return s.Component + "." + s.Action }

// TelemetryEventCatalog is a read-only taxonomy registry. Its map is private
// and all accessors return copies, so worker and master cannot mutate the
// shared contract at runtime.
type TelemetryEventCatalog struct {
	entries map[string]TelemetryEventSpec
}

// Catalog is the single canonical registry used by both modules.
var Catalog = newTelemetryEventCatalog()

func newTelemetryEventCatalog() *TelemetryEventCatalog {
	entries := make(map[string]TelemetryEventSpec, len(canonicalEventDescriptors))
	for _, d := range canonicalEventDescriptors {
		if d.Component == "" || d.Action == "" {
			panic("telemetry: empty component/action in canonical descriptor")
		}
		if d.Origin == "" || d.Scope == "" {
			panic("telemetry: missing origin/scope for " + d.Key())
		}
		if err := ValidateEventDescriptorSemantics(d); err != nil {
			panic(err.Error())
		}
		if _, exists := entries[d.Key()]; exists {
			panic("telemetry: duplicate canonical descriptor " + d.Key())
		}
		entries[d.Key()] = TelemetryEventSpec{
			Origin: d.Origin, Scope: d.Scope, Component: d.Component,
			Action: d.Action, SchemaVersion: SchemaVersion,
			Phase: d.Phase, EventType: d.EventType,
			Kind: d.Kind, Unit: d.Unit, TimingMode: d.TimingMode,
			Aggregation: d.Aggregation, Cardinality: d.Cardinality,
			Owner: d.Owner,
		}
	}
	return &TelemetryEventCatalog{entries: entries}
}

// Lookup returns the canonical registration for a component/action pair.
func (c *TelemetryEventCatalog) Lookup(component, action string) (TelemetryEventSpec, bool) {
	if c == nil {
		return TelemetryEventSpec{}, false
	}
	spec, ok := c.entries[component+"."+action]
	return spec, ok
}

// Validate checks the complete taxonomy tuple. SchemaVersion zero is accepted
// for legacy callers and normalized by Normalize; non-zero unknown versions
// are rejected before the event can cross the wire boundary.
func (c *TelemetryEventCatalog) Validate(spec TelemetryEventSpec) error {
	if spec.Component == "" || spec.Action == "" {
		return fmt.Errorf("telemetry event requires component and action")
	}
	registered, ok := c.Lookup(spec.Component, spec.Action)
	if !ok {
		return fmt.Errorf("unregistered component/action %q.%q", spec.Component, spec.Action)
	}
	if spec.Origin != registered.Origin || spec.Scope != registered.Scope {
		return fmt.Errorf("origin/scope mismatch for %q.%q: got %q/%q, want %q/%q",
			spec.Component, spec.Action, spec.Origin, spec.Scope, registered.Origin, registered.Scope)
	}
	if spec.SchemaVersion != 0 && spec.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported telemetry schema version %d", spec.SchemaVersion)
	}
	return nil
}

// Normalize validates and fills the authoritative schema version.
func (c *TelemetryEventCatalog) Normalize(spec *TelemetryEventSpec) error {
	if spec == nil {
		return fmt.Errorf("nil telemetry event")
	}
	if err := c.Validate(*spec); err != nil {
		return err
	}
	spec.SchemaVersion = SchemaVersion
	return nil
}

// Entries returns a defensive copy of the catalog.
func (c *TelemetryEventCatalog) Entries() map[string]TelemetryEventSpec {
	out := make(map[string]TelemetryEventSpec)
	if c == nil {
		return out
	}
	for key, spec := range c.entries {
		out[key] = spec
	}
	return out
}

func (c *TelemetryEventCatalog) Count() int {
	if c == nil {
		return 0
	}
	return len(c.entries)
}

const (
	OriginMaster     = "master"
	OriginWorker     = "worker"
	OriginEngine     = "engine"
	OriginFFmpeg     = "ffmpeg"
	OriginUpload     = "upload"
	OriginValidation = "validation"

	ScopeJob           = "job"
	ScopeTask          = "task"
	ScopeAttempt       = "attempt"
	ScopeSegment       = "segment"
	ScopeAudioTrack    = "audio_track"
	ScopeSubtitleTrack = "subtitle_track"
	ScopeArtifact      = "artifact"
)

// canonicalEventDescriptors is loaded from the language-neutral JSON source.
// Go and C++ consume the same shared/telemetry/schema/catalog.json; the C++ binding is
// generated from that file by telemetry/cmd/cataloggen.
var canonicalEventDescriptors = loadCanonicalEventDescriptors()
