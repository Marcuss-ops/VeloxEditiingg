// phase_registry.go — closed canonical observability taxonomy.
//
// This file is the worker-side VIEW of the single canonical event taxonomy.
// The taxonomy itself lives in shared/telemetry/catalog.json (the language-
// neutral source loaded by shared/telemetry and compiled to the C++ binding
// by cataloggen). The worker does NOT maintain a second registry: adding an
// event means editing shared/telemetry/catalog.json, and every worker lookup
// derives from that shared catalog. This removes the dual-registry drift that
// forced fixes in both phase_registry.go and shared/telemetry/catalog.go.
//
// Origins, scopes, phases and event types mirror the SQL CHECK constraints in
// migration 110. Component/action pairs are deliberately closed: producers
// must use a catalog entry rather than inventing a label at runtime. The
// shared catalog is initialized once and only exposed through defensive
// copies/lookups.
package telemetry

import (
	"fmt"

	sharedtelemetry "velox-shared/telemetry"
)

// SchemaVersion is the shared event catalog version emitted by this worker.
const SchemaVersion = sharedtelemetry.SchemaVersion

// ── Closed origin enum ─────────────────────────────────────────────────────
const (
	OriginMaster     = sharedtelemetry.OriginMaster
	OriginWorker     = sharedtelemetry.OriginWorker
	OriginEngine     = sharedtelemetry.OriginEngine
	OriginFFmpeg     = sharedtelemetry.OriginFFmpeg
	OriginUpload     = sharedtelemetry.OriginUpload
	OriginValidation = sharedtelemetry.OriginValidation
)

// canonicalOrigins is private so callers cannot mutate the taxonomy.
var canonicalOrigins = [...]string{
	sharedtelemetry.OriginMaster,
	sharedtelemetry.OriginWorker,
	sharedtelemetry.OriginEngine,
	sharedtelemetry.OriginFFmpeg,
	sharedtelemetry.OriginUpload,
	sharedtelemetry.OriginValidation,
}

// CanonicalOrigins returns a defensive copy of the closed origin vocabulary.
func CanonicalOrigins() []string {
	return append([]string(nil), canonicalOrigins[:]...)
}

var originSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(canonicalOrigins))
	for _, origin := range canonicalOrigins {
		m[origin] = struct{}{}
	}
	return m
}()

// IsCanonicalOrigin reports whether s is a member of the closed origin enum.
func IsCanonicalOrigin(s string) bool {
	_, ok := originSet[s]
	return ok
}

// ── Closed scope enum ──────────────────────────────────────────────────────
const (
	ScopeJob           = sharedtelemetry.ScopeJob
	ScopeTask          = sharedtelemetry.ScopeTask
	ScopeAttempt       = sharedtelemetry.ScopeAttempt
	ScopeSegment       = sharedtelemetry.ScopeSegment
	ScopeAudioTrack    = sharedtelemetry.ScopeAudioTrack
	ScopeSubtitleTrack = sharedtelemetry.ScopeSubtitleTrack
	ScopeArtifact      = sharedtelemetry.ScopeArtifact
)

// canonicalScopes is private so callers cannot mutate the taxonomy.
var canonicalScopes = [...]string{
	sharedtelemetry.ScopeJob,
	sharedtelemetry.ScopeTask,
	sharedtelemetry.ScopeAttempt,
	sharedtelemetry.ScopeSegment,
	sharedtelemetry.ScopeAudioTrack,
	sharedtelemetry.ScopeSubtitleTrack,
	sharedtelemetry.ScopeArtifact,
}

// CanonicalScopes returns a defensive copy of the closed scope vocabulary.
func CanonicalScopes() []string {
	return append([]string(nil), canonicalScopes[:]...)
}

var scopeSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(canonicalScopes))
	for _, scope := range canonicalScopes {
		m[scope] = struct{}{}
	}
	return m
}()

// IsCanonicalScope reports whether s is a member of the closed scope enum.
func IsCanonicalScope(s string) bool {
	_, ok := scopeSet[s]
	return ok
}

// PhaseSpec is the worker-side projection of one canonical catalog entry.
// Component may contain dotted namespaces (for example "engine.audio");
// Action is the final stable operation name used in SQL and Prometheus-safe
// projections. Phase, EventType and the semantic attributes (Kind, TimingMode,
// Aggregation, Cardinality, Owner) come from the shared catalog — the worker
// never declares them locally.
type PhaseSpec struct {
	Origin    string
	Scope     string
	Component string
	Action    string
	Phase     string
	EventType string

	// Semantic attributes resolved from the single shared catalog. Owner is
	// the Fact Owner: the single authoritative producer of the fact — no
	// projection may reconstruct a fact owned by another component.
	Kind        sharedtelemetry.EventKind
	TimingMode  sharedtelemetry.TimingMode
	Aggregation sharedtelemetry.AggregationMode
	Cardinality sharedtelemetry.CardinalityPolicy
	Owner       sharedtelemetry.ComponentOwner
}

// Key returns the stable registry key "component.action".
func (p PhaseSpec) Key() string { return p.Component + "." + p.Action }

// validateSharedCatalog runs once at init: every shared catalog entry must
// use closed worker origins/scopes and a canonical phase (when set). A broken
// shared catalog is a link-time contract violation, surfaced as a panic
// instead of silently dropping events.
func validateSharedCatalog() {
	for key, spec := range sharedtelemetry.Catalog.Entries() {
		if !IsCanonicalOrigin(spec.Origin) {
			panic("telemetry: invalid canonical origin for " + key)
		}
		if !IsCanonicalScope(spec.Scope) {
			panic("telemetry: invalid canonical scope for " + key)
		}
		if spec.Phase != "" && !IsCanonical(spec.Phase) {
			panic("telemetry: invalid canonical phase for " + key)
		}
	}
}

func init() { validateSharedCatalog() }

// LookupPhaseSpec returns the immutable canonical specification for a
// component/action pair, derived from the single shared catalog.
func LookupPhaseSpec(component, action string) (PhaseSpec, bool) {
	spec, ok := sharedtelemetry.Catalog.Lookup(component, action)
	if !ok {
		return PhaseSpec{}, false
	}
	return PhaseSpec{
		Origin: spec.Origin, Scope: spec.Scope,
		Component: spec.Component, Action: spec.Action,
		Phase: spec.Phase, EventType: spec.EventType,
		Kind: spec.Kind, TimingMode: spec.TimingMode,
		Aggregation: spec.Aggregation, Cardinality: spec.Cardinality,
		Owner: spec.Owner,
	}, true
}

// LookupCanonicalPhaseSpec is the explicit name for new callers.
func LookupCanonicalPhaseSpec(component, action string) (PhaseSpec, bool) {
	return LookupPhaseSpec(component, action)
}

// RegisteredPhaseSpecs returns a defensive copy of the shared catalog
// projected to the worker PhaseSpec shape.
func RegisteredPhaseSpecs() map[string]PhaseSpec {
	shared := sharedtelemetry.Catalog.Entries()
	out := make(map[string]PhaseSpec, len(shared))
	for key, spec := range shared {
		out[key] = PhaseSpec{
			Origin: spec.Origin, Scope: spec.Scope,
			Component: spec.Component, Action: spec.Action,
			Phase: spec.Phase, EventType: spec.EventType,
			Kind: spec.Kind, TimingMode: spec.TimingMode,
			Aggregation: spec.Aggregation, Cardinality: spec.Cardinality,
			Owner: spec.Owner,
		}
	}
	return out
}

// CanonicalPhaseSpecCount returns the number of registered component/action
// pairs in the single shared catalog.
func CanonicalPhaseSpecCount() int { return sharedtelemetry.Catalog.Count() }

// CanonicalizeEventSpec validates and stamps the authoritative origin, scope,
// phase and default event type for a producer event. Unknown component/action
// pairs return false and must not be emitted. The shared catalog is the sole
// authority for the complete tuple.
func CanonicalizeEventSpec(spec *EventSpec) bool {
	if spec == nil {
		return false
	}
	sharedSpec := sharedtelemetry.TelemetryEventSpec{
		Origin: spec.Origin, Scope: spec.Scope, Component: spec.Component,
		Action: spec.Action, SchemaVersion: spec.SchemaVersion,
	}
	if err := sharedtelemetry.Catalog.Normalize(&sharedSpec); err != nil {
		return false
	}
	canonical, ok := LookupPhaseSpec(spec.Component, spec.Action)
	if !ok {
		return false
	}
	spec.SchemaVersion = sharedSpec.SchemaVersion
	spec.Phase = canonical.Phase
	if spec.EventType == "" {
		spec.EventType = canonical.EventType
	}
	return true
}

// String renders the spec for debug logs.
func (p PhaseSpec) String() string {
	return fmt.Sprintf("%s/%s %s.%s", p.Origin, p.Scope, p.Component, p.Action)
}
