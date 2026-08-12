package telemetry

import (
	"testing"
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

// TestEventRecorder_BeginComplete verifies the begin/complete lifecycle:
// duration is measured on a monotonic base, wall stamps are UTC, and a
// completed event carries the caller-supplied status.
func TestEventRecorder_BeginComplete(t *testing.T) {
	r := NewEventRecorder()
	h := r.Begin(EventSpec{
		Origin:    OriginWorker,
		Scope:     ScopeAttempt,
		Component: "runner",
		Action:    "cache_lookup",
		Phase:     "cache_lookup",
	})
	time.Sleep(2 * time.Millisecond)
	h.CompleteWith(0, 0, 0, StatusOK, "", "")

	events := r.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	e := events[0]
	if e.EventIndex != 0 {
		t.Errorf("expected event_index 0, got %d", e.EventIndex)
	}
	if e.DurationMS < 1 {
		t.Errorf("expected duration_ms >= 1 after sleep, got %d", e.DurationMS)
	}
	if e.Status != StatusOK || e.EventType != "completed" {
		t.Errorf("expected status=ok event_type=completed, got %q/%q", e.Status, e.EventType)
	}
	if !e.StartedAt.Equal(e.StartedAt.UTC()) {
		t.Errorf("started_at should be UTC, got %v", e.StartedAt.Location())
	}
	if e.CompletedAt.Before(e.StartedAt) {
		t.Errorf("completed_at %v before started_at %v", e.CompletedAt, e.StartedAt)
	}
}

// TestEventRecorder_AbortAndEventType verifies that Abort yields a
// "failed" event type and that Begin on a nil recorder is a safe no-op.
func TestEventRecorder_AbortAndEventType(t *testing.T) {
	r := NewEventRecorder()
	h := r.Begin(EventSpec{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.encode", Action: "setup"})
	h.Abort("EIO", "disk full")
	events := r.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Status != StatusFailed || events[0].EventType != "failed" {
		t.Errorf("expected status=failed event_type=failed, got %q/%q", events[0].Status, events[0].EventType)
	}
	if events[0].ErrorCode != "EIO" || events[0].ErrorMessage != "disk full" {
		t.Errorf("expected error code/message, got %q/%q", events[0].ErrorCode, events[0].ErrorMessage)
	}

	var nilRec *EventRecorder
	if h2 := nilRec.Begin(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt}); h2 != nil {
		t.Error("Begin on nil recorder should return nil handle")
	}
	nilRec.Emit(EventSpec{}, "ok", "", "")
	nilRec.Record(EventSpec{}, time.Now(), time.Now(), 0, "ok", "", "")
	nilRec.Flush()
}

// TestEventRecorder_PerOriginIndexes verifies that event_index increments
// per origin: mixing origins keeps each counter independent (mirroring
// the master's UNIQUE(attempt_id, origin, event_index) guard).
func TestEventRecorder_PerOriginIndexes(t *testing.T) {
	r := NewEventRecorder()
	r.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "cache_lookup"}, "ok", "", "")
	r.Emit(EventSpec{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.encode", Action: "setup"}, "ok", "", "")
	r.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "execute"}, "ok", "", "")

	events := r.Flush()
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	got := map[string]int64{}
	for _, e := range events {
		got[e.Origin] = e.EventIndex
	}
	if got[OriginWorker] != 1 {
		t.Errorf("expected worker index 1, got %d", got[OriginWorker])
	}
	if got[OriginEngine] != 0 {
		t.Errorf("expected engine index 0, got %d", got[OriginEngine])
	}
}

// TestEventRecorder_RetainsNonCanonical verifies that invalid origin,
// scope, and component/action values remain available for master quarantine.
func TestEventRecorder_RetainsNonCanonical(t *testing.T) {
	r := NewEventRecorder()
	before := GetPrometheusMetrics().telemetryInvalidEvents.total()
	r.Emit(EventSpec{Origin: "bogus", Scope: "nope", Component: "runner", Action: "cache_lookup"}, "ok", "", "")
	r.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "unknown"}, "ok", "", "")
	events := r.Flush()
	if len(events) != 2 {
		t.Fatalf("invalid taxonomy must remain available for quarantine, got %d events", len(events))
	}
	if events[0].Origin != "bogus" || events[0].Scope != "nope" || events[1].Action != "unknown" {
		t.Fatalf("raw invalid taxonomy was not preserved: %+v", events)
	}
	if GetPrometheusMetrics().telemetryInvalidEvents.total() < before+2 {
		t.Fatalf("invalid telemetry counter did not increase: before=%v after=%v", before, GetPrometheusMetrics().telemetryInvalidEvents.total())
	}
}

// TestEventRecorder_RecordExplicitStamps verifies the Record API: the
// caller's wall stamps and duration pass through untouched.
func TestEventRecorder_InvalidSchemaVersionIsForwarded(t *testing.T) {
	r := NewEventRecorder()
	r.Emit(EventSpec{
		Origin: OriginWorker, Scope: ScopeAttempt,
		Component: "runner", Action: "execute", SchemaVersion: SchemaVersion + 1,
	}, StatusOK, "", "")
	events := r.Flush()
	if len(events) != 1 || events[0].SchemaVersion != SchemaVersion+1 {
		t.Fatalf("invalid schema version was not preserved: %+v", events)
	}
}

func TestEventRecorder_InvalidStartedEventIsForwarded(t *testing.T) {
	r := NewEventRecorder()
	h := r.Start(EventSpec{
		Origin: OriginWorker, Scope: ScopeAttempt,
		Component: "runner", Action: "not_registered",
	})
	if h == nil {
		t.Fatal("invalid event must still receive a completion handle")
	}
	h.Complete()
	events := r.Flush()
	if len(events) != 1 || events[0].Action != "not_registered" {
		t.Fatalf("invalid started event was not preserved: %+v", events)
	}
}

func TestEventRecorder_RecordExplicitStamps(t *testing.T) {
	r := NewEventRecorder()
	start := time.Date(2026, 7, 31, 10, 0, 0, 0, time.UTC)
	end := start.Add(150 * time.Millisecond)
	r.Record(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "upload", Phase: "upload"}, start, end, 150, StatusOK, "", "")

	events := r.Flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].StartedAt != start || events[0].CompletedAt != end {
		t.Errorf("stamps not preserved: got %v..%v want %v..%v", events[0].StartedAt, events[0].CompletedAt, start, end)
	}
	if events[0].DurationMS != 150 {
		t.Errorf("expected duration_ms 150, got %d", events[0].DurationMS)
	}
}

// TestEventRecorderSnapshotsAreAppendOnly verifies that Flush is a
// non-destructive compatibility snapshot and incremental projections use an
// offset without deleting the Attempt journal.
func TestEventRecorderSnapshotsAreAppendOnly(t *testing.T) {
	r := NewEventRecorder()
	r.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "cache_lookup"}, "ok", "", "")
	first := r.Flush()
	if len(first) != 1 || first[0].EventIndex != 0 {
		t.Fatalf("first snapshot = %+v, want one event at index 0", first)
	}
	r.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "cache_lookup"}, "ok", "", "")
	second := r.Flush()
	if len(second) != 2 {
		t.Fatalf("expected 2 events in non-destructive snapshot, got %d", len(second))
	}
	if second[1].EventIndex != 1 {
		t.Errorf("expected monotonic index 1 after append, got %d", second[1].EventIndex)
	}
	incremental := r.SnapshotFrom(1)
	if len(incremental) != 1 || incremental[0].EventIndex != 1 {
		t.Fatalf("incremental snapshot = %+v, want only post-offset event", incremental)
	}
	if got := len(r.Snapshot()); got != 2 {
		t.Fatalf("append-only journal length = %d after projection, want 2", got)
	}
}

// TestEventRecorderImportCXXPreservesIndexes verifies the official native
// import boundary: catalog defaults are filled, engine indexes are retained,
// later Go events continue after the imported sequence, and re-import is
// idempotent.
func TestEventRecorderImportCXXPreservesIndexes(t *testing.T) {
	r := NewEventRecorder()
	external := RecordedPhase{
		Origin: OriginEngine, Scope: ScopeSegment,
		Component: "engine.video", Action: "decode", EventIndex: 7,
		Status: StatusOK, DurationMS: 12,
	}
	if err := r.ImportCXX([]RecordedPhase{external}); err != nil {
		t.Fatalf("ImportCXX: %v", err)
	}
	got := r.Snapshot()
	if len(got) != 1 || got[0].EventIndex != 7 || got[0].SchemaVersion != SchemaVersion || got[0].Phase != PhaseDecode {
		t.Fatalf("imported event = %+v, want preserved index and catalog defaults", got)
	}
	if err := r.ImportCXX([]RecordedPhase{external}); err != nil {
		t.Fatalf("idempotent ImportCXX: %v", err)
	}
	if len(r.Snapshot()) != 1 {
		t.Fatalf("idempotent import duplicated journal event: %+v", r.Snapshot())
	}
	r.Emit(EventSpec{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.video", Action: "decode"}, StatusOK, "", "")
	got = r.Snapshot()
	if len(got) != 2 || got[1].EventIndex != 8 {
		t.Fatalf("post-import engine index = %+v, want 8", got)
	}
}

func TestEventRecorderImportCXXRetainsInvalidForQuarantine(t *testing.T) {
	r := NewEventRecorder()
	event := RecordedPhase{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "invented", EventIndex: 0}
	if err := r.ImportCXX([]RecordedPhase{event}); err == nil {
		t.Fatal("unknown C++ event unexpectedly imported without validation error")
	}
	got := r.Snapshot()
	if len(got) != 1 || got[0].Action != "invented" {
		t.Fatalf("invalid C++ event was not retained for quarantine: %+v", got)
	}
}

// TestWorkerRegistryIsDerivedFromSharedCatalog verifies the single-source
// rule: the worker phase registry is EXACTLY a projection of the shared
// catalog — every shared entry is visible through the worker lookup, every
// PhaseSpec matches its shared source on all five attributes, and no worker
// entry exists outside the shared catalog. This is the guard that prevents
// the dual-registry drift (adding an event to only one side).
func TestWorkerRegistryIsDerivedFromSharedCatalog(t *testing.T) {
	shared := sharedtelemetry.Catalog.Entries()
	if len(shared) == 0 {
		t.Fatal("shared catalog is empty")
	}

	worker := RegisteredPhaseSpecs()
	if len(worker) != len(shared) {
		t.Fatalf("worker registry size %d != shared catalog size %d (parallel registry detected)", len(worker), len(shared))
	}

	for key, sharedSpec := range shared {
		ws, ok := worker[key]
		if !ok {
			t.Fatalf("shared event %s missing from worker projection", key)
		}
		if ws.Origin != sharedSpec.Origin || ws.Scope != sharedSpec.Scope ||
			ws.Component != sharedSpec.Component || ws.Action != sharedSpec.Action ||
			ws.Phase != sharedSpec.Phase || ws.EventType != sharedSpec.EventType ||
			ws.Kind != sharedSpec.Kind || ws.TimingMode != sharedSpec.TimingMode ||
			ws.Aggregation != sharedSpec.Aggregation || ws.Cardinality != sharedSpec.Cardinality ||
			ws.Owner != sharedSpec.Owner {
			t.Fatalf("worker projection of %s differs from shared: worker=%+v shared=%+v", key, ws, sharedSpec)
		}

		looked, ok := LookupPhaseSpec(sharedSpec.Component, sharedSpec.Action)
		if !ok || looked.Key() != key {
			t.Fatalf("LookupPhaseSpec(%s) = %+v, want key %s", key, looked, key)
		}
	}

	// No worker entry outside the shared catalog.
	for key := range worker {
		if _, ok := shared[key]; !ok {
			t.Fatalf("worker event %s has no shared catalog entry (parallel registry detected)", key)
		}
	}

	// Count must match the single source.
	if CanonicalPhaseSpecCount() != len(shared) {
		t.Fatalf("CanonicalPhaseSpecCount=%d != shared count %d", CanonicalPhaseSpecCount(), len(shared))
	}
}

func TestCanonicalEnumsAndRegistry(t *testing.T) {
	for _, o := range CanonicalOrigins() {
		if !IsCanonicalOrigin(o) {
			t.Errorf("expected %q canonical", o)
		}
	}
	for _, s := range CanonicalScopes() {
		if !IsCanonicalScope(s) {
			t.Errorf("expected %q canonical", s)
		}
	}
	if IsCanonicalOrigin("bogus") || IsCanonicalScope("bogus") || IsCanonicalOrigin("") {
		t.Error("non-canonical or empty values must not be canonical")
	}

	spec, ok := LookupPhaseSpec("runner", "cache_lookup")
	if !ok || spec.Origin != OriginWorker || spec.Scope != ScopeAttempt || spec.Phase != PhaseCacheLookup {
		t.Errorf("lookup failed: ok=%v spec=%+v", ok, spec)
	}
	if spec.Key() != "runner.cache_lookup" {
		t.Errorf("expected key runner.cache_lookup, got %q", spec.Key())
	}
	if _, ok := LookupPhaseSpec("runner", "not_registered"); ok {
		t.Error("unknown component/action must not be registered")
	}

	catalog := RegisteredPhaseSpecs()
	if len(catalog) != CanonicalPhaseSpecCount() || len(catalog) < 100 {
		t.Errorf("unexpected catalog size: len=%d count=%d", len(catalog), CanonicalPhaseSpecCount())
	}
	for _, assertion := range []struct {
		component string
		action    string
		origin    string
		scope     string
		phase     string
	}{
		{component: "runner", action: "cache_lookup", origin: OriginWorker, scope: ScopeAttempt, phase: PhaseCacheLookup},
		{component: "runner", action: "execute", origin: OriginWorker, scope: ScopeAttempt, phase: PhaseRender},
		{component: "engine.video", action: "decode", origin: OriginEngine, scope: ScopeSegment, phase: PhaseDecode},
		{component: "engine", action: "simulate", origin: OriginEngine, scope: ScopeSegment, phase: PhaseSimulate},
		{component: "engine.audio", action: "mix", origin: OriginEngine, scope: ScopeAudioTrack, phase: PhaseComposite},
		{component: "quality", action: "ffprobe", origin: OriginValidation, scope: ScopeAttempt, phase: PhaseFinalize},
		{component: "db", action: "result_ingest_tx", origin: OriginMaster, scope: ScopeAttempt, phase: PhaseFinalize},
	} {
		got, ok := LookupPhaseSpec(assertion.component, assertion.action)
		if !ok || got.Origin != assertion.origin || got.Scope != assertion.scope || got.Phase != assertion.phase {
			t.Errorf("catalog[%s.%s] = %+v, want origin=%q scope=%q phase=%q", assertion.component, assertion.action, got, assertion.origin, assertion.scope, assertion.phase)
		}
	}
	delete(catalog, "runner.cache_lookup")
	if _, ok := LookupPhaseSpec("runner", "cache_lookup"); !ok {
		t.Error("mutating catalog copy must not mutate registry")
	}

	seenPhases := make(map[string]bool)
	for _, registered := range catalog {
		if registered.Phase != "" {
			seenPhases[registered.Phase] = true
		}
	}
	for _, phase := range CanonicalPhaseOrder {
		if !seenPhases[phase] {
			t.Errorf("canonical catalog does not cover phase %q", phase)
		}
	}
}
