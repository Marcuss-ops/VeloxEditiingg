// phase_registry.go — closed canonical observability taxonomy.
//
// This file is the worker-side source of truth for the event taxonomy
// persisted by the master in task_execution_events. The master and C++
// engine mirror the same wire vocabulary, but this Go package owns the
// worker's producer-side validation.
//
// Origins and scopes mirror the SQL CHECK constraints in migration 110.
// Component/action pairs are deliberately closed: producers must use a
// catalog entry rather than inventing a label at runtime. The catalog is
// initialized once and only exposed through defensive copies/lookups.
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

// PhaseSpec is one closed component/action registration. Component may
// contain dotted namespaces (for example "engine.audio"); Action is the
// final stable operation name used in SQL and Prometheus-safe projections.
type PhaseSpec struct {
	Origin    string
	Scope     string
	Component string
	Action    string
	Phase     string
	EventType string
}

// Key returns the stable registry key "component.action".
func (p PhaseSpec) Key() string { return p.Component + "." + p.Action }

// canonicalPhaseSpecs is intentionally a literal, reviewable catalog. Do
// not add runtime registration: adding a new event taxonomy requires a code
// review and corresponding master/C++ contract updates.
var canonicalPhaseSpecs = []PhaseSpec{
	// Existing worker runner boundary events.
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "cache_lookup", Phase: PhaseCacheLookup},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "prefetch", Phase: PhaseAssetWait},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "execute", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "upload", Phase: PhaseUpload},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "report", Phase: PhaseFinalize},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "runner", Action: "run", Phase: PhaseFinalize, EventType: "failed"},

	// Master intake, planning, queue and placement.
	{Origin: OriginMaster, Scope: ScopeJob, Component: "master.http", Action: "auth", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeJob, Component: "master.http", Action: "decode", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.intake", Action: "validate", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.manifest", Action: "fetch", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.manifest", Action: "hash_verify", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.manifest", Action: "parse", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.payload", Action: "normalize", Phase: PhaseCompile},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.plan", Action: "compile", Phase: PhaseCompile},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.enqueue", Action: "transaction", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.queue", Action: "ready_wait", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.placement", Action: "snapshot_load", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.placement", Action: "match", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.placement", Action: "candidate_scan", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.placement", Action: "rejection", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.lease", Action: "issue", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.offer", Action: "send", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.offer", Action: "offer_to_accept", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "master.offer", Action: "accept_to_start", Phase: PhaseQueue},

	// Control-plane and heartbeat timing.
	{Origin: OriginMaster, Scope: ScopeTask, Component: "control.grpc", Action: "send_queue_wait", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "control.grpc", Action: "offer_rtt", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "control.grpc", Action: "result_send", Phase: PhaseUpload},
	{Origin: OriginMaster, Scope: ScopeTask, Component: "control.grpc", Action: "result_ack_wait", Phase: PhaseFinalize},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "control.grpc", Action: "reconnect", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "control", Action: "heartbeat_rtt", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "control", Action: "lease_renewal_rtt", Phase: PhaseQueue},

	// Worker asset resolution, cache and transfer.
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.asset", Action: "resolve", Phase: PhaseAssetWait},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.asset", Action: "dns", Phase: PhaseDownload},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.asset", Action: "connect", Phase: PhaseDownload},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.asset", Action: "ttfb", Phase: PhaseDownload},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.asset", Action: "transfer", Phase: PhaseDownload},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.asset", Action: "disk_write", Phase: PhaseDownload},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.asset", Action: "fsync", Phase: PhaseDownload},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.asset", Action: "final_hash", Phase: PhaseDownload},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.cache", Action: "lookup", Phase: PhaseCacheLookup},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.cache", Action: "metadata_read", Phase: PhaseCacheLookup},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.cache", Action: "hash_verify", Phase: PhaseCacheLookup},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.cache", Action: "hit_read", Phase: PhaseCacheLookup},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.cache", Action: "miss", Phase: PhaseCacheLookup},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.cache", Action: "eviction", Phase: PhaseCacheLookup},

	// Worker plan preparation and engine lifecycle.
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.plan", Action: "deserialize", Phase: PhaseCompile},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.plan", Action: "validate", Phase: PhaseCompile},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.plan", Action: "resolve_assets", Phase: PhaseCompile},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.plan", Action: "compile", Phase: PhaseCompile},
	{Origin: OriginWorker, Scope: ScopeTask, Component: "worker.plan", Action: "serialize", Phase: PhaseCompile},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.plan", Action: "write", Phase: PhaseCompile},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "binary_resolve", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "tempdir_create", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "spawn", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "first_progress", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "wait", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.engine", Action: "sidecar_read", Phase: PhaseFinalize},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.engine", Action: "output_stat", Phase: PhaseFinalize},

	// Native engine input, decode, composition and transforms.
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.input", Action: "open", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.input", Action: "demux_probe", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.input", Action: "stream_discovery", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.input", Action: "duration_probe", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.input", Action: "keyframe_scan", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.input", Action: "seek", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.video", Action: "decode", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.video", Action: "frame_reorder", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.video", Action: "timestamp_normalize", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.video", Action: "hw_download", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "simulate", Phase: PhaseSimulate},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "color_convert_in", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "scale", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "crop", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "composite", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "transform", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "opacity", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "mask", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine", Action: "color_convert_out", Phase: PhaseComposite},

	// Audio, subtitle, encode and mux.
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "voiceover_decode", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "music_decode", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "sfx_decode", Phase: PhaseDecode},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "resample", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "channel_convert", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "timeline_align", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "ducking", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "mix", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "loudness_scan", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "limit", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeAudioTrack, Component: "engine.audio", Action: "encode", Phase: PhaseEncode},
	{Origin: OriginValidation, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "audio_extract", Phase: PhaseCompile},
	{Origin: OriginValidation, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "transcribe", Phase: PhaseCompile},
	{Origin: OriginValidation, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "word_alignment", Phase: PhaseCompile},
	{Origin: OriginValidation, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "segment", Phase: PhaseCompile},
	{Origin: OriginValidation, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "ass_compile", Phase: PhaseCompile},
	{Origin: OriginEngine, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "parse", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "font_load", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "font_fallback", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "glyph_raster", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "layout", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSubtitleTrack, Component: "subtitle", Action: "burn_in", Phase: PhaseComposite},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.encode", Action: "setup", Phase: PhaseEncode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.encode", Action: "frame_submit", Phase: PhaseEncode},
	{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.encode", Action: "flush", Phase: PhaseEncode},
	{Origin: OriginEngine, Scope: ScopeArtifact, Component: "engine.mux", Action: "header", Phase: PhaseEncode},
	{Origin: OriginEngine, Scope: ScopeArtifact, Component: "engine.mux", Action: "packet_write", Phase: PhaseEncode},
	{Origin: OriginEngine, Scope: ScopeArtifact, Component: "engine.mux", Action: "trailer", Phase: PhaseEncode},
	{Origin: OriginEngine, Scope: ScopeArtifact, Component: "engine.output", Action: "fsync", Phase: PhaseFinalize},

	// FFmpeg progress, parallelism, temporary files and I/O.
	{Origin: OriginFFmpeg, Scope: ScopeSegment, Component: "ffmpeg", Action: "progress", Phase: PhaseEncode},
	{Origin: OriginWorker, Scope: ScopeSegment, Component: "worker.parallel", Action: "queue_wait", Phase: PhaseQueue},
	{Origin: OriginWorker, Scope: ScopeSegment, Component: "worker.parallel", Action: "segment_start", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeSegment, Component: "worker.parallel", Action: "segment_finish", Phase: PhaseFinalize},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.temp", Action: "create", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.temp", Action: "write", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.temp", Action: "read", Phase: PhaseRender},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.temp", Action: "delete", Phase: PhaseFinalize},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.disk", Action: "wait", Phase: PhaseRender},

	// Upload, commit, quality, retries and database persistence.
	{Origin: OriginUpload, Scope: ScopeArtifact, Component: "worker.output", Action: "hash", Phase: PhaseUpload},
	{Origin: OriginUpload, Scope: ScopeArtifact, Component: "worker.output", Action: "declare", Phase: PhaseUpload},
	{Origin: OriginUpload, Scope: ScopeArtifact, Component: "master.upload_plan", Action: "create", Phase: PhaseUpload},
	{Origin: OriginUpload, Scope: ScopeArtifact, Component: "worker.upload", Action: "connect", Phase: PhaseUpload},
	{Origin: OriginUpload, Scope: ScopeArtifact, Component: "worker.upload", Action: "transfer", Phase: PhaseUpload},
	{Origin: OriginUpload, Scope: ScopeArtifact, Component: "master.upload", Action: "verify", Phase: PhaseFinalize},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "master.commit", Action: "transaction", Phase: PhaseFinalize},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "master.commit_ack", Action: "send", Phase: PhaseFinalize},
	{Origin: OriginUpload, Scope: ScopeAttempt, Component: "worker", Action: "commit_ack_wait", Phase: PhaseFinalize},
	{Origin: OriginWorker, Scope: ScopeArtifact, Component: "worker.output", Action: "cleanup", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeArtifact, Component: "quality", Action: "ffprobe", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeArtifact, Component: "quality", Action: "duration_check", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeArtifact, Component: "quality", Action: "stream_check", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeArtifact, Component: "quality", Action: "black_frame_scan", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeArtifact, Component: "quality", Action: "silence_scan", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeArtifact, Component: "quality", Action: "audio_sync", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeSubtitleTrack, Component: "quality", Action: "subtitle_timeline", Phase: PhaseFinalize},
	{Origin: OriginValidation, Scope: ScopeArtifact, Component: "quality", Action: "sha256", Phase: PhaseFinalize},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "attempt", Action: "retry", Phase: PhaseFinalize},
	{Origin: OriginWorker, Scope: ScopeAttempt, Component: "attempt", Action: "failure", Phase: PhaseFinalize},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "db", Action: "enqueue_tx", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "db", Action: "claim_tx", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "db", Action: "result_ingest_tx", Phase: PhaseFinalize},
	{Origin: OriginMaster, Scope: ScopeArtifact, Component: "db", Action: "artifact_commit_tx", Phase: PhaseFinalize},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "db", Action: "lock_wait", Phase: PhaseQueue},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "db", Action: "wal_checkpoint", Phase: PhaseFinalize},
	{Origin: OriginMaster, Scope: ScopeAttempt, Component: "db", Action: "query", Phase: PhaseFinalize},
}

var canonicalPhaseSpecByKey = func() map[string]PhaseSpec {
	catalog := make(map[string]PhaseSpec, len(canonicalPhaseSpecs))
	for _, spec := range canonicalPhaseSpecs {
		if !IsCanonicalOrigin(spec.Origin) {
			panic("telemetry: invalid canonical origin for " + spec.Key())
		}
		if !IsCanonicalScope(spec.Scope) {
			panic("telemetry: invalid canonical scope for " + spec.Key())
		}
		if spec.Component == "" || spec.Action == "" {
			panic("telemetry: empty canonical component/action")
		}
		if spec.Phase != "" && !IsCanonical(spec.Phase) {
			panic("telemetry: invalid canonical phase for " + spec.Key())
		}
		if _, exists := catalog[spec.Key()]; exists {
			panic("telemetry: duplicate canonical component/action " + spec.Key())
		}
		catalog[spec.Key()] = spec
	}
	return catalog
}()

// LookupPhaseSpec returns the immutable canonical specification for a
// component/action pair.
func LookupPhaseSpec(component, action string) (PhaseSpec, bool) {
	sharedSpec, ok := sharedtelemetry.Catalog.Lookup(component, action)
	if !ok {
		return PhaseSpec{}, false
	}
	spec, ok := canonicalPhaseSpecByKey[component+"."+action]
	if !ok || spec.Origin != sharedSpec.Origin || spec.Scope != sharedSpec.Scope {
		return PhaseSpec{}, false
	}
	return spec, true
}

// LookupCanonicalPhaseSpec is the explicit name for new callers.
func LookupCanonicalPhaseSpec(component, action string) (PhaseSpec, bool) {
	return LookupPhaseSpec(component, action)
}

// RegisteredPhaseSpecs returns a defensive copy of the closed catalog.
func RegisteredPhaseSpecs() map[string]PhaseSpec {
	out := make(map[string]PhaseSpec, len(canonicalPhaseSpecByKey))
	for key, spec := range canonicalPhaseSpecByKey {
		out[key] = spec
	}
	return out
}

// CanonicalPhaseSpecCount returns the number of registered component/action
// pairs without exposing mutable registry state.
func CanonicalPhaseSpecCount() int { return len(canonicalPhaseSpecByKey) }

// CanonicalizeEventSpec validates and stamps the authoritative origin, scope,
// phase and default event type for a producer event. Unknown component/action
// pairs return false and must not be emitted.
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
	if spec.Origin != canonical.Origin || spec.Scope != canonical.Scope {
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
