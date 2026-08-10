// Package telemetry contains the immutable wire contract shared by workers
// and the master. Producers and consumers must validate event taxonomy through
// this catalog instead of maintaining local origin/scope lists.
//
// Single-source rule: the canonical taxonomy is EXACTLY ONE literal list —
// canonicalEventDescriptors. Every consumer (master validation, worker phase
// registry, SQL CHECK projections) derives from this catalog; adding an event
// means editing this one list, never a parallel registry.
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
//	Origin    — closed producer vocabulary (master | worker | engine | ...)
//	Scope     — closed resource vocabulary (job | task | attempt | ...)
//	Phase     — canonical worker-side lifecycle phase (queue | render | ...)
//	           or "" when the event has no phase projection.
//	EventType — default event_type stamped by the worker when the producer
//	           omits it ("" = leave producer default).
type EventDescriptor struct {
	Component string
	Action    string
	Origin    string
	Scope     string
	Phase     string
	EventType string
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
		if _, exists := entries[d.Key()]; exists {
			panic("telemetry: duplicate canonical descriptor " + d.Key())
		}
		entries[d.Key()] = TelemetryEventSpec{
			Origin: d.Origin, Scope: d.Scope, Component: d.Component,
			Action: d.Action, SchemaVersion: SchemaVersion,
			Phase: d.Phase, EventType: d.EventType,
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

// canonicalEventDescriptors is the SINGLE canonical taxonomy. Each entry is
// complete (origin/scope/phase/eventtype resolved at declaration time) so no
// consumer needs a second registry or a derivation switch. Entries are
// sorted by Key() for reviewability.
var canonicalEventDescriptors = []EventDescriptor{
	{Component: "attempt", Action: "failure", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "attempt", Action: "retry", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "audio", Action: "summary", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "control.grpc", Action: "offer_rtt", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "control.grpc", Action: "reconnect", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "queue", EventType: ""},
	{Component: "control.grpc", Action: "result_ack_wait", Origin: OriginMaster, Scope: ScopeTask, Phase: "finalize", EventType: ""},
	{Component: "control.grpc", Action: "result_send", Origin: OriginMaster, Scope: ScopeTask, Phase: "upload", EventType: ""},
	{Component: "control.grpc", Action: "send_queue_wait", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "control", Action: "heartbeat_rtt", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "queue", EventType: ""},
	{Component: "control", Action: "lease_renewal_rtt", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "queue", EventType: ""},
	{Component: "db", Action: "artifact_commit_tx", Origin: OriginMaster, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "db", Action: "claim_tx", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "queue", EventType: ""},
	{Component: "db", Action: "enqueue_tx", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "queue", EventType: ""},
	{Component: "db", Action: "lock_wait", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "queue", EventType: ""},
	{Component: "db", Action: "query", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "db", Action: "result_ingest_tx", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "db", Action: "wal_checkpoint", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "engine.audio", Action: "channel_convert", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "composite", EventType: ""},
	{Component: "engine.audio", Action: "ducking", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "composite", EventType: ""},
	{Component: "engine.audio", Action: "encode", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "encode", EventType: ""},
	{Component: "engine.audio", Action: "limit", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "composite", EventType: ""},
	{Component: "engine.audio", Action: "loudness_scan", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "composite", EventType: ""},
	{Component: "engine.audio", Action: "mix", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "composite", EventType: ""},
	{Component: "engine.audio", Action: "music_decode", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "decode", EventType: ""},
	{Component: "engine.audio", Action: "resample", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "composite", EventType: ""},
	{Component: "engine.audio", Action: "sfx_decode", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "decode", EventType: ""},
	{Component: "engine.audio", Action: "timeline_align", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "composite", EventType: ""},
	{Component: "engine.audio", Action: "voiceover_decode", Origin: OriginEngine, Scope: ScopeAudioTrack, Phase: "decode", EventType: ""},
	{Component: "engine", Action: "color_convert_in", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine", Action: "color_convert_out", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine", Action: "composite", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine", Action: "crop", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine.encode", Action: "flush", Origin: OriginEngine, Scope: ScopeSegment, Phase: "encode", EventType: ""},
	{Component: "engine.encode", Action: "frame_submit", Origin: OriginEngine, Scope: ScopeSegment, Phase: "encode", EventType: ""},
	{Component: "engine.encode", Action: "setup", Origin: OriginEngine, Scope: ScopeSegment, Phase: "encode", EventType: ""},
	{Component: "engine.input", Action: "demux_probe", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.input", Action: "duration_probe", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.input", Action: "keyframe_scan", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.input", Action: "open", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.input", Action: "seek", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.input", Action: "stream_discovery", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine", Action: "mask", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine.mux", Action: "header", Origin: OriginEngine, Scope: ScopeArtifact, Phase: "encode", EventType: ""},
	{Component: "engine.mux", Action: "packet_write", Origin: OriginEngine, Scope: ScopeArtifact, Phase: "encode", EventType: ""},
	{Component: "engine.mux", Action: "trailer", Origin: OriginEngine, Scope: ScopeArtifact, Phase: "encode", EventType: ""},
	{Component: "engine", Action: "opacity", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine.output", Action: "fsync", Origin: OriginEngine, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "engine", Action: "render", Origin: OriginEngine, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "engine", Action: "scale", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine", Action: "simulate", Origin: OriginEngine, Scope: ScopeSegment, Phase: "simulate", EventType: ""},
	{Component: "engine", Action: "transform", Origin: OriginEngine, Scope: ScopeSegment, Phase: "composite", EventType: ""},
	{Component: "engine.video", Action: "decode", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.video", Action: "frame_reorder", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.video", Action: "hw_download", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "engine.video", Action: "timestamp_normalize", Origin: OriginEngine, Scope: ScopeSegment, Phase: "decode", EventType: ""},
	{Component: "ffmpeg", Action: "progress", Origin: OriginFFmpeg, Scope: ScopeSegment, Phase: "encode", EventType: ""},
	{Component: "io", Action: "summary", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "master.commit", Action: "transaction", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "master.commit_ack", Action: "send", Origin: OriginMaster, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "master.enqueue", Action: "transaction", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.http", Action: "auth", Origin: OriginMaster, Scope: ScopeJob, Phase: "queue", EventType: ""},
	{Component: "master.http", Action: "decode", Origin: OriginMaster, Scope: ScopeJob, Phase: "queue", EventType: ""},
	{Component: "master.intake", Action: "validate", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.lease", Action: "issue", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.manifest", Action: "fetch", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.manifest", Action: "hash_verify", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.manifest", Action: "parse", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.offer", Action: "accept_to_start", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.offer", Action: "offer_to_accept", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.offer", Action: "send", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.payload", Action: "normalize", Origin: OriginMaster, Scope: ScopeTask, Phase: "compile", EventType: ""},
	{Component: "master.placement", Action: "candidate_scan", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.placement", Action: "match", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.placement", Action: "rejection", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.placement", Action: "snapshot_load", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.plan", Action: "compile", Origin: OriginMaster, Scope: ScopeTask, Phase: "compile", EventType: ""},
	{Component: "master.queue", Action: "ready_wait", Origin: OriginMaster, Scope: ScopeTask, Phase: "queue", EventType: ""},
	{Component: "master.upload", Action: "verify", Origin: OriginUpload, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "master.upload_plan", Action: "create", Origin: OriginUpload, Scope: ScopeArtifact, Phase: "upload", EventType: ""},
	{Component: "quality", Action: "audio_sync", Origin: OriginValidation, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "black_frame_scan", Origin: OriginValidation, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "duration_check", Origin: OriginValidation, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "ffprobe", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "sha256", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "silence_scan", Origin: OriginValidation, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "stream_check", Origin: OriginValidation, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "subtitle_timeline", Origin: OriginValidation, Scope: ScopeSubtitleTrack, Phase: "finalize", EventType: ""},
	{Component: "quality", Action: "summary", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "retry", Action: "summary", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "runner", Action: "cache_lookup", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "cache_lookup", EventType: ""},
	{Component: "runner", Action: "execute", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "runner", Action: "prefetch", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "asset_wait", EventType: ""},
	{Component: "runner", Action: "report", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "runner", Action: "run", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "finalize", EventType: "failed"},
	{Component: "runner", Action: "upload", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "upload", EventType: ""},
	{Component: "subtitle", Action: "ass_compile", Origin: OriginValidation, Scope: ScopeSubtitleTrack, Phase: "compile", EventType: ""},
	{Component: "subtitle", Action: "audio_extract", Origin: OriginValidation, Scope: ScopeSubtitleTrack, Phase: "compile", EventType: ""},
	{Component: "subtitle", Action: "burn_in", Origin: OriginEngine, Scope: ScopeSubtitleTrack, Phase: "composite", EventType: ""},
	{Component: "subtitle", Action: "font_fallback", Origin: OriginEngine, Scope: ScopeSubtitleTrack, Phase: "composite", EventType: ""},
	{Component: "subtitle", Action: "font_load", Origin: OriginEngine, Scope: ScopeSubtitleTrack, Phase: "composite", EventType: ""},
	{Component: "subtitle", Action: "glyph_raster", Origin: OriginEngine, Scope: ScopeSubtitleTrack, Phase: "composite", EventType: ""},
	{Component: "subtitle", Action: "layout", Origin: OriginEngine, Scope: ScopeSubtitleTrack, Phase: "composite", EventType: ""},
	{Component: "subtitle", Action: "parse", Origin: OriginEngine, Scope: ScopeSubtitleTrack, Phase: "composite", EventType: ""},
	{Component: "subtitle", Action: "segment", Origin: OriginValidation, Scope: ScopeSubtitleTrack, Phase: "compile", EventType: ""},
	{Component: "subtitle", Action: "summary", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "subtitle", Action: "transcribe", Origin: OriginValidation, Scope: ScopeSubtitleTrack, Phase: "compile", EventType: ""},
	{Component: "subtitle", Action: "word_alignment", Origin: OriginValidation, Scope: ScopeSubtitleTrack, Phase: "compile", EventType: ""},
	{Component: "waste", Action: "summary", Origin: OriginValidation, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "worker.asset", Action: "connect", Origin: OriginWorker, Scope: ScopeTask, Phase: "download", EventType: ""},
	{Component: "worker.asset", Action: "disk_write", Origin: OriginWorker, Scope: ScopeArtifact, Phase: "download", EventType: ""},
	{Component: "worker.asset", Action: "dns", Origin: OriginWorker, Scope: ScopeTask, Phase: "download", EventType: ""},
	{Component: "worker.asset", Action: "final_hash", Origin: OriginWorker, Scope: ScopeArtifact, Phase: "download", EventType: ""},
	{Component: "worker.asset", Action: "fsync", Origin: OriginWorker, Scope: ScopeArtifact, Phase: "download", EventType: ""},
	{Component: "worker.asset", Action: "progress_checkpoint", Origin: OriginWorker, Scope: ScopeTask, Phase: "download", EventType: ""},
	{Component: "worker.asset", Action: "resolve", Origin: OriginWorker, Scope: ScopeTask, Phase: "asset_wait", EventType: ""},
	{Component: "worker.asset", Action: "transfer", Origin: OriginWorker, Scope: ScopeTask, Phase: "download", EventType: ""},
	{Component: "worker.asset", Action: "ttfb", Origin: OriginWorker, Scope: ScopeTask, Phase: "download", EventType: ""},
	{Component: "worker.cache", Action: "eviction", Origin: OriginWorker, Scope: ScopeTask, Phase: "cache_lookup", EventType: ""},
	{Component: "worker.cache", Action: "hash_verify", Origin: OriginWorker, Scope: ScopeTask, Phase: "cache_lookup", EventType: ""},
	{Component: "worker.cache", Action: "hit_read", Origin: OriginWorker, Scope: ScopeTask, Phase: "cache_lookup", EventType: ""},
	{Component: "worker.cache", Action: "lookup", Origin: OriginWorker, Scope: ScopeTask, Phase: "cache_lookup", EventType: ""},
	{Component: "worker.cache", Action: "metadata_read", Origin: OriginWorker, Scope: ScopeTask, Phase: "cache_lookup", EventType: ""},
	{Component: "worker.cache", Action: "miss", Origin: OriginWorker, Scope: ScopeTask, Phase: "cache_lookup", EventType: ""},
	{Component: "worker", Action: "commit_ack_wait", Origin: OriginUpload, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "worker.disk", Action: "wait", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.engine", Action: "binary_resolve", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.engine", Action: "first_progress", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.engine", Action: "output_stat", Origin: OriginWorker, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "worker.engine", Action: "sidecar_read", Origin: OriginWorker, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "worker.engine", Action: "spawn", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.engine", Action: "tempdir_create", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.engine", Action: "wait", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.output", Action: "cleanup", Origin: OriginWorker, Scope: ScopeArtifact, Phase: "finalize", EventType: ""},
	{Component: "worker.output", Action: "declare", Origin: OriginUpload, Scope: ScopeArtifact, Phase: "upload", EventType: ""},
	{Component: "worker.output", Action: "hash", Origin: OriginUpload, Scope: ScopeArtifact, Phase: "upload", EventType: ""},
	{Component: "worker.parallel", Action: "queue_wait", Origin: OriginWorker, Scope: ScopeSegment, Phase: "queue", EventType: ""},
	{Component: "worker.parallel", Action: "segment_finish", Origin: OriginWorker, Scope: ScopeSegment, Phase: "finalize", EventType: ""},
	{Component: "worker.parallel", Action: "segment_start", Origin: OriginWorker, Scope: ScopeSegment, Phase: "render", EventType: ""},
	{Component: "worker.plan", Action: "compile", Origin: OriginWorker, Scope: ScopeTask, Phase: "compile", EventType: ""},
	{Component: "worker.plan", Action: "deserialize", Origin: OriginWorker, Scope: ScopeTask, Phase: "compile", EventType: ""},
	{Component: "worker.plan", Action: "resolve_assets", Origin: OriginWorker, Scope: ScopeTask, Phase: "compile", EventType: ""},
	{Component: "worker.plan", Action: "serialize", Origin: OriginWorker, Scope: ScopeTask, Phase: "compile", EventType: ""},
	{Component: "worker.plan", Action: "validate", Origin: OriginWorker, Scope: ScopeTask, Phase: "compile", EventType: ""},
	{Component: "worker.plan", Action: "write", Origin: OriginWorker, Scope: ScopeArtifact, Phase: "compile", EventType: ""},
	{Component: "worker.temp", Action: "create", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.temp", Action: "delete", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "finalize", EventType: ""},
	{Component: "worker.temp", Action: "read", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.temp", Action: "write", Origin: OriginWorker, Scope: ScopeAttempt, Phase: "render", EventType: ""},
	{Component: "worker.upload", Action: "connect", Origin: OriginUpload, Scope: ScopeArtifact, Phase: "upload", EventType: ""},
	{Component: "worker.upload", Action: "transfer", Origin: OriginUpload, Scope: ScopeArtifact, Phase: "upload", EventType: ""},
}
