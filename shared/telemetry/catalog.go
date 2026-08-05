// Package telemetry contains the immutable wire contract shared by workers
// and the master. Producers and consumers must validate event taxonomy through
// this catalog instead of maintaining local origin/scope lists.
package telemetry

import (
	"fmt"
	"strings"
)

// SchemaVersion is the version of the event taxonomy carried by TaskResult.
const SchemaVersion int32 = 1

// TelemetryEventSpec is the immutable catalog entry for one component/action
// pair. Origin and Scope are deliberately strings on the wire, but their
// values are closed by this catalog.
type TelemetryEventSpec struct {
	Origin        string
	Scope         string
	Component     string
	Action        string
	SchemaVersion int32
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
	entries := make(map[string]TelemetryEventSpec, len(canonicalEventKeys))
	for _, key := range canonicalEventKeys {
		component, action, ok := strings.Cut(key, ".")
		if !ok {
			panic("telemetry: invalid catalog key " + key)
		}
		// Components may contain dots. Split at the final separator.
		last := strings.LastIndexByte(key, '.')
		component, action = key[:last], key[last+1:]
		origin, scope := canonicalOriginScope(component, action)
		if origin == "" || scope == "" {
			panic("telemetry: missing origin/scope for " + key)
		}
		entries[key] = TelemetryEventSpec{
			Origin: origin, Scope: scope, Component: component,
			Action: action, SchemaVersion: SchemaVersion,
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

var canonicalEventKeys = []string{
	"attempt.failure", "attempt.retry",
	"control.grpc.offer_rtt", "control.grpc.reconnect", "control.grpc.result_ack_wait", "control.grpc.result_send", "control.grpc.send_queue_wait", "control.heartbeat_rtt", "control.lease_renewal_rtt",
	"db.artifact_commit_tx", "db.claim_tx", "db.enqueue_tx", "db.lock_wait", "db.query", "db.result_ingest_tx", "db.wal_checkpoint",
	"engine.audio.channel_convert", "engine.audio.ducking", "engine.audio.encode", "engine.audio.limit", "engine.audio.loudness_scan", "engine.audio.mix", "engine.audio.music_decode", "engine.audio.resample", "engine.audio.sfx_decode", "engine.audio.timeline_align", "engine.audio.voiceover_decode",
	"engine.color_convert_in", "engine.color_convert_out", "engine.composite", "engine.crop", "engine.encode.flush", "engine.encode.frame_submit", "engine.encode.setup", "engine.input.demux_probe", "engine.input.duration_probe", "engine.input.keyframe_scan", "engine.input.open", "engine.input.seek", "engine.input.stream_discovery", "engine.mask", "engine.mux.header", "engine.mux.packet_write", "engine.mux.trailer", "engine.opacity", "engine.output.fsync", "engine.scale", "engine.simulate", "engine.transform", "engine.video.decode", "engine.video.frame_reorder", "engine.video.hw_download", "engine.video.timestamp_normalize",
	"ffmpeg.progress",
	"master.commit_ack.send", "master.commit.transaction", "master.enqueue.transaction", "master.http.auth", "master.http.decode", "master.intake.validate", "master.lease.issue", "master.manifest.fetch", "master.manifest.hash_verify", "master.manifest.parse", "master.offer.accept_to_start", "master.offer.offer_to_accept", "master.offer.send", "master.payload.normalize", "master.placement.candidate_scan", "master.placement.match", "master.placement.rejection", "master.placement.snapshot_load", "master.plan.compile", "master.queue.ready_wait", "master.upload_plan.create", "master.upload.verify",
	"quality.audio_sync", "quality.black_frame_scan", "quality.duration_check", "quality.ffprobe", "quality.sha256", "quality.silence_scan", "quality.stream_check", "quality.subtitle_timeline",
	"runner.cache_lookup", "runner.execute", "runner.prefetch", "runner.report", "runner.run", "runner.upload",
	"subtitle.ass_compile", "subtitle.audio_extract", "subtitle.burn_in", "subtitle.font_fallback", "subtitle.font_load", "subtitle.glyph_raster", "subtitle.layout", "subtitle.parse", "subtitle.segment", "subtitle.transcribe", "subtitle.word_alignment",
	"worker.asset.connect", "worker.asset.disk_write", "worker.asset.dns", "worker.asset.final_hash", "worker.asset.fsync", "worker.asset.resolve", "worker.asset.transfer", "worker.asset.ttfb",
	"worker.cache.eviction", "worker.cache.hash_verify", "worker.cache.hit_read", "worker.cache.lookup", "worker.cache.metadata_read", "worker.cache.miss",
	"worker.commit_ack_wait", "worker.disk.wait", "worker.engine.binary_resolve", "worker.engine.first_progress", "worker.engine.output_stat", "worker.engine.sidecar_read", "worker.engine.spawn", "worker.engine.tempdir_create", "worker.engine.wait", "worker.output.cleanup", "worker.output.declare", "worker.output.hash", "worker.parallel.queue_wait", "worker.parallel.segment_finish", "worker.parallel.segment_start", "worker.plan.compile", "worker.plan.deserialize", "worker.plan.resolve_assets", "worker.plan.serialize", "worker.plan.validate", "worker.plan.write", "worker.temp.create", "worker.temp.delete", "worker.temp.read", "worker.temp.write", "worker.upload.connect", "worker.upload.transfer",
}

func canonicalOriginScope(component, action string) (string, string) {
	key := component + "." + action
	switch key {
	case "attempt.failure", "attempt.retry":
		return OriginWorker, ScopeAttempt
	case "control.grpc.reconnect", "control.heartbeat_rtt", "control.lease_renewal_rtt", "master.commit.transaction", "master.commit_ack.send":
		return OriginMaster, ScopeAttempt
	case "master.http.auth", "master.http.decode":
		return OriginMaster, ScopeJob
	case "master.enqueue.transaction":
		return OriginMaster, ScopeTask
	case "master.upload.verify", "master.upload_plan.create", "worker.output.declare", "worker.output.hash", "worker.upload.connect", "worker.upload.transfer":
		return OriginUpload, ScopeArtifact
	case "quality.subtitle_timeline", "subtitle.ass_compile", "subtitle.audio_extract", "subtitle.segment", "subtitle.transcribe", "subtitle.word_alignment":
		return OriginValidation, ScopeSubtitleTrack
	case "subtitle.burn_in", "subtitle.font_fallback", "subtitle.font_load", "subtitle.glyph_raster", "subtitle.layout", "subtitle.parse":
		return OriginEngine, ScopeSubtitleTrack
	case "worker.commit_ack_wait":
		return OriginUpload, ScopeAttempt
	}

	switch {
	case strings.HasPrefix(component, "control.grpc"):
		return OriginMaster, ScopeTask
	case strings.HasPrefix(component, "db"):
		if action == "artifact_commit_tx" {
			return OriginMaster, ScopeArtifact
		}
		return OriginMaster, ScopeAttempt
	case strings.HasPrefix(component, "engine.audio"):
		return OriginEngine, ScopeAudioTrack
	case strings.HasPrefix(component, "engine.mux"), strings.HasPrefix(component, "engine.output"):
		return OriginEngine, ScopeArtifact
	case strings.HasPrefix(component, "engine"):
		return OriginEngine, ScopeSegment
	case component == "ffmpeg":
		return OriginFFmpeg, ScopeSegment
	case strings.HasPrefix(component, "master"):
		return OriginMaster, ScopeTask
	case strings.HasPrefix(component, "quality"):
		if action == "sha256" || action == "ffprobe" {
			// Emitted at render-validation time, BEFORE the master assigns
			// an artifact_id to the output. Artifact scope would require an
			// artifact_id that is unavailable here (same reasoning as the
			// worker.cache comment above), so these two events are
			// attempt-scoped; the other quality.* checks run once the
			// artifact identity exists and stay artifact-scoped.
			return OriginValidation, ScopeAttempt
		}
		return OriginValidation, ScopeArtifact
	case strings.HasPrefix(component, "runner"):
		return OriginWorker, ScopeAttempt
	case strings.HasPrefix(component, "worker.asset"):
		if action == "disk_write" || action == "fsync" || action == "final_hash" {
			return OriginWorker, ScopeArtifact
		}
		return OriginWorker, ScopeTask
	case strings.HasPrefix(component, "worker.cache"):
		// Cache access describes task-scoped resolver work. It does not
		// identify a committed output artifact, so artifact scope would
		// require an artifact_id that is unavailable during asset lookup.
		return OriginWorker, ScopeTask
	case strings.HasPrefix(component, "worker.engine"):
		if action == "sidecar_read" || action == "output_stat" {
			return OriginWorker, ScopeArtifact
		}
		return OriginWorker, ScopeAttempt
	case strings.HasPrefix(component, "worker.output"):
		return OriginWorker, ScopeArtifact
	case strings.HasPrefix(component, "worker.parallel"):
		return OriginWorker, ScopeSegment
	case strings.HasPrefix(component, "worker.plan"):
		if action == "write" {
			return OriginWorker, ScopeArtifact
		}
		return OriginWorker, ScopeTask
	case strings.HasPrefix(component, "worker.temp"), component == "worker.disk":
		return OriginWorker, ScopeArtifact
	case strings.HasPrefix(component, "worker.upload"):
		return OriginUpload, ScopeArtifact
	case strings.HasPrefix(component, "subtitle"):
		return OriginEngine, ScopeSubtitleTrack
	}
	return "", ""
}
