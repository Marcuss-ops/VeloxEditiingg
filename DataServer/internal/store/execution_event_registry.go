package store

import "strings"

// executionEventRegistration is the canonical origin/scope tuple for a
// registered component/action. It mirrors the worker PhaseSpec contract.
type executionEventRegistration struct {
	Origin string
	Scope  string
}

// executionEventRegistry is the master-side mirror of the worker telemetry
// catalog. The worker and engine emit only these stable component/action keys;
// keeping the validation closed at ingest prevents arbitrary labels from
// entering the SQL timeline. Origin and scope remain separately closed by
// the SQL schema and are validated in execution_event_persistence.go.
var executionEventKeys = map[string]struct{}{
	"attempt.failure": {}, "attempt.retry": {},
	"control.grpc.offer_rtt": {}, "control.grpc.reconnect": {},
	"control.grpc.result_ack_wait": {}, "control.grpc.result_send": {},
	"control.grpc.send_queue_wait": {}, "control.heartbeat_rtt": {},
	"control.lease_renewal_rtt": {},
	"db.artifact_commit_tx":     {}, "db.claim_tx": {}, "db.enqueue_tx": {},
	"db.lock_wait": {}, "db.query": {}, "db.result_ingest_tx": {},
	"db.wal_checkpoint":            {},
	"engine.audio.channel_convert": {}, "engine.audio.ducking": {},
	"engine.audio.encode": {}, "engine.audio.limit": {},
	"engine.audio.loudness_scan": {}, "engine.audio.mix": {},
	"engine.audio.music_decode": {}, "engine.audio.resample": {},
	"engine.audio.sfx_decode": {}, "engine.audio.timeline_align": {},
	"engine.audio.voiceover_decode": {},
	"engine.color_convert_in":       {}, "engine.color_convert_out": {},
	"engine.composite": {}, "engine.crop": {},
	"engine.encode.flush": {}, "engine.encode.frame_submit": {},
	"engine.encode.setup":      {},
	"engine.input.demux_probe": {}, "engine.input.duration_probe": {},
	"engine.input.keyframe_scan": {}, "engine.input.open": {},
	"engine.input.seek": {}, "engine.input.stream_discovery": {},
	"engine.mask": {}, "engine.mux.header": {},
	"engine.mux.packet_write": {}, "engine.mux.trailer": {},
	"engine.opacity": {}, "engine.output.fsync": {}, "engine.scale": {},
	"engine.simulate": {}, "engine.transform": {},
	"engine.video.decode": {}, "engine.video.frame_reorder": {},
	"engine.video.hw_download": {}, "engine.video.timestamp_normalize": {},
	"ffmpeg.progress":        {},
	"master.commit_ack.send": {}, "master.commit.transaction": {},
	"master.enqueue.transaction": {}, "master.http.auth": {},
	"master.http.decode": {}, "master.intake.validate": {},
	"master.lease.issue": {}, "master.manifest.fetch": {},
	"master.manifest.hash_verify": {}, "master.manifest.parse": {},
	"master.offer.accept_to_start": {}, "master.offer.offer_to_accept": {},
	"master.offer.send": {}, "master.payload.normalize": {},
	"master.placement.candidate_scan": {}, "master.placement.match": {},
	"master.placement.rejection": {}, "master.placement.snapshot_load": {},
	"master.plan.compile": {}, "master.queue.ready_wait": {},
	"master.upload_plan.create": {}, "master.upload.verify": {},
	"quality.audio_sync": {}, "quality.black_frame_scan": {},
	"quality.duration_check": {}, "quality.ffprobe": {},
	"quality.sha256": {}, "quality.silence_scan": {},
	"quality.stream_check": {}, "quality.subtitle_timeline": {},
	"runner.cache_lookup": {}, "runner.execute": {}, "runner.prefetch": {},
	"runner.report": {}, "runner.run": {}, "runner.upload": {},
	"subtitle.ass_compile": {}, "subtitle.audio_extract": {},
	"subtitle.burn_in": {}, "subtitle.font_fallback": {},
	"subtitle.font_load": {}, "subtitle.glyph_raster": {},
	"subtitle.layout": {}, "subtitle.parse": {}, "subtitle.segment": {},
	"subtitle.transcribe": {}, "subtitle.word_alignment": {},
	"worker.asset.connect": {}, "worker.asset.disk_write": {},
	"worker.asset.dns": {}, "worker.asset.final_hash": {},
	"worker.asset.fsync": {}, "worker.asset.resolve": {},
	"worker.asset.transfer": {}, "worker.asset.ttfb": {},
	"worker.cache.eviction": {}, "worker.cache.hash_verify": {},
	"worker.cache.hit_read": {}, "worker.cache.lookup": {},
	"worker.cache.metadata_read": {}, "worker.cache.miss": {},
	"worker.commit_ack_wait": {}, "worker.disk.wait": {},
	"worker.engine.binary_resolve": {}, "worker.engine.first_progress": {},
	"worker.engine.output_stat": {}, "worker.engine.sidecar_read": {},
	"worker.engine.spawn": {}, "worker.engine.tempdir_create": {},
	"worker.engine.wait": {}, "worker.output.cleanup": {},
	"worker.output.declare": {}, "worker.output.hash": {},
	"worker.parallel.queue_wait": {}, "worker.parallel.segment_finish": {},
	"worker.parallel.segment_start": {}, "worker.plan.compile": {},
	"worker.plan.deserialize": {}, "worker.plan.resolve_assets": {},
	"worker.plan.serialize": {}, "worker.plan.validate": {},
	"worker.plan.write": {}, "worker.temp.create": {},
	"worker.temp.delete": {}, "worker.temp.read": {},
	"worker.temp.write": {}, "worker.upload.connect": {},
	"worker.upload.transfer": {},
}

func isRegisteredExecutionEvent(component, action string) bool {
	_, ok := executionEventKeys[component+"."+action]
	return ok
}

// canonicalExecutionEventRegistration returns the closed origin/scope tuple
// for a registered key. The explicit exceptions capture the worker catalog's
// boundary changes; all other namespaces use their stable producer defaults.
func canonicalExecutionEventRegistration(component, action string) (executionEventRegistration, bool) {
	key := component + "." + action
	if _, ok := executionEventKeys[key]; !ok {
		return executionEventRegistration{}, false
	}
	switch key {
	case "attempt.failure", "attempt.retry":
		return executionEventRegistration{"worker", "attempt"}, true
	case "control.grpc.reconnect", "control.heartbeat_rtt", "control.lease_renewal_rtt":
		return executionEventRegistration{"master", "attempt"}, true
	case "master.http.auth", "master.http.decode":
		return executionEventRegistration{"master", "job"}, true
	case "master.commit.transaction", "master.commit_ack.send":
		return executionEventRegistration{"master", "attempt"}, true
	case "master.enqueue.transaction":
		return executionEventRegistration{"master", "task"}, true
	case "master.upload.verify", "master.upload_plan.create":
		return executionEventRegistration{"upload", "artifact"}, true
	case "quality.subtitle_timeline":
		return executionEventRegistration{"validation", "subtitle_track"}, true
	case "subtitle.ass_compile", "subtitle.audio_extract", "subtitle.segment", "subtitle.transcribe", "subtitle.word_alignment":
		return executionEventRegistration{"validation", "subtitle_track"}, true
	case "subtitle.burn_in", "subtitle.font_fallback", "subtitle.font_load", "subtitle.glyph_raster", "subtitle.layout", "subtitle.parse":
		return executionEventRegistration{"engine", "subtitle_track"}, true
	case "worker.commit_ack_wait":
		return executionEventRegistration{"upload", "attempt"}, true
	case "worker.output.declare", "worker.output.hash", "worker.upload.connect", "worker.upload.transfer":
		return executionEventRegistration{"upload", "artifact"}, true
	}

	switch {
	case strings.HasPrefix(component, "control.grpc"):
		return executionEventRegistration{"master", "task"}, true
	case strings.HasPrefix(component, "db"):
		if action == "artifact_commit_tx" {
			return executionEventRegistration{"master", "artifact"}, true
		}
		return executionEventRegistration{"master", "attempt"}, true
	case strings.HasPrefix(component, "engine.audio"):
		return executionEventRegistration{"engine", "audio_track"}, true
	case strings.HasPrefix(component, "engine.mux"), strings.HasPrefix(component, "engine.output"):
		return executionEventRegistration{"engine", "artifact"}, true
	case strings.HasPrefix(component, "engine"):
		return executionEventRegistration{"engine", "segment"}, true
	case component == "ffmpeg":
		return executionEventRegistration{"ffmpeg", "segment"}, true
	case strings.HasPrefix(component, "master.http"):
		return executionEventRegistration{"master", "job"}, true
	case strings.HasPrefix(component, "master"):
		return executionEventRegistration{"master", "task"}, true
	case strings.HasPrefix(component, "quality"):
		return executionEventRegistration{"validation", "artifact"}, true
	case strings.HasPrefix(component, "runner"):
		return executionEventRegistration{"worker", "attempt"}, true
	case strings.HasPrefix(component, "worker.asset"):
		if action == "disk_write" || action == "fsync" || action == "final_hash" {
			return executionEventRegistration{"worker", "artifact"}, true
		}
		return executionEventRegistration{"worker", "task"}, true
	case strings.HasPrefix(component, "worker.cache"):
		if action == "lookup" {
			return executionEventRegistration{"worker", "task"}, true
		}
		return executionEventRegistration{"worker", "artifact"}, true
	case strings.HasPrefix(component, "worker.engine"):
		if action == "sidecar_read" || action == "output_stat" {
			return executionEventRegistration{"worker", "artifact"}, true
		}
		return executionEventRegistration{"worker", "attempt"}, true
	case strings.HasPrefix(component, "worker.output"):
		return executionEventRegistration{"worker", "artifact"}, true
	case strings.HasPrefix(component, "worker.parallel"):
		return executionEventRegistration{"worker", "segment"}, true
	case strings.HasPrefix(component, "worker.plan"):
		if action == "write" {
			return executionEventRegistration{"worker", "artifact"}, true
		}
		return executionEventRegistration{"worker", "task"}, true
	case strings.HasPrefix(component, "worker.temp"), component == "worker.disk":
		return executionEventRegistration{"worker", "artifact"}, true
	case strings.HasPrefix(component, "worker.upload"):
		return executionEventRegistration{"upload", "artifact"}, true
	case strings.HasPrefix(component, "subtitle"):
		return executionEventRegistration{"engine", "subtitle_track"}, true
	}
	return executionEventRegistration{}, false
}
