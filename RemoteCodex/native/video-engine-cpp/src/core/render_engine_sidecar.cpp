#include "velox/core/render_engine.hpp"
#include "velox/services/io_counters.hpp"

#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace velox::core {

namespace {

struct ObservabilityAggregate {
    int64_t events{0};
    double wall_ms{0};
    double cpu_ms{0};
    double queue_wait_ms{0};
    int64_t bytes_in{0};
    int64_t bytes_out{0};
    int64_t frames_in{0};
    int64_t frames_out{0};
    int64_t retry_count{0};
    int64_t wasted_cpu_ms{0};
    int64_t wasted_download_bytes{0};
    int64_t completed_segments{0};
    std::string error_component;
    std::string error_phase;
};

struct ObservabilityRollup {
    ObservabilityAggregate audio;
    ObservabilityAggregate subtitle;
    ObservabilityAggregate io;
    ObservabilityAggregate quality;
    ObservabilityAggregate retry;
    ObservabilityAggregate waste;
};

bool containsToken(const std::string& value, const char* token) {
    return value.find(token) != std::string::npos;
}

void addEventToAggregate(ObservabilityAggregate& aggregate,
                         const telemetry::PhaseEvent& event) {
    aggregate.events++;
    aggregate.wall_ms += static_cast<double>(event.duration_ms);
    aggregate.cpu_ms += event.cpu_ms;
    aggregate.queue_wait_ms += event.queue_wait_ms;
    aggregate.bytes_in += event.bytes_in;
    aggregate.bytes_out += event.bytes_out;
    aggregate.frames_in += event.frames_in;
    aggregate.frames_out += event.frames_out;
}

ObservabilityRollup aggregateObservability(
    const std::vector<telemetry::PhaseEvent>& phases) {
    ObservabilityRollup rollup;
    for (const auto& event : phases) {
        if (event.scope == telemetry::kScopeAudioTrack ||
            containsToken(event.component, "audio") ||
            containsToken(event.action, "audio")) {
            addEventToAggregate(rollup.audio, event);
        } else if (event.scope == telemetry::kScopeSubtitleTrack ||
                   containsToken(event.component, "subtitle") ||
                   containsToken(event.action, "subtitle")) {
            addEventToAggregate(rollup.subtitle, event);
        } else if (containsToken(event.component, "quality") ||
                   containsToken(event.action, "quality")) {
            addEventToAggregate(rollup.quality, event);
        } else if (containsToken(event.component, "asset") ||
                   containsToken(event.component, "cache") ||
                   containsToken(event.component, "upload") ||
                   containsToken(event.action, "disk") ||
                   containsToken(event.action, "transfer") ||
                   containsToken(event.action, "hash")) {
            addEventToAggregate(rollup.io, event);
        }

        if (event.action == "retry" || containsToken(event.event_name, "retry")) {
            addEventToAggregate(rollup.retry, event);
            rollup.retry.retry_count++;
        }
        if (event.status == telemetry::kStatusFailed) {
            rollup.waste.wasted_cpu_ms += static_cast<int64_t>(event.cpu_ms);
            if (containsToken(event.component, "asset") ||
                containsToken(event.action, "download")) {
                rollup.waste.wasted_download_bytes += event.bytes_in;
            }
            rollup.waste.error_component = event.component;
            rollup.waste.error_phase = event.phase;
        }
        if (event.scope == telemetry::kScopeSegment &&
            event.status == telemetry::kStatusOk) {
            rollup.waste.completed_segments++;
        }
    }
    return rollup;
}

void appendAggregateJson(std::ostringstream& out,
                         const ObservabilityAggregate& aggregate) {
    out << "{"
        << "\"events\":" << aggregate.events
        << ",\"wall_ms\":" << aggregate.wall_ms
        << ",\"cpu_ms\":" << aggregate.cpu_ms
        << ",\"queue_wait_ms\":" << aggregate.queue_wait_ms
        << ",\"bytes_in\":" << aggregate.bytes_in
        << ",\"bytes_out\":" << aggregate.bytes_out
        << ",\"frames_in\":" << aggregate.frames_in
        << ",\"frames_out\":" << aggregate.frames_out
        << "}";
}

} // namespace

std::string RenderEngine::sidecarJson(const std::string& output_path) const {
    using services::escapeProgressJsonString;

    fs::path outPath(output_path);

    const services::EngineProgress& last = last_progress_;

    std::ostringstream s;
    s << "{";
    s << "\"progress\":" << static_cast<int>(last.progress_pct);
    s << ",\"progress_pct\":" << static_cast<int>(last.progress_pct);
    s << ",\"frames\":" << frames_encoded_.load();
    s << ",\"frames_decoded\":" << frames_decoded_.load();
    s << ",\"frames_composited\":" << frames_composited_.load();
    s << ",\"fps\":" << last.fps;
    s << ",\"speed\":\"" << escapeProgressJsonString(last.speed) << "\"";
    s << ",\"speed_x\":" << last.speed_x;
    s << ",\"encode_passes\":" << encode_passes_.load();
    s << ",\"concat_mode\":\"" << concat_mode_ << "\"";
    s << ",\"copy_segments\":" << copy_segments_.load();
    s << ",\"transcode_segments\":" << transcode_segments_.load();
    s << ",\"temp_bytes\":" << temp_bytes_written_.load();
    s << ",\"out_time_us\":" << last.out_time_us;
    s << ",\"out_time_ms\":" << last.out_time_ms;
    s << ",\"out_time\":\"" << escapeProgressJsonString(last.out_time) << "\"";
    s << ",\"total_size\":" << last.total_size;
    s << ",\"dup_frames\":" << last.dup_frames;
    s << ",\"drop_frames\":" << last.drop_frames;
    s << ",\"bitrate\":" << last.bitrate;
    s << ",\"duration_seconds\":" << duration_seconds_.load();
    s << ",\"output_durable\":" << (output_durable_.load() ? "true" : "false");
    s << ",\"output_path\":\"" << escapeProgressJsonString(outPath.string()) << "\"";
    s << ",\"artifact_write_progress\":{";
    s << "\"artifact\":\"final_video\"";
    s << ",\"path\":\"" << escapeProgressJsonString(outPath.string()) << "\"";
    s << ",\"high_watermark_bytes\":" << last.total_size;
    s << ",\"safe_offset_bytes\":" << (output_durable_.load() ? last.total_size : 0);
    s << ",\"finalized\":" << (output_durable_.load() ? "true" : "false");
    s << "}";

    {
        const auto& io = services::ioCounters();
        s << ",\"io_counters\":{";
        s << "\"file_copy_count\":" << io.file_copy_count.load();
        s << ",\"file_copy_bytes\":" << io.file_copy_bytes.load();
        s << ",\"asset_bytes_copied\":" << io.asset_bytes_copied.load();
        s << ",\"input_open_count\":" << io.input_open_count.load();
        s << ",\"input_reopen_count\":" << io.input_reopen_count.load();
        s << ",\"input_seek_count\":" << io.input_seek_count.load();
        s << ",\"output_backward_seek_count\":" << io.output_backward_seek_count.load();
        s << ",\"output_backward_seek_bytes\":" << io.output_backward_seek_bytes.load();
        s << ",\"first_packet_read_ms\":" << io.first_packet_read_ms.load();
        s << ",\"first_output_write_ms\":" << io.first_output_write_ms.load();
        s << ",\"file_fsync_ms\":" << io.file_fsync_ms.load();
        s << ",\"directory_fsync_ms\":" << io.directory_fsync_ms.load();
        s << ",\"output_rename_ms\":" << io.output_rename_ms.load();
        s << "}";
    }

    {
        const auto& io = services::ioCounters();
        const services::ProcessUsage usage = services::processUsage();
        s << ",\"process_counters\":{";
        s << "\"external_spawn_count\":" << io.external_spawn_count.load();
        s << ",\"ffmpeg_spawn_count\":" << io.ffmpeg_spawn_count.load();
        s << ",\"ffprobe_spawn_count\":" << io.ffprobe_spawn_count.load();
        s << ",\"shell_spawn_count\":" << io.shell_spawn_count.load();
        s << ",\"curl_spawn_count\":" << io.curl_spawn_count.load();
        s << ",\"cpu_user_ms\":" << usage.cpu_user_ms;
        s << ",\"cpu_system_ms\":" << usage.cpu_system_ms;
        s << ",\"voluntary_context_switches\":" << usage.voluntary_context_switches;
        s << ",\"involuntary_context_switches\":" << usage.involuntary_context_switches;
        s << ",\"minor_page_faults\":" << usage.minor_page_faults;
        s << ",\"major_page_faults\":" << usage.major_page_faults;
        s << "}";
    }

    s << ",\"phase_ms\":{";
    {
        auto pm = metrics_.phaseSnapshot();
        bool first = true;
        for (const auto& [name, ms] : pm) {
            if (!first) s << ",";
            first = false;
            s << "\"" << name << "\":" << ms;
        }
    }
    s << "}";

    if (frame_pipeline_runs_ > 0) {
        const auto& pipeline = frame_pipeline_metrics_;
        s << ",\"frame_pipeline\":{";
        s << "\"producer_busy_ms\":" << pipeline.producer_busy_ms;
        s << ",\"producer_wait_ms\":" << pipeline.producer_wait_ms;
        s << ",\"consumer_busy_ms\":" << pipeline.consumer_busy_ms;
        s << ",\"consumer_wait_ms\":" << pipeline.consumer_wait_ms;
        s << ",\"queue_depth_avg\":" <<
            (pipeline.queue_depth_avg / frame_pipeline_runs_);
        s << ",\"queue_depth_max\":" << pipeline.queue_depth_max;
        s << ",\"queue_empty_ms\":" << pipeline.queue_empty_ms;
        s << ",\"queue_full_ms\":" << pipeline.queue_full_ms;
        s << ",\"producer_stall_ratio\":" << pipeline.producer_stall_ratio;
        s << ",\"encoder_starvation_ratio\":" << pipeline.encoder_starvation_ratio;
        s << ",\"backpressure_ratio\":" << pipeline.backpressure_ratio;
        s << "}";
    }

    s << ",\"segments\":[";
    {
        auto segs = metrics_.segmentsSnapshot();
        for (size_t i = 0; i < segs.size(); ++i) {
            if (i > 0) s << ",";
            const auto& seg = segs[i];
            s << "{";
            s << "\"index\":" << seg.index;
            s << ",\"worker_index\":" << seg.worker_index;
            s << ",\"scene_id\":\"" << escapeProgressJsonString(seg.scene_id) << "\"";
            s << ",\"source_type\":\"" << escapeProgressJsonString(seg.source_type) << "\"";
            s << ",\"total_ms\":" << seg.total_ms;
            s << ",\"asset_download_ms\":" << seg.asset_download_ms;
            s << ",\"ffmpeg_encode_ms\":" << seg.ffmpeg_encode_ms;
            s << ",\"source_bytes\":" << seg.source_bytes;
            s << ",\"output_bytes\":" << seg.output_bytes;
            s << ",\"frames_encoded\":" << seg.frames_encoded;
            s << ",\"frames_decoded\":" << seg.frames_decoded;
            s << ",\"frames_composited\":" << seg.frames_composited;
            s << ",\"ffmpeg_speed_x\":" << seg.ffmpeg_speed_x;
            s << ",\"codec\":\"" << escapeProgressJsonString(seg.codec) << "\"";
            s << ",\"preset\":\"" << escapeProgressJsonString(seg.preset) << "\"";
            s << ",\"ffmpeg_threads\":" << seg.ffmpeg_threads;
            s << ",\"status\":\"" << escapeProgressJsonString(seg.status) << "\"";
            s << ",\"error_code\":\"" << escapeProgressJsonString(seg.error_code) << "\"";
            s << ",\"error_message\":\"" << escapeProgressJsonString(seg.error_message) << "\"";
            s << ",\"started_offset_ms\":" << seg.started_offset_ms;
            s << ",\"finished_offset_ms\":" << seg.finished_offset_ms;
            s << ",\"worker_slot\":" << seg.worker_slot;
            s << ",\"cpu_threads\":" << seg.cpu_threads;
            s << ",\"parallel_group\":\"" << escapeProgressJsonString(seg.parallel_group) << "\"";
            s << "}";
        }
    }
    s << "]";

    s << ",\"phases\":[";
    {
        auto phases = recorder_.Snapshot();
        for (size_t i = 0; i < phases.size(); ++i) {
            if (i > 0) s << ",";
            phases[i].AppendJson(s);
        }
    }
    s << "]";

    const auto rollup = aggregateObservability(recorder_.Snapshot());
    s << ",\"observability\":{";
    s << "\"audio\":";
    appendAggregateJson(s, rollup.audio);
    s << ",\"subtitle\":";
    appendAggregateJson(s, rollup.subtitle);
    s << ",\"io\":";
    appendAggregateJson(s, rollup.io);
    s << ",\"quality\":";
    appendAggregateJson(s, rollup.quality);
    s << ",\"retry\":{\"count\":" << rollup.retry.retry_count << "}";
    s << ",\"waste\":{";
    s << "\"wasted_cpu_ms\":" << rollup.waste.wasted_cpu_ms;
    s << ",\"wasted_download_bytes\":" << rollup.waste.wasted_download_bytes;
    s << ",\"completed_segments\":" << rollup.waste.completed_segments;
    s << ",\"error_component\":\""
      << escapeProgressJsonString(rollup.waste.error_component) << "\"";
    s << ",\"error_phase\":\""
      << escapeProgressJsonString(rollup.waste.error_phase) << "\"";
    s << "}";
    s << "}";

    s << "}";
    return s.str();
}

void RenderEngine::emitSidecar(const std::string& output_path) const {
    fs::path sidecar(output_path);
    sidecar += ".progress.json";
    if (!services::SidecarWriter::writeAtomic(sidecar, sidecarJson(output_path))) {
        std::cerr << "warning: failed to write progress sidecar at " << sidecar << "\n";
    }
}

} // namespace velox::core
