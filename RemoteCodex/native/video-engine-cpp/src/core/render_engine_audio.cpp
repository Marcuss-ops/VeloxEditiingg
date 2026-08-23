#include "velox/core/render_engine.hpp"
#include "velox/audio/audio_benchmark.hpp"
#include "velox/audio/audio_plan.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_utils.hpp"

#include <chrono>
#include <cmath>
#include <filesystem>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace velox::core {

namespace {

std::string escapeJsonString(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
        switch (c) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default: out += c; break;
        }
    }
    return out;
}

std::string optimizedAudioFilter(
    const std::vector<std::pair<fs::path, const plan::AudioTrack*>>& tracks,
    const audio::CompiledAudioPlan& compiled) {
    std::ostringstream filter;
    if (compiled.primary_indices.size() == 1) {
        const auto index = compiled.primary_indices.front();
        const auto* track = tracks[index].second;
        filter << "[" << index << ":a]atrim=duration=" << track->duration_seconds
               << ",asetpts=PTS-STARTPTS,volume=" << track->volume << "[aout]";
        return filter.str();
    }
    for (std::size_t position = 0; position < compiled.primary_indices.size(); ++position) {
        const auto index = compiled.primary_indices[position];
        const auto* track = tracks[index].second;
        if (position > 0) filter << ";";
        filter << "[" << index << ":a]atrim=duration=" << track->duration_seconds
               << ",asetpts=PTS-STARTPTS,volume=" << track->volume
               << "[p" << position << "]";
    }
    filter << ";";
    for (std::size_t position = 0; position < compiled.primary_indices.size(); ++position) {
        filter << "[p" << position << "]";
    }
    filter << "concat=n=" << compiled.primary_indices.size() << ":v=0:a=1[aout]";
    return filter.str();
}

std::string audioPlanMetadata(
    const audio::CompiledAudioPlan& plan,
    audio::AudioMixStrategy requested,
    audio::AudioMixStrategy selected,
    std::size_t input_count,
    std::size_t filter_count,
    std::size_t amix_input_count,
    const audio::AudioMixBenchmarkResult* benchmark) {
    std::string metadata = std::string("{\"audio_mix_strategy_requested\":\"") +
        audio::audioMixStrategyName(requested) +
        "\",\"audio_mix_strategy\":\"" +
        audio::audioMixStrategyName(selected) +
        "\",\"audio_mix_input_count\":" + std::to_string(input_count) +
        ",\"audio_filter_count\":" + std::to_string(filter_count) +
        ",\"audio_amix_input_count\":" + std::to_string(amix_input_count) +
        ",\"audio_sequential_input_count\":" + std::to_string(plan.sequential_count) +
        ",\"audio_overlapping_input_count\":" + std::to_string(plan.overlapping_count) +
        ",\"audio_max_concurrent_inputs\":" + std::to_string(plan.max_concurrent_inputs) +
        ",\"audio_plan_safe_for_optimized\":" +
        (plan.safe_for_optimized_timeline ? "true" : "false") +
        ",\"audio_plan_fallback_reason\":\"" +
        escapeJsonString(plan.fallback_reason) + "\"";

    std::string benchmark_json = ",\"audio_profile_enabled\":";
    benchmark_json += benchmark != nullptr && benchmark->enabled ? "true" : "false";
    if (benchmark != nullptr && benchmark->ran) {
        benchmark_json += ",\"audio_profile_method\":\"" +
            escapeJsonString(benchmark->method) +
            "\",\"audio_inputs_open_ms\":" + std::to_string(benchmark->inputs_open_ms) +
            ",\"audio_decode_ms\":" + std::to_string(benchmark->decode_ms) +
            ",\"audio_filtergraph_ms\":" + std::to_string(benchmark->filtergraph_ms) +
            ",\"audio_encode_ms\":" + std::to_string(benchmark->encode_ms) +
            ",\"audio_output_write_ms\":" + std::to_string(benchmark->output_write_ms) +
            ",\"audio_profile_wall_ms\":" + std::to_string(benchmark->wall_ms) +
            ",\"audio_profile_user_cpu_ms\":" + std::to_string(benchmark->user_cpu_ms) +
            ",\"audio_profile_system_cpu_ms\":" + std::to_string(benchmark->system_cpu_ms) +
            ",\"audio_profile_peak_rss_kb\":" + std::to_string(benchmark->peak_rss_kb) +
            ",\"audio_profile_input_bytes\":" + std::to_string(benchmark->input_bytes) +
            ",\"audio_profile_output_bytes\":" + std::to_string(benchmark->output_bytes);
    } else {
        benchmark_json += ",\"audio_profile_method\":\"" +
            escapeJsonString(benchmark == nullptr ? "not_requested" : benchmark->method) +
            "\",\"audio_inputs_open_ms\":null,\"audio_decode_ms\":null"
            ",\"audio_filtergraph_ms\":null,\"audio_encode_ms\":null"
            ",\"audio_output_write_ms\":null";
        if (benchmark != nullptr && !benchmark->failure_reason.empty()) {
            benchmark_json += ",\"audio_profile_failure_reason\":\"" +
                escapeJsonString(benchmark->failure_reason) + "\"";
        }
    }
    benchmark_json += ",\"audio_mix_encode_passes\":1,\"audio_mix_required\":true}";
    return metadata + benchmark_json;
}

std::string finalAudioMetadataJson(const media::FinalAudioDecision& decision) {
    const auto& metadata = decision.metadata;
    return std::string("{\"final_mux_audio_mode\":\"") +
        media::finalAudioModeName(decision.mode) +
        "\",\"final_mux_audio_encode_passes\":" +
        (decision.mode == media::FinalAudioMode::Copy ? "0" : "1") +
        ",\"audio_metadata_verified\":" +
        (metadata.metadata_verified ? "true" : "false") +
        ",\"audio_codec\":\"" + escapeJsonString(metadata.codec) +
        "\",\"audio_sample_rate\":" + std::to_string(metadata.sample_rate) +
        ",\"audio_channels\":" + std::to_string(metadata.channels) +
        ",\"audio_channel_layout\":\"" +
        escapeJsonString(metadata.channel_layout) +
        "\",\"audio_duration_seconds\":" +
        std::to_string(metadata.duration_seconds) +
        ",\"audio_start_time_seconds\":" +
        std::to_string(metadata.start_time_seconds) +
        "\",\"audio_format_name\":\"" +
        escapeJsonString(metadata.format_name) +
        "\",\"audio_extradata_verified\":" +
        (metadata.extradata_verified ? "true" : "false") +
        ",\"audio_container_verified\":" +
        (metadata.container_verified ? "true" : "false") +
        ",\"decision_reason\":\"" +
        escapeJsonString(decision.reason) + "\"}";
}

bool publishVideoWithoutAudio(
    const std::filesystem::path& video_for_mux,
    const std::function<std::string(const std::filesystem::path&)>& publish_output,
    RenderResult& result,
    const std::string& context,
    const std::function<RenderResult(const std::string&)>& fail_render) {
    const std::string publish_error = publish_output(video_for_mux);
    if (!publish_error.empty()) {
        result.error = "failed to publish final output (" + context + "): " + publish_error;
        fail_render("audio_publish_failed");
        return false;
    }
    result.success = true;
    return true;
}

} // namespace

bool RenderEngine::finalizeAudioTracks(
    const plan::RenderPlan& plan,
    const std::filesystem::path& work_dir,
    const std::filesystem::path& out_path,
    const std::filesystem::path& video_for_mux,
    const std::chrono::steady_clock::time_point& render_start,
    const std::function<std::string(const std::filesystem::path&)>& publish_output,
    const std::function<void(const std::filesystem::path&)>& track_partial,
    RenderResult& result,
    const std::function<RenderResult(const std::string&)>& fail_render) {
    if (plan.audio_tracks.empty()) {
        return publishVideoWithoutAudio(video_for_mux, publish_output, result,
                                        "no audio tracks", fail_render);
    }

    telemetry::ScopedPhase audio_phase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
        "engine.audio", "mix", "composite");
    const auto downloaded_tracks = downloadAudioTracks(plan, work_dir);
    if (downloaded_tracks.empty()) {
        std::cerr << "warning: no audio tracks downloaded, exporting video without audio\n";
        return publishVideoWithoutAudio(video_for_mux, publish_output, result,
                                        "no audio", fail_render);
    }

    if (downloaded_tracks.size() == 1 && !downloaded_tracks[0].second->loop) {
        const fs::path final_muxed = file::makePartialPath(out_path);
        track_partial(final_muxed);
        const double volume = downloaded_tracks[0].second->volume;
        const double offset = downloaded_tracks[0].second->start_time_offset;
        telemetry::ScopedPhase mux_phase(
            recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
            "engine.mux", "audio", "encode");
        file::CommandResult mux_profile;
        media::FinalAudioDecision mux_decision;
        bool mux_ok;
        {
            ScopedTimer timer(metrics_, "mux_audio_ms");
            mux_ok = media::muxAudio(video_for_mux, downloaded_tracks[0].first,
                                     final_muxed, volume, offset, &mux_profile,
                                     true, duration_seconds_.load(), &mux_decision);
        }
        mux_phase.SetMetadataJSON(finalAudioMetadataJson(mux_decision));
        if (!mux_ok) {
            mux_phase.Abort("audio_mux_failed", "failed to mux audio track");
            audio_phase.Abort("audio_mux_failed", "failed to mux audio track");
            result.error = "failed to mux audio track";
            fail_render("audio_mux_failed");
            return false;
        }
        const std::string publish_error = publish_output(final_muxed);
        if (!publish_error.empty()) {
            mux_phase.Abort("audio_publish_failed", publish_error);
            audio_phase.Abort("audio_publish_failed", publish_error);
            result.error = "failed to publish final output: " + publish_error;
            fail_render("audio_publish_failed");
            return false;
        }
        mux_phase.Complete();
        result.success = true;
        return true;
    }

    const auto audio_prepare_start = std::chrono::steady_clock::now();
    std::vector<audio::AudioPlanInput> audio_plan_inputs;
    audio_plan_inputs.reserve(downloaded_tracks.size());
    for (const auto& track : downloaded_tracks) {
        audio_plan_inputs.push_back({
            track.first.string(), track.second->role, track.second->volume,
            track.second->start_time_offset, track.second->duration_seconds,
            track.second->loop});
    }
    const auto compiled_audio_plan = audio::compileAudioPlan(
        audio_plan_inputs, duration_seconds_.load());
    const auto requested_strategy = audio::requestedAudioMixStrategy();
    const auto selected_strategy = audio::resolveAudioMixStrategy(
        requested_strategy, compiled_audio_plan);

    std::ostringstream audio_filter;
    std::ostringstream audio_inputs;
    for (std::size_t index = 0; index < downloaded_tracks.size(); ++index) {
        const auto* track = downloaded_tracks[index].second;
        if (track->loop) audio_inputs << " -stream_loop -1";
        audio_inputs << " -i " << file::shellQuote(downloaded_tracks[index].first.string());
        if (index > 0) audio_filter << ";";
        audio_filter << "[" << index << ":a]";
        const double declared_duration = track->duration_seconds;
        const double track_duration = declared_duration > 0.0
            ? declared_duration
            : (track->loop ? duration_seconds_.load() : 0.0);
        if (track_duration > 0.0) {
            audio_filter << "atrim=duration=" << track_duration
                         << ",asetpts=PTS-STARTPTS,";
        }
        audio_filter << "volume=" << track->volume;
        if (track->start_time_offset > 0.0) {
            const int delay_ms = static_cast<int>(
                std::llround(track->start_time_offset * 1000.0));
            audio_filter << ",adelay=" << delay_ms << "|" << delay_ms;
        }
        audio_filter << "[a" << index << "]";
    }

    const int input_count = static_cast<int>(downloaded_tracks.size());
    std::size_t filter_count = downloaded_tracks.size();
    std::size_t amix_input_count = downloaded_tracks.size();
    if (selected_strategy == audio::AudioMixStrategy::OptimizedTimeline) {
        audio_filter.str(optimizedAudioFilter(downloaded_tracks, compiled_audio_plan));
        audio_filter.clear();
        filter_count += compiled_audio_plan.primary_indices.size() > 1 ? 1 : 0;
        amix_input_count = 0;
    } else {
        audio_filter << ";";
        for (int index = 0; index < input_count; ++index) {
            audio_filter << "[a" << index << "]";
        }
        audio_filter << "amix=inputs=" << input_count << ":duration=longest[aout]";
    }

    const fs::path mixed_audio = work_dir / "mixed_audio.m4a";
    std::ostringstream mix_command;
    mix_command << "ffmpeg -y -hide_banner -loglevel error"
                << audio_inputs.str()
                << " -filter_complex " << file::shellQuote(audio_filter.str())
                << " -map \"[aout]\" -t " << duration_seconds_.load()
                << " -c:a aac " << file::shellQuote(mixed_audio.string());
    metrics_.addMs("audio_prepare_ms", std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - audio_prepare_start).count());

    const auto benchmark = audio::runAudioMixBenchmark(
        audio_plan_inputs, audio_filter.str(), duration_seconds_.load(), work_dir.string());
    telemetry::ScopedPhase encode_phase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
        "engine.audio", "encode", "encode", "", "audio_mix_encode");
    encode_phase.SetMetadataJSON(audioPlanMetadata(
        compiled_audio_plan, requested_strategy, selected_strategy,
        downloaded_tracks.size(), filter_count, amix_input_count, &benchmark));

    bool mix_ok;
    file::CommandResult mix_profile;
    {
        ScopedTimer timer(metrics_, "mix_audio_ms");
        mix_profile = file::runCommandTimed(mix_command.str());
        std::error_code output_error;
        const auto output_bytes = fs::file_size(mixed_audio, output_error);
        uintmax_t input_bytes = 0;
        for (const auto& track : downloaded_tracks) {
            std::error_code input_error;
            const auto bytes = fs::file_size(track.first, input_error);
            if (!input_error) input_bytes += bytes;
        }
        std::cerr << "{\"metric\":\"ffmpeg.audio_mix_encode\",\"value\":"
                  << mix_profile.wall_ms
                  << ",\"ok\":" << (mix_profile.ok ? "true" : "false")
                  << ",\"exit_code\":" << mix_profile.exit_code
                  << ",\"child_user_ms\":" << mix_profile.child_user_ms
                  << ",\"child_system_ms\":" << mix_profile.child_system_ms
                  << ",\"child_max_rss_kb\":" << mix_profile.child_max_rss_kb
                  << ",\"child_input_blocks\":" << mix_profile.child_input_blocks
                  << ",\"child_output_blocks\":" << mix_profile.child_output_blocks
                  << ",\"input_audio_bytes\":" << input_bytes
                  << ",\"output_bytes\":" << (output_error ? 0 : output_bytes)
                  << ",\"command\":\"" << escapeJsonString(mix_command.str()) << "\"}" 
                  << std::endl;
        mix_ok = mix_profile.ok;
    }

    if (!mix_ok) {
        encode_phase.Abort("audio_mix_failed", "failed to mix audio tracks");
        std::cerr << "warning: audio mix failed, exporting video without audio\n";
        return publishVideoWithoutAudio(video_for_mux, publish_output, result,
                                        "mix failed", fail_render);
    }
    encode_phase.Complete();

    const fs::path final_muxed = file::makePartialPath(out_path);
    track_partial(final_muxed);
    telemetry::ScopedPhase mux_phase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
        "engine.mux", "audio", "encode");
    media::FinalAudioDecision mux_decision;
    file::CommandResult mux_profile;
    bool mux_ok;
    {
        ScopedTimer timer(metrics_, "mux_audio_ms");
        mux_ok = media::muxAudio(video_for_mux, mixed_audio, final_muxed,
                                 1.0, 0.0, &mux_profile, true,
                                 duration_seconds_.load(), &mux_decision);
    }
    mux_phase.SetMetadataJSON(finalAudioMetadataJson(mux_decision));
    if (!mux_ok) {
        mux_phase.Abort("audio_mux_failed", "failed to mux mixed audio");
        audio_phase.Abort("audio_mux_failed", "failed to mux mixed audio");
        result.error = "failed to mux mixed audio";
        fail_render("audio_mux_failed");
        return false;
    }
    const std::string publish_error = publish_output(final_muxed);
    if (!publish_error.empty()) {
        mux_phase.Abort("audio_publish_failed", publish_error);
        audio_phase.Abort("audio_publish_failed", publish_error);
        result.error = "failed to publish final output: " + publish_error;
        fail_render("audio_publish_failed");
        return false;
    }
    mux_phase.Complete();
    result.success = true;
    return true;
}

} // namespace velox::core
