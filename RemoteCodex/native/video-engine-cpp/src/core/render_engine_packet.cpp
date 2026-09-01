#include "velox/core/render_engine.hpp"
#include "render_engine_helpers.hpp"
#include "velox/core/canonical_video_profile.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/media_utils.hpp"
#include "velox/services/segment_execution.hpp"
#ifdef VELOX_ENABLE_LIBAV
#include "velox/services/segment_execution_libav.hpp"
#endif

#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <utility>

namespace fs = std::filesystem;

namespace velox::core {

#ifdef VELOX_ENABLE_LIBAV

namespace {
    using render_detail::fileSize;
    using render_detail::reportArtifactWriteProgress;
    using render_detail::reportDetailedProgress;
    using render_detail::reportProgress;

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

    fs::path numberedWorkPath(const fs::path& workDir, const char* prefix,
                              const char* suffix, std::size_t index) {
        char name[64];
        std::snprintf(name, sizeof(name), "%s%zu%s", prefix, index, suffix);
        return workDir / name;
    }
}

#ifdef VELOX_ENABLE_LIBAV

RenderResult RenderEngine::renderCopyOnly(
    const plan::RenderPlan& plan,
    const std::filesystem::path& workDir,
    const std::filesystem::path& outPath,
    RenderResult& result,
    const std::function<RenderResult(const std::string&)>& failRender,
    const std::chrono::steady_clock::time_point& renderStart) {
    if (plan.timeline.empty()) {
        result.error = "copy_only requires at least one timeline video";
        return failRender("copy_only_empty_timeline");
    }
    if (!plan.subtitle_tracks.empty()) {
        result.error = "copy_only does not support subtitle burn-in";
        return failRender("copy_only_subtitles_unsupported");
    }
    if (plan.watermark_requested && !plan.watermark_already_applied) {
        result.error = "copy_only watermark would require composition; source must already contain the watermark";
        return failRender("copy_only_watermark_composition_required");
    }

    media::CopyOnlyMuxRequest request;
    request.output_path = outPath;
    // Emit progressive-safe byte ranges during the mux so the Go upload
    // can start before the mux finishes.  The partial path is reported
    // because that is where the sink writes; the Go side opens it and
    // continues reading after publishAtomic renames it.
    request.write_progress_callback = [this, outPath](const fs::path& path, int64_t bytes_written) {
        // The mux writes to the .partial path; report that path so the
        // Go progressive upload can open it before publishAtomic renames.
        reportArtifactWriteProgress("final_video", path, bytes_written, bytes_written, false);
    };
    request.video_segments.reserve(plan.timeline.size());
    double total_copy_duration = 0.0;
    int64_t total_copy_duration_us = 0;

    const auto bindOrStage = [&](const std::string& source,
                                 const std::string& cache_reference,
                                 const fs::path& staged_path)
        -> std::pair<fs::path, bool> {
        if (const fs::path local = file::resolveLocalAssetPath(
                source, cache_reference); !local.empty()) {
            return {local, true};
        }
        if (file::downloadAsset(source, staged_path, cache_reference)) {
            return {staged_path, false};
        }
        return {};
    };

    for (size_t i = 0; i < plan.timeline.size(); ++i) {
        const auto& item = plan.timeline[i];
        if (!std::holds_alternative<plan::VideoSource>(item.source)) {
            result.error = "copy_only requires video sources only (segment " +
                std::to_string(i) + ")";
            return failRender("copy_only_source_unsupported");
        }
        const bool hasDuration = item.duration_us > 0 ||
            (item.duration_seconds > 0.0 && std::isfinite(item.duration_seconds));
        if (!hasDuration) {
            result.error = "copy_only requires positive finite duration for segment " +
                std::to_string(i);
            return failRender("copy_only_duration_invalid");
        }
        if (item.transform.explicit_request &&
            (item.transform.slow_zoom || item.transform.scale_mode != "cover")) {
            result.error = "copy_only segment requests a video transform; creator must provide "
                           "canonical media with slow_zoom=false and scale_mode=cover";
            return failRender("copy_only_transform_required");
        }
        const auto& source = std::get<plan::VideoSource>(item.source);
        const auto boundVideo = bindOrStage(
            source.url, source.cache_key,
            numberedWorkPath(workDir, "copy_input_", ".mp4", i));
        if (boundVideo.first.empty()) {
            result.error = "failed to resolve copy-only video source for segment " +
                std::to_string(i);
            return failRender("asset_download_failed");
        }
        const fs::path& localVideo = boundVideo.first;
        SegmentTiming segment;
        segment.index = i;
        segment.worker_index = 0;
        segment.scene_id = item.scene_id;
        segment.source_type = "video";
        const auto segmentStart = std::chrono::steady_clock::now();
        {
            telemetry::ScopedPhase assetPhase(
                recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                "worker.asset", boundVideo.second ? "bind" : "transfer",
                boundVideo.second ? "asset_wait" : "download");
            assetPhase.Complete();
        }
        segment.source_bytes = fileSize(localVideo);
        segment.total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - segmentStart).count();
        metrics_.addMs(boundVideo.second ? "asset_bind_ms" : "asset_download_ms",
                       segment.total_ms);
        segment.finished_offset_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - renderStart).count();
        segment.status = telemetry::kStatusOk;
        metrics_.addSegment(segment);

        const int64_t duration_us = item.source_duration_us > 0
            ? item.source_duration_us
            : item.duration_us > 0
            ? item.duration_us
            : static_cast<int64_t>(
                  std::llround(item.duration_seconds * 1'000'000.0));
        request.video_segments.push_back({
            localVideo, item.source_in_us, duration_us, item.include_audio});
        if (item.duration_us > 0) {
            total_copy_duration_us += item.duration_us;
        } else {
            total_copy_duration += item.duration_seconds;
        }
        const int progress = 10 + static_cast<int>(
            (static_cast<double>(i + 1) / plan.timeline.size()) * 55.0);
        reportProgress(progress, "staging_copy_inputs");
        reportDetailedProgress(
            last_progress_, static_cast<int>(i + 1), static_cast<int>(plan.timeline.size()),
            static_cast<int>(i + 1), static_cast<int>(plan.timeline.size()),
            "staging_copy_inputs", 0, 0, 0,
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - renderStart).count(), true);
    }

    if (total_copy_duration_us > 0) {
        total_copy_duration = static_cast<double>(total_copy_duration_us) / 1'000'000.0;
    }

    if (plan.audio_tracks.size() > 1) {
        result.error = "copy_only supports at most one final audio track";
        return failRender("copy_only_audio_mix_unsupported");
    }
    media::FinalAudioDecision finalAudioDecision;
    if (!plan.audio_tracks.empty()) {
        const auto& track = plan.audio_tracks.front();
        if (track.loop || track.volume != 1.0 || track.start_time_offset < 0.0) {
            result.error = "copy_only final audio requires finite, neutral-volume audio";
            return failRender("copy_only_audio_transform_unsupported");
        }
        const auto boundAudio = bindOrStage(
            track.source_url, "", workDir / "copy_input_audio.m4a");
        if (boundAudio.first.empty()) {
            result.error = "copy_only_audio_invalid " +
                media::describeFinalAudioProbe(boundAudio.first, {});
            return failRender("copy_only_audio_invalid");
        }
        const media::FinalAudioMetadata finalAudioMetadata =
            media::probeFinalAudioMetadata(boundAudio.first);
        if (finalAudioMetadata.codec.empty() || !finalAudioMetadata.metadata_verified) {
            result.error = "copy_only final audio is not FINAL_AUDIO_COPY: " +
                std::string("audio_metadata_unverified copy_only_audio_invalid ") +
                media::describeFinalAudioProbe(boundAudio.first, finalAudioMetadata);
            return failRender("copy_only_audio_invalid");
        }
        finalAudioDecision = media::resolveFinalAudioModePacket(
            finalAudioMetadata, true, total_copy_duration);
        if (finalAudioDecision.mode != media::FinalAudioMode::Copy) {
            result.error = "copy_only final audio is not FINAL_AUDIO_COPY: " +
                finalAudioDecision.reason;
            return failRender("copy_only_audio_not_final_copy");
        }
        {
            telemetry::ScopedPhase assetPhase(
                recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                "worker.asset", boundAudio.second ? "bind" : "transfer",
                boundAudio.second ? "asset_wait" : "download");
            assetPhase.Complete();
        }
        const int64_t declared_audio_duration = track.duration_us > 0
            ? track.duration_us
            : (track.duration_seconds > 0.0
                   ? static_cast<int64_t>(
                         std::llround(track.duration_seconds * 1'000'000.0))
                   : 0);
        request.audio = media::CopyOnlyAudioTrack{
            boundAudio.first,
            track.start_offset_us > 0
                ? track.start_offset_us
                : static_cast<int64_t>(
                      std::llround(track.start_time_offset * 1'000'000.0)),
            declared_audio_duration};
    }

    duration_seconds_.store(total_copy_duration);
    concat_mode_ = "packet_copy";
    reportProgress(75, "packet_mux");
    reportDetailedProgress(
        last_progress_, static_cast<int>(plan.timeline.size()),
        static_cast<int>(plan.timeline.size()), static_cast<int>(plan.timeline.size()),
        static_cast<int>(plan.timeline.size()), "packet_mux", 0, 0, 0,
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - renderStart).count());
    telemetry::ScopedPhase packetPhase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAttempt,
        "engine", "packet_mux", "finalize");
    if (!plan.audio_tracks.empty()) {
        packetPhase.SetMetadataJSON(
            std::string("{\"final_mux_audio_mode\":\"") +
            media::finalAudioModeName(finalAudioDecision.mode) +
            "\",\"final_mux_audio_encode_passes\":" +
            (finalAudioDecision.mode == media::FinalAudioMode::Copy ? "0" : "1") +
            ",\"audio_metadata_verified\":" +
            (finalAudioDecision.metadata.metadata_verified ? "true" : "false") +
            ",\"audio_codec\":\"" + escapeJsonString(finalAudioDecision.metadata.codec) +
            "\",\"audio_sample_rate\":" + std::to_string(finalAudioDecision.metadata.sample_rate) +
            ",\"audio_channels\":" + std::to_string(finalAudioDecision.metadata.channels) +
            ",\"audio_channel_layout\":\"" +
            escapeJsonString(finalAudioDecision.metadata.channel_layout) +
            "\",\"audio_duration_seconds\":" +
            std::to_string(finalAudioDecision.metadata.duration_seconds) +
            ",\"audio_start_time_seconds\":" +
            std::to_string(finalAudioDecision.metadata.start_time_seconds) +
            ",\"audio_format_name\":\"" +
            escapeJsonString(finalAudioDecision.metadata.format_name) +
            "\",\"audio_extradata_verified\":" +
            (finalAudioDecision.metadata.extradata_verified ? "true" : "false") +
            ",\"audio_container_verified\":" +
            (finalAudioDecision.metadata.container_verified ? "true" : "false") +
            ",\"decision_reason\":\"" +
            escapeJsonString(finalAudioDecision.reason) + "\"}");
    }
    media::CopyOnlyMuxResult muxResult;
    bool muxOk;
    {
        ScopedTimer timer(metrics_, "packet_mux_ms");
        muxOk = media::muxCopyOnly(request, &muxResult);
    }
    if (!muxOk) {
        packetPhase.Abort("packet_mux_failed", muxResult.error);
        result.error = "copy-only packet mux failed: " + muxResult.error;
        return failRender("packet_mux_failed");
    }
    output_durable_.store(muxResult.output_durable);
    trailer_to_publish_us_.store(muxResult.trailer_to_publish_us);
    if (!muxResult.output_durable) {
        std::cerr << "warning: output was atomically published but directory durability was not confirmed\n";
    }
    copy_segments_.store(static_cast<int64_t>(plan.timeline.size()));
    transcode_segments_.store(0);
    packetPhase.Complete(
        0,
        static_cast<int64_t>(fileSize(outPath)),
        0,
        telemetry::kStatusOk);
    last_progress_.total_size = static_cast<int64_t>(fileSize(outPath));
    reportArtifactWriteProgress("final_video", outPath, last_progress_.total_size,
                                last_progress_.total_size, true);
    last_progress_.progress_pct = 100.0;
    last_progress_.finished = true;
    reportProgress(100, "completed");
    result.success = true;
    return result;
}

bool RenderEngine::resolveMixedFinalAudio(
    const plan::RenderPlan& plan,
    const std::filesystem::path& workDir,
    double total_duration,
    media::CopyOnlyMuxRequest& request,
    RenderResult& result,
    std::string& error_code) {
    if (plan.audio_tracks.empty()) {
        return true;
    }
    const auto& track = plan.audio_tracks.front();
    if (track.loop || track.volume != 1.0 || track.start_time_offset < 0.0) {
        error_code = "mixed_audio_transform_unsupported";
        result.error = "mixed render final audio requires finite, neutral-volume audio";
        return false;
    }
    const fs::path local_audio = workDir / "mixed_final_audio.m4a";
    if (!file::downloadAsset(track.source_url, local_audio)) {
        error_code = "mixed_audio_download_failed";
        result.error = "failed to resolve mixed audio track";
        return false;
    }
    if (!media::hasAudioStream(local_audio)) {
        error_code = "mixed_audio_invalid";
        result.error = "failed to resolve valid mixed audio track";
        return false;
    }
    const media::FinalAudioDecision audio_decision = media::resolveFinalAudioModePacket(
        media::probeFinalAudioMetadata(local_audio), true, total_duration);
    if (audio_decision.mode != media::FinalAudioMode::Copy) {
        error_code = "mixed_audio_not_final_copy";
        result.error = "mixed render final audio is not FINAL_AUDIO_COPY: " +
            audio_decision.reason;
        return false;
    }
    const int64_t declared_audio_duration = track.duration_us > 0
        ? track.duration_us
        : (track.duration_seconds > 0.0
               ? static_cast<int64_t>(
                     std::llround(track.duration_seconds * 1'000'000.0))
               : 0);
    request.audio = media::CopyOnlyAudioTrack{
        local_audio,
        track.start_offset_us > 0
            ? track.start_offset_us
            : static_cast<int64_t>(
                  std::llround(track.start_time_offset * 1'000'000.0)),
        declared_audio_duration};
    return true;
}

RenderResult RenderEngine::renderMixed(
    const plan::RenderPlan& plan,
    const std::filesystem::path& workDir,
    const std::filesystem::path& outPath,
    RenderResult& result,
    const std::function<RenderResult(const std::string&)>& failRender) {
    if (plan.timeline.empty()) {
        result.error = "mixed render requires at least one timeline video";
        return failRender("mixed_empty_timeline");
    }
    if (!plan.subtitle_tracks.empty()) {
        result.error = "mixed render does not support subtitle burn-in";
        return failRender("mixed_subtitles_unsupported");
    }
    if (plan.audio_tracks.size() > 1) {
        result.error = "mixed render supports at most one final audio track";
        return failRender("mixed_audio_mix_unsupported");
    }

    const media::MediaSignature canonical =
        mediaSignatureFromCanonicalProfile(canonicalVideoProfileV1());
    media::CopyOnlyMuxRequest request;
    request.output_path = outPath;
    request.video_segments.reserve(plan.timeline.size());

    int64_t total_duration_us = 0;
    int64_t packet_copy_segments = 0;
    int64_t rejected_segments = 0;
    std::optional<media::MediaSignature> mixed_canonical_source;
    for (std::size_t i = 0; i < plan.timeline.size(); ++i) {
        const auto& item = plan.timeline[i];
        if (!std::holds_alternative<plan::VideoSource>(item.source)) {
            result.error = "mixed render requires video sources only (segment " +
                std::to_string(i) + ")";
            return failRender("mixed_source_unsupported");
        }
        SegmentTiming segment;
        segment.index = i;
        segment.worker_index = 0;
        segment.scene_id = item.scene_id;
        segment.source_type = "video";
        const auto segmentStart = std::chrono::steady_clock::now();
        const auto& source = std::get<plan::VideoSource>(item.source);
        const int64_t duration_us = item.source_duration_us > 0
            ? item.source_duration_us
            : item.duration_us > 0
            ? item.duration_us
            : static_cast<int64_t>(std::llround(item.duration_seconds * 1'000'000.0));
        if (duration_us <= 0) {
            result.error = "mixed render requires positive duration for segment " +
                std::to_string(i);
            return failRender("mixed_duration_invalid");
        }
        const fs::path local_video = numberedWorkPath(workDir, "mixed_video_", ".mp4", i);
        const auto downloadStart = std::chrono::steady_clock::now();
        telemetry::ScopedPhase assetPhase(
            recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
            "worker.asset", "transfer", "download");
        if (!file::downloadAsset(source.url, local_video, source.cache_key)) {
            assetPhase.Abort("asset_download_failed", "failed to download mixed video source");
            result.error = "failed to download video source for segment " + std::to_string(i);
            return failRender("asset_download_failed");
        }
        assetPhase.Complete();
        segment.asset_download_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - downloadStart).count();
        segment.source_bytes = fileSize(local_video);

        media::SegmentProbe probe;
        std::string probe_error;
        {
            ScopedTimer timer(metrics_, "mixed_probe_ms");
            if (!media::probeSegmentForExecution(local_video, item.source_in_us,
                                                 media::MediaKind::Video, &probe, &probe_error)) {
                result.error = "failed to probe segment " + std::to_string(i) + ": " + probe_error;
                return failRender("mixed_probe_failed");
            }
        }
        media::SegmentExecutionRequest execution_request;
        execution_request.source = probe.signature;
        if (!mixed_canonical_source.has_value()) {
            mixed_canonical_source = canonical;
            mixed_canonical_source->time_base_num = probe.signature.time_base_num;
            mixed_canonical_source->time_base_den = probe.signature.time_base_den;
        }
        execution_request.target = *mixed_canonical_source;
        execution_request.transform_required = item.transform.slow_zoom;
        execution_request.source_window_keyframe_safe = probe.source_window_keyframe_safe;
        execution_request.legacy_required = item.include_audio;
        const media::SegmentExecutionDecision decision =
            media::resolveSegmentExecution(execution_request);
        if (decision.mode != media::SegmentExecutionMode::PacketCopy) {
            ++rejected_segments;
            result.error = "segment_execution_rejected: " + decision.reason +
                " (segment " + std::to_string(i) + ")";
            return failRender("segment_execution_rejected");
        }
        segment.codec = "packet_copy";
        ++packet_copy_segments;
        request.video_segments.push_back({
            local_video, item.source_in_us, duration_us, false, false});
        segment.total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - segmentStart).count();
        segment.status = telemetry::kStatusOk;
        metrics_.addSegment(segment);
        total_duration_us += duration_us;
    }
    copy_segments_.store(packet_copy_segments);
    transcode_segments_.store(0);

    const double total_duration = static_cast<double>(total_duration_us) / 1'000'000.0;
    std::string error_code;
    if (!resolveMixedFinalAudio(plan, workDir, total_duration, request, result, error_code)) {
        return failRender(error_code);
    }
    duration_seconds_.store(total_duration);
    concat_mode_ = "mixed_packet";
    reportProgress(90, "mixed_packet_mux");
    telemetry::ScopedPhase mixedPhase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAttempt,
        "engine", "mixed_packet_mux", "finalize");
    mixedPhase.SetMetadataJSON(
        std::string("{\"packet_copy_segments\":") + std::to_string(packet_copy_segments) +
        ",\"rejected_segments\":" + std::to_string(rejected_segments) +
        ",\"total_segments\":" + std::to_string(plan.timeline.size()) +
        ",\"total_duration_seconds\":" + std::to_string(total_duration) + "}");
    media::CopyOnlyMuxResult muxResult;
    bool muxOk;
    {
        ScopedTimer timer(metrics_, "packet_mux_ms");
        muxOk = media::muxCopyOnly(request, &muxResult);
    }
    if (!muxOk) {
        mixedPhase.Abort("mixed_packet_mux_failed", muxResult.error);
        result.error = "mixed packet mux failed: " + muxResult.error;
        return failRender("mixed_packet_mux_failed");
    }
    output_durable_.store(muxResult.output_durable);
    mixedPhase.Complete(0, static_cast<int64_t>(fileSize(outPath)), 0,
                        telemetry::kStatusOk);
    last_progress_.total_size = static_cast<int64_t>(fileSize(outPath));
    reportArtifactWriteProgress("final_video", outPath, last_progress_.total_size,
                                last_progress_.total_size, true);
    last_progress_.progress_pct = 100.0;
    last_progress_.finished = true;
    reportProgress(100, "completed");
    result.success = true;
    return result;
}

#endif

#endif // VELOX_ENABLE_LIBAV

} // namespace velox::core
