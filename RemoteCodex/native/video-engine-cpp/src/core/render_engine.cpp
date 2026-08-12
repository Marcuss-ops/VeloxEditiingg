#include "velox/core/render_engine.hpp"
#include "velox/audio/audio_benchmark.hpp"
#include "velox/audio/audio_plan.hpp"
#include "render_engine_helpers.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/media_utils.hpp"
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace velox::core {

namespace {
    using render_detail::burnSubtitleTrack;
    using render_detail::composeSegmentCmd;
    using render_detail::decodedFramesFromShowInfo;
    using render_detail::extractColorHex;
    using render_detail::fileSize;
    using render_detail::makeParams;
    using render_detail::reportProgress;
    using render_detail::reportDetailedProgress;
    using render_detail::runFfmpegSegmentWithProgress;

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

    std::string audioPlanMetadata(const audio::CompiledAudioPlan& plan,
                                  audio::AudioMixStrategy requested,
                                  audio::AudioMixStrategy selected,
                                  std::size_t inputCount,
                                  std::size_t filterCount,
                                  std::size_t amixInputCount,
                                  const audio::AudioMixBenchmarkResult* benchmark) {
        std::string reason = escapeJsonString(plan.fallback_reason);
        std::string metadata = std::string("{\"audio_mix_strategy_requested\":\"") +
               audio::audioMixStrategyName(requested) +
               "\",\"audio_mix_strategy\":\"" +
               audio::audioMixStrategyName(selected) +
               "\",\"audio_mix_input_count\":" + std::to_string(inputCount) +
               ",\"audio_filter_count\":" + std::to_string(filterCount) +
               ",\"audio_amix_input_count\":" + std::to_string(amixInputCount) +
               ",\"audio_sequential_input_count\":" + std::to_string(plan.sequential_count) +
               ",\"audio_overlapping_input_count\":" + std::to_string(plan.overlapping_count) +
               ",\"audio_max_concurrent_inputs\":" + std::to_string(plan.max_concurrent_inputs) +
               ",\"audio_plan_safe_for_optimized\":" +
               (plan.safe_for_optimized_timeline ? "true" : "false") +
               ",\"audio_plan_fallback_reason\":\"" + reason + "\"";
        std::string benchmarkJson =
            std::string(",\"audio_profile_enabled\":") +
            (benchmark != nullptr && benchmark->enabled ? "true" : "false");
        if (benchmark != nullptr && benchmark->ran) {
            benchmarkJson += ",\"audio_profile_method\":\"" +
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
            benchmarkJson += ",\"audio_profile_method\":\"" +
                escapeJsonString(benchmark == nullptr ? "not_requested" : benchmark->method) +
                "\",\"audio_inputs_open_ms\":null,\"audio_decode_ms\":null" +
                ",\"audio_filtergraph_ms\":null,\"audio_encode_ms\":null" +
                ",\"audio_output_write_ms\":null";
            if (benchmark != nullptr && !benchmark->failure_reason.empty()) {
                benchmarkJson += ",\"audio_profile_failure_reason\":\"" +
                    escapeJsonString(benchmark->failure_reason) + "\"";
            }
        }
        benchmarkJson += ",\"audio_mix_encode_passes\":1,\"audio_mix_required\":true}";
        metadata += benchmarkJson;
        return metadata;
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
}

void RenderEngine::setProgressCallback(services::ProgressCallback cb) {
    progress_cb_ = std::move(cb);
}

RenderEngine::SidecarGuard::~SidecarGuard() {
    if (engine_ != nullptr && !output_path_.empty()) {
        engine_->emitSidecar(output_path_);
    }
}

RenderResult RenderEngine::render(const plan::RenderPlan& plan) {
    // Reset accumulators on every fresh render() call.
    frames_encoded_.store(0);
    frames_decoded_.store(0);
    frames_composited_.store(0);
    encode_passes_.store(0);
    temp_bytes_written_.store(0);
    duration_seconds_.store(0.0);
    output_durable_.store(false);
    concat_mode_ = "reencode";
    last_progress_ = services::EngineProgress{};
    metrics_.reset();
    recorder_.Reset();
    // The engine CLI runs one render per process, so the process-scoped
    // I/O counters are reset here to keep sequential in-process renders
    // independent.
    services::resetIOCounters();

    RenderResult result;
    result.output_path = plan.output_path;

    // Declare the guard before renderPhase so destruction order finalizes
    // the enclosing event first, then writes the complete sidecar on every
    // success and failure return path.
    SidecarGuard sidecarGuard(this, result.output_path);

    // Block-1: the whole render call is one engine-origin "render"
    // event. RAII completion fires on every return path (success or
    // error) without touching the early returns below.
    telemetry::ScopedPhase renderPhase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAttempt,
        "engine", "render", "render");
    auto failRender = [&renderPhase, &result](const std::string& error_code) -> RenderResult {
        renderPhase.Abort(error_code, result.error);
        return result;
    };
    auto renderStart = std::chrono::steady_clock::now();

    reportProgress(0, "starting");
    reportDetailedProgress(last_progress_, 0, static_cast<int>(plan.timeline.size()), 0,
                           static_cast<int>(plan.timeline.size()), "starting", 0, 0, 0, 0);

    fs::path workBase = fs::temp_directory_path() / "velox_video_engine_plan";
    fs::path workDir;
    {
		telemetry::ScopedPhase tempPhase(
			recorder_, telemetry::kOriginWorker, telemetry::kScopeAttempt,
            "worker.temp", "create", "prepare");
        ScopedTimer t(metrics_, "workdir_create_ms");
        workDir = file::makeTempDir(workBase, "plan_job_");
        if (workDir.empty()) {
            tempPhase.Abort("tempdir_create_failed", "failed to create temp work dir");
            result.error = "failed to create temp work dir";
            return failRender("tempdir_create_failed");
        }
        tempPhase.Complete();
    }

    struct CleanupGuard {
        fs::path path;
        ~CleanupGuard() {
            if (!path.empty()) {
                std::error_code ec;
                fs::remove_all(path, ec);
            }
        }
    } cleanup{workDir};

    struct PartialOutputGuard {
        std::vector<fs::path> paths;
        ~PartialOutputGuard() {
            for (const auto& path : paths) {
                if (!path.empty()) {
                    std::error_code ec;
                    fs::remove(path, ec);
                }
            }
        }
        void track(const fs::path& path) {
            if (!path.empty()) {
                paths.push_back(path);
            }
        }
    } partialOutputs;

    fs::path outPath(plan.output_path);
    const auto publishOutput = [&](const fs::path& partial) -> std::string {
        std::string error;
        bool durable = false;
        {
            ScopedTimer timer(metrics_, "publish_atomic_ms");
            // Compatibility alias: downstream reports still expose
            // engine.copy_final_ms, but this duration is now the atomic
            // publication (fsync + rename), never a full-file copy.
            ScopedTimer legacyTimer(metrics_, "copy_final_ms");
            if (!file::publishAtomic(partial, outPath, &error, &durable)) {
                return error;
            }
        }
        output_durable_.store(durable);
        if (!durable) {
            std::cerr << "warning: output was atomically published but directory durability was not confirmed\n";
        }
        return {};
    };
    std::error_code ec_parents;
    fs::create_directories(outPath.parent_path(), ec_parents);

#ifdef VELOX_ENABLE_LIBAV
    // Copy-only is a strict packet contract. It never creates per-segment
    // MP4s, never invokes FFmpeg for segment/concat/mux work, and publishes
    // the final MP4 directly through the in-process LibAV muxer. Keep this
    // branch before the legacy segment loop so non-copy renders retain their
    // existing frame/filter/audio behavior unchanged.
    if (plan.copy_only) {
        if (plan.timeline.empty()) {
            result.error = "copy_only requires at least one timeline video";
            return failRender("copy_only_empty_timeline");
        }
        if (!plan.subtitle_tracks.empty()) {
            result.error = "copy_only does not support subtitle burn-in";
            return failRender("copy_only_subtitles_unsupported");
        }

        media::CopyOnlyMuxRequest request;
        request.output_path = outPath;
        request.video_segments.reserve(plan.timeline.size());
        // CompiledRenderPlanV2 carries the timeline in exact integer
        // microseconds; the legacy V1 fallback sums floating seconds. The
        // packet mux always receives int64 duration_us — no float is ever
        // used as the source of truth for the trim.
        double total_copy_duration = 0.0;
        int64_t total_copy_duration_us = 0;

        // Bind existing local/cache assets in place for this copy-only
        // packet path. The Go worker resolver rewrites velox-asset://
        // references into verified immutable cache paths before this plan is
        // dispatched, so the canonical wire form is either url = local path
        // or url = velox-asset://<id> with cache_key = the verified path.
        // resolveLocalAssetPath() returns both in place; libavformat opens
        // the bound path directly and this packet path never performs
        // cache -> temp copies (asset_bytes_copied stays 0). Only a genuinely
        // remote source is staged into the workdir. Note the scoping: the
        // legacy segment/mux paths below still stage assets via downloadAsset
        // into the workdir — this in-place guarantee is specific to the
        // copy-only packet pipeline.
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
            // CompiledRenderPlanV2 carries the duration as integer
            // microseconds; the legacy V1 plan as floating seconds. Either
            // source of truth must be positive and finite.
            const bool hasDuration = item.duration_us > 0 ||
                (item.duration_seconds > 0.0 && std::isfinite(item.duration_seconds));
            if (!hasDuration) {
                result.error = "copy_only requires positive finite duration for segment " +
                    std::to_string(i);
                return failRender("copy_only_duration_invalid");
            }
            const auto& source = std::get<plan::VideoSource>(item.source);
            const auto boundVideo = bindOrStage(
                source.url, source.cache_key,
                workDir / ("copy_input_" + std::to_string(i) + ".mp4"));
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
                    boundVideo.second ? "resolve" : "download");
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

            // Prefer the V2 integer microseconds; fall back to converting
            // the V1 floating seconds only when the int64 field is absent.
            const int64_t duration_us = item.duration_us > 0
                ? item.duration_us
                : static_cast<int64_t>(
                      std::llround(item.duration_seconds * 1'000'000.0));
            request.video_segments.push_back({localVideo, duration_us, item.include_audio});
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
            // Display/validation only: the FINAL_AUDIO_COPY resolver takes a
            // tolerance of tens of milliseconds, so the double derived from
            // the exact int64 total is safe; the mux trim stays int64.
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
            if (boundAudio.first.empty() || !media::hasAudioStream(boundAudio.first)) {
                result.error = "failed to resolve valid copy-only audio track";
                return failRender("copy_only_audio_invalid");
            }
            // FINAL_AUDIO_COPY contract gate for the in-process packet path.
            // The upstream-prepared final audio must be a verified MP4-AAC
            // track covering the video timeline; the packet mux then copies
            // its packets into the same MP4 as the video with zero decode,
            // zero filter and zero AAC re-encode. Anything that would need a
            // re-encode (non-AAC codec, raw ADTS container, unverified
            // transport, duration shorter than the timeline) fails closed:
            // the zero-spawn path cannot repair audio.
            finalAudioDecision = media::resolveFinalAudioModePacket(
                media::probeFinalAudioMetadata(boundAudio.first),
                true, total_copy_duration);
            if (finalAudioDecision.mode != media::FinalAudioMode::Copy) {
                result.error = "copy_only final audio is not FINAL_AUDIO_COPY: " +
                    finalAudioDecision.reason;
                return failRender("copy_only_audio_not_final_copy");
            }
            {
                telemetry::ScopedPhase assetPhase(
                    recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                    "worker.asset", boundAudio.second ? "bind" : "transfer",
                    boundAudio.second ? "resolve" : "download");
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
            "engine", "packet_mux", "composite");
        if (!plan.audio_tracks.empty()) {
            // The packet mux performs the single final mux: video packets +
            // the upstream-prepared final audio packets, no AAC encode pass.
            // Mirror the legacy muxAudio decision telemetry so downstream
            // consumers see the FINAL_AUDIO_COPY mode on this path too.
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
        if (!muxResult.output_durable) {
            std::cerr << "warning: output was atomically published but directory durability was not confirmed\n";
        }
        // Packet counters are not decoded/encoded frame counters. Keep the
        // phase event truthful and leave frames_encoded/decoded at zero; the
        // sidecar's packet work is represented by the packet_mux phase itself.
        packetPhase.Complete(
            0,
            static_cast<int64_t>(fileSize(outPath)),
            0,
            telemetry::kStatusOk);
        // The zero-spawn packet path owns the published artifact, so it
        // declares the final size in the sidecar total_size directly (the
        // ffmpeg-based paths fill this field from their progress stream).
        // The receipt's I/O amplification denominator reads this value.
        last_progress_.total_size = static_cast<int64_t>(fileSize(outPath));
        last_progress_.progress_pct = 100.0;
        last_progress_.finished = true;
        reportProgress(100, "completed");
        result.success = true;
        return result;
    }
#endif // VELOX_ENABLE_LIBAV

    // 1. Build timeline segments
    reportProgress(10, "resolving_assets");
    reportDetailedProgress(last_progress_, 0, static_cast<int>(plan.timeline.size()), 0,
                           static_cast<int>(plan.timeline.size()), "resolving_assets", 0, 0, 0,
                           std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - renderStart).count());

    std::vector<fs::path> segmentPaths;
    segmentPaths.reserve(plan.timeline.size());

    double total_duration_seconds = 0.0;

    for (size_t i = 0; i < plan.timeline.size(); ++i) {
        const auto& item = plan.timeline[i];
        fs::path segmentOut = workDir / ("segment_" + std::to_string(i) + ".mp4");
        auto params = makeParams(plan.canvas, item.transform, extractColorHex(item.source));
        params.copy_only = plan.copy_only;

        const int64_t expected_us = static_cast<int64_t>(item.duration_seconds * 1'000'000.0);
        total_duration_seconds += item.duration_seconds;

        // Per-segment timing record
        SegmentTiming seg;
        seg.index = i;
        seg.worker_index = 0;
        seg.scene_id = item.scene_id;
        int64_t segmentFrames = 0;
        int64_t segmentDecodedFrames = 0;
        services::EngineProgress segmentProgress{};
        const auto onProgress = progress_cb_;
        const bool copyOnly = params.copy_only;
        services::ProgressCallback segmentCallback =
            [this, onProgress, &segmentFrames, &segmentProgress, copyOnly, i, totalSegments = plan.timeline.size(), renderStart](const services::EngineProgress& p) {
                segmentProgress = p;
                if (p.frame > segmentFrames) {
                    segmentFrames = p.frame;
                }
                last_progress_ = p;
                if (onProgress) {
                    onProgress(p);
                }
                const int64_t reportedSegmentFrames = copyOnly ? 0 : segmentFrames;
                reportDetailedProgress(
                    p, static_cast<int>(i + 1), static_cast<int>(totalSegments),
                    static_cast<int>(i + 1), static_cast<int>(totalSegments),
                    "building_segments", frames_encoded_.load() + reportedSegmentFrames,
                    frames_decoded_.load(), frames_composited_.load() + reportedSegmentFrames,
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - renderStart).count());
            };
        auto segStart = std::chrono::steady_clock::now();
        seg.started_offset_ms = std::chrono::duration<double, std::milli>(
            segStart - renderStart).count();

        std::string args_only;
        if (std::holds_alternative<plan::ImageSource>(item.source)) {
            seg.source_type = "image";
            auto src = std::get<plan::ImageSource>(item.source);
            fs::path localImg = workDir / ("image_" + std::to_string(i) + ".jpg");
            bool gotImage;
			telemetry::ScopedPhase assetPhase(
				recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                "worker.asset", "transfer", "download");
            auto dlStart = std::chrono::steady_clock::now();
            {
                ScopedTimer t(metrics_, "asset_download_ms");
                gotImage = file::downloadAsset(src.url, localImg, src.cache_key);
            }
            if (gotImage) {
                assetPhase.Complete();
            } else {
                assetPhase.Abort("asset_download_failed", "image download failed; using fallback");
            }
            seg.asset_download_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - dlStart).count();
            if (gotImage) {
                seg.source_bytes = fileSize(localImg);
                args_only = media::buildSceneSegmentArgs(localImg, segmentOut, item.duration_seconds, params);
            } else {
                std::string hex = extractColorHex(item.source);
                args_only = media::buildColorSegmentArgs(segmentOut, item.duration_seconds, params, hex);
            }
        } else if (std::holds_alternative<plan::VideoSource>(item.source)) {
            seg.source_type = "video";
            auto src = std::get<plan::VideoSource>(item.source);
            fs::path localVid = workDir / ("video_" + std::to_string(i) + ".mp4");
            bool gotVid;
			telemetry::ScopedPhase assetPhase(
				recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                "worker.asset", "transfer", "download");
            auto dlStart = std::chrono::steady_clock::now();
            {
                ScopedTimer t(metrics_, "asset_download_ms");
                gotVid = file::downloadAsset(src.url, localVid, src.cache_key);
            }
            seg.asset_download_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - dlStart).count();
            if (!gotVid) {
                assetPhase.Abort("asset_download_failed", "failed to download video source");
                result.error = "failed to download video source for segment " + std::to_string(i);
                return failRender("asset_download_failed");
            }
            assetPhase.Complete();
            seg.source_bytes = fileSize(localVid);
            args_only = media::buildVideoSegmentArgs(localVid, segmentOut, item.duration_seconds, params, item.include_audio);
        } else if (std::holds_alternative<plan::ColorSource>(item.source)) {
            seg.source_type = "color";
            auto color = std::get<plan::ColorSource>(item.source);
            args_only = media::buildColorSegmentArgs(segmentOut, item.duration_seconds, params, color.color_hex);
        }

        if (args_only.empty()) {
            if (params.copy_only && std::holds_alternative<plan::VideoSource>(item.source)) {
                result.error = "copy_only media contract rejected video segment " + std::to_string(i);
                return failRender("copy_only_media_incompatible");
            }
            result.error = "unknown segment source type for " + std::to_string(i);
            return failRender("unknown_segment_source");
        }

        {
            // Stream-copy segments must not be reported as video encoding.
            // FFmpeg's progress `frame` value is packet/stream progress in
            // this mode, not frames produced by a video encoder.
            std::unique_ptr<telemetry::ScopedPhase> encodePhase;
            if (!params.copy_only) {
                encodePhase = std::make_unique<telemetry::ScopedPhase>(
                    recorder_, telemetry::kOriginEngine, telemetry::kScopeSegment,
                    "ffmpeg", "encode_segment_" + std::to_string(i), "encode");
            }
            auto encStart = std::chrono::steady_clock::now();
            ScopedTimer t(metrics_, "segment_build_ms");
            bool built = runFfmpegSegmentWithProgress(
                composeSegmentCmd(args_only), segmentCallback, expected_us, segmentDecodedFrames);
            if (!built) {
                seg.status = telemetry::kStatusFailed;
                seg.error_code = "encode_failed";
                seg.error_message = "failed to build timeline segment " + std::to_string(i);
                if (encodePhase) {
                    encodePhase->Abort(seg.error_code, seg.error_message);
                }
                result.error = seg.error_message;
                return failRender(seg.error_code);
            }
            const double ffmpegWallMs = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - encStart).count();
            seg.ffmpeg_encode_ms = params.copy_only ? 0.0 : ffmpegWallMs;
            seg.frames_encoded = params.copy_only ? 0 : segmentFrames;
            seg.frames_decoded = segmentDecodedFrames;
            // FFmpeg reports the frame count after the filter graph, so it
            // is the exact composited/output frame count for this segment.
            seg.frames_composited = params.copy_only ? 0 : segmentFrames;
            seg.ffmpeg_speed_x = segmentProgress.speed_x;
            if (!params.copy_only) {
                frames_encoded_.fetch_add(segmentFrames);
            }
            frames_decoded_.fetch_add(segmentDecodedFrames);
            if (!params.copy_only) {
                frames_composited_.fetch_add(segmentFrames);
            }
            seg.status = telemetry::kStatusOk;
            seg.ffmpeg_threads = 0;
            if (encodePhase) {
                encodePhase->SetDetailedMetrics(
                    static_cast<int32_t>(i), "video", -1,
                    seg.started_offset_ms,
                    std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - renderStart).count(),
                    0.0, 0.0, 0, segmentFrames);
                encodePhase->Complete(0, fileSize(segmentOut), segmentFrames,
                                      telemetry::kStatusOk);
            }
        }

        if (!params.copy_only) {
            encode_passes_.fetch_add(1);
        }
        const int64_t segBytes = fileSize(segmentOut);
        temp_bytes_written_.fetch_add(segBytes);
        segmentPaths.push_back(segmentOut);

        seg.output_bytes = segBytes;
        seg.total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - segStart).count();
        seg.finished_offset_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - renderStart).count();
        metrics_.addSegment(seg);

        int pct = 10 + static_cast<int>((static_cast<double>(i + 1) / plan.timeline.size()) * 60);
        reportProgress(pct, "building_segments");
        reportDetailedProgress(last_progress_, static_cast<int>(i + 1),
                               static_cast<int>(plan.timeline.size()), static_cast<int>(i + 1),
                               static_cast<int>(plan.timeline.size()), "building_segments",
                               frames_encoded_.load(), frames_decoded_.load(),
                               frames_composited_.load(),
                               std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - renderStart).count(),
                               true);
    }

    if (total_duration_seconds > 0.0) {
        duration_seconds_.store(total_duration_seconds);
    }

    // 2. Concatenate video segments
    reportProgress(75, "concatenating");
    reportDetailedProgress(last_progress_, static_cast<int>(plan.timeline.size()),
                           static_cast<int>(plan.timeline.size()), static_cast<int>(plan.timeline.size()),
                           static_cast<int>(plan.timeline.size()), "concatenating",
                           frames_encoded_.load(), frames_decoded_.load(), frames_composited_.load(),
                           std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - renderStart).count());
    // Keep the concat output beside the final target. The later mux or
    // subtitle pass can consume this partial directly, and the final
    // successful stage commits with rename instead of copying the whole file.
    fs::path videoOnly = file::makePartialPath(outPath);
    partialOutputs.track(videoOnly);
    {
        telemetry::ScopedPhase concatPhase(
            recorder_, telemetry::kOriginEngine, telemetry::kScopeAttempt,
            "engine", "concat", "composite");
        ScopedTimer t(metrics_, "concat_ms");
        if (!media::concatSegments(segmentPaths, videoOnly, workDir)) {
            concatPhase.Abort("concat_failed", "failed to concatenate video segments");
            result.error = "failed to concatenate video segments";
            return failRender("concat_failed");
        }
        concatPhase.Complete();
    }
    temp_bytes_written_.fetch_add(fileSize(videoOnly));
    concat_mode_ = "stream_copy";

    fs::path videoForMux = videoOnly;
    if (!plan.subtitle_tracks.empty()) {
        telemetry::ScopedPhase subtitlePhase(
            recorder_, telemetry::kOriginEngine, telemetry::kScopeSubtitleTrack,
            "subtitle", "burn_in", "subtitle");
        reportProgress(80, "burning_subtitles");
        reportDetailedProgress(last_progress_, static_cast<int>(plan.timeline.size()),
                               static_cast<int>(plan.timeline.size()), static_cast<int>(plan.timeline.size()),
                               static_cast<int>(plan.timeline.size()), "burning_subtitles",
                               frames_encoded_.load(), frames_decoded_.load(), frames_composited_.load(),
                               std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - renderStart).count());
        const auto& subtitle = plan.subtitle_tracks.front();
        fs::path localSubtitle = workDir / "subtitle_track_0.srt";
        {
            ScopedTimer t(metrics_, "asset_download_ms");
            if (!file::downloadAsset(subtitle.source, localSubtitle)) {
                subtitlePhase.Abort("subtitle_download_failed", "failed to download subtitle track");
                result.error = "failed to download subtitle track";
                return failRender("subtitle_download_failed");
            }
        }
        fs::path subtitledVideo = file::makePartialPath(outPath);
        partialOutputs.track(subtitledVideo);
        if (!burnSubtitleTrack(videoOnly, localSubtitle, subtitledVideo)) {
            subtitlePhase.Abort("subtitle_burn_failed", "failed to burn subtitle track");
            result.error = "failed to burn subtitle track";
            return failRender("subtitle_burn_failed");
        }
        temp_bytes_written_.fetch_add(fileSize(subtitledVideo));
        videoForMux = subtitledVideo;
    }

    // 3. Mix audio tracks (supports multi-track with volume/offset)
    reportProgress(85, "muxing_audio");
    reportDetailedProgress(last_progress_, static_cast<int>(plan.timeline.size()),
                           static_cast<int>(plan.timeline.size()), static_cast<int>(plan.timeline.size()),
                           static_cast<int>(plan.timeline.size()), "muxing_audio",
                           frames_encoded_.load(), frames_decoded_.load(), frames_composited_.load(),
                           std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - renderStart).count());
    if (!plan.audio_tracks.empty()) {
        telemetry::ScopedPhase audioPhase(
            recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
            "engine.audio", "mix", "audio");
        std::vector<std::pair<fs::path, const plan::AudioTrack*>> downloadedTracks;
        {
            ScopedTimer t(metrics_, "audio_download_ms");
            for (size_t t = 0; t < plan.audio_tracks.size(); ++t) {
                const auto& track = plan.audio_tracks[t];
                fs::path localAudio = workDir / ("audio_track_" + std::to_string(t) + ".m4a");
                if (file::downloadAsset(track.source_url, localAudio)) {
                    if (media::hasAudioStream(localAudio)) {
                        downloadedTracks.emplace_back(localAudio, &track);
                    } else {
                        std::cerr << "warning: audio track " << t
                                  << " contains no audio stream, skipping\n";
                    }
                } else {
                    std::cerr << "warning: failed to download audio track " << t << "\n";
                }
            }
        }

        if (downloadedTracks.empty()) {
            std::cerr << "warning: no audio tracks downloaded, exporting video without audio\n";
            std::error_code ec;
            const std::string publishError = publishOutput(videoForMux);
            if (!publishError.empty()) {
                audioPhase.Abort("audio_publish_failed", publishError);
                result.error = "failed to publish final output (no audio): " + publishError;
                return failRender("audio_publish_failed");
            }
            result.success = true;
        } else if (downloadedTracks.size() == 1
                   && !downloadedTracks[0].second->loop) {
            // A plain finite track can use the fast mux path. Looped or
            // filtered tracks must use the bounded filter graph below so
            // `-stream_loop -1` can never outrun the rendered video.
            fs::path finalMuxed = file::makePartialPath(outPath);
            partialOutputs.track(finalMuxed);
            double vol = downloadedTracks[0].second->volume;
            double offset = downloadedTracks[0].second->start_time_offset;
            bool muxOk;
            telemetry::ScopedPhase muxPhase(
                recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
                "engine.mux", "audio", "audio_mux");
            file::CommandResult muxProfile;
            media::FinalAudioDecision muxDecision;
            {
                ScopedTimer t(metrics_, "mux_audio_ms");
                // This is the final audio track. Let the resolver select FINAL_AUDIO_COPY
                // when the verified AAC metadata and neutral timing match the
                // rendered video; incompatible inputs still fall back to AAC.
                muxOk = media::muxAudio(videoForMux, downloadedTracks[0].first, finalMuxed, vol, offset, &muxProfile, true, duration_seconds_.load(), &muxDecision);
            }
            muxPhase.SetMetadataJSON(
                std::string("{\"final_mux_audio_mode\":\"") + media::finalAudioModeName(muxDecision.mode) +
                "\",\"final_mux_audio_encode_passes\":" +
                (muxDecision.mode == media::FinalAudioMode::Copy ? "0" : "1") +
                ",\"audio_metadata_verified\":" + (muxDecision.metadata.metadata_verified ? "true" : "false") +
                ",\"audio_format_name\":\"" + muxDecision.metadata.format_name +
                "\",\"audio_extradata_verified\":" + (muxDecision.metadata.extradata_verified ? "true" : "false") +
                ",\"audio_container_verified\":" + (muxDecision.metadata.container_verified ? "true" : "false") +
                ",\"decision_reason\":\"" + muxDecision.reason + "\"}");
            if (muxOk) {
                const std::string publishError = publishOutput(finalMuxed);
                if (!publishError.empty()) {
                    muxPhase.Abort("audio_publish_failed", publishError);
                    audioPhase.Abort("audio_publish_failed", publishError);
                    result.error = "failed to publish final output: " + publishError;
                    return failRender("audio_publish_failed");
                }
                result.success = true;
            } else {
                muxPhase.Abort("audio_mux_failed", "failed to mux audio track");
                audioPhase.Abort("audio_mux_failed", "failed to mux audio track");
                result.error = "failed to mux audio track";
                return failRender("audio_mux_failed");
            }
            muxPhase.Complete();
        } else {
            const auto audioPrepareStart = std::chrono::steady_clock::now();
            std::vector<audio::AudioPlanInput> audioPlanInputs;
            audioPlanInputs.reserve(downloadedTracks.size());
            for (const auto& track : downloadedTracks) {
                audioPlanInputs.push_back({
                    track.first.string(),
                    track.second->role,
                    track.second->volume,
                    track.second->start_time_offset,
                    track.second->duration_seconds,
                    track.second->loop,
                });
            }
            const auto compiledAudioPlan = audio::compileAudioPlan(
                audioPlanInputs, duration_seconds_.load());
            const auto requestedAudioStrategy = audio::requestedAudioMixStrategy();
            const auto selectedAudioStrategy = audio::resolveAudioMixStrategy(
                requestedAudioStrategy, compiledAudioPlan);

            std::ostringstream audioFilter;
            std::ostringstream audioInputs;
            for (size_t t = 0; t < downloadedTracks.size(); ++t) {
                if (downloadedTracks[t].second->loop) {
                    // Looping tracks (notably background music) are bounded
                    // by their declared duration below; they must never
                    // extend the video or shorten it when the source file is
                    // shorter than the final render.
                    audioInputs << " -stream_loop -1";
                }
                audioInputs << " -i " << file::shellQuote(downloadedTracks[t].first.string());
                if (t > 0) audioFilter << ";";
                double vol = downloadedTracks[t].second->volume;
                double offset = downloadedTracks[t].second->start_time_offset;
                audioFilter << "[" << t << ":a]";
                // A looped track without an explicit duration must be
                // bounded by the rendered video. Otherwise ffmpeg receives
                // both `-stream_loop -1` and `amix=duration=longest` and the
                // mix never terminates. This is the safe default for
                // background music; explicit durations still win.
                const double declaredDuration = downloadedTracks[t].second->duration_seconds;
                const double trackDuration = declaredDuration > 0.0
                    ? declaredDuration
                    : (downloadedTracks[t].second->loop ? duration_seconds_.load() : 0.0);
                if (trackDuration > 0.0) {
                    audioFilter << "atrim=duration=" << trackDuration
                                << ",asetpts=PTS-STARTPTS,";
                }
                audioFilter << "volume=" << vol;
                if (offset > 0.0) {
                    // adelay accepts integer milliseconds; round to the
                    // nearest ms instead of truncating (offset*1000 truncation
                    // would silently shorten the offset and is banned).
                    const int delayMs = static_cast<int>(std::llround(offset * 1000.0));
                    audioFilter << ",adelay=" << delayMs << "|" << delayMs;
                }
                audioFilter << "[a" << t << "]";
            }
            const int n = static_cast<int>(downloadedTracks.size());
            std::size_t filterCount = downloadedTracks.size();
            std::size_t amixInputCount = downloadedTracks.size();
            if (selectedAudioStrategy == audio::AudioMixStrategy::OptimizedTimeline) {
                audioFilter.str(optimizedAudioFilter(downloadedTracks, compiledAudioPlan));
                audioFilter.clear();
                filterCount += compiledAudioPlan.primary_indices.size() > 1 ? 1 : 0;
                amixInputCount = 0;
            } else {
                audioFilter << ";";
                for (int t = 0; t < n; ++t) {
                    audioFilter << "[a" << t << "]";
                }
                audioFilter << "amix=inputs=" << n << ":duration=longest[aout]";
            }

            fs::path mixedAudio = workDir / "mixed_audio.m4a";
            std::ostringstream mixCmd;
            mixCmd << "ffmpeg -y -hide_banner -loglevel error"
                   << audioInputs.str()
                   << " -filter_complex " << file::shellQuote(audioFilter.str())
                   << " -map \"[aout]\" -t " << duration_seconds_.load()
                   << " -c:a aac "
                   << file::shellQuote(mixedAudio.string());

            metrics_.addMs("audio_prepare_ms", std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - audioPrepareStart).count());

            const auto audioBenchmark = audio::runAudioMixBenchmark(
                audioPlanInputs, audioFilter.str(), duration_seconds_.load(), workDir.string());

            bool mixOk;
            telemetry::ScopedPhase audioEncodePhase(
                recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
                "engine.audio", "encode", "audio_encode", "audio", "audio_mix_encode");
            audioEncodePhase.SetMetadataJSON(
                audioPlanMetadata(compiledAudioPlan, requestedAudioStrategy,
                                  selectedAudioStrategy, downloadedTracks.size(),
                                  filterCount, amixInputCount, &audioBenchmark));
            {
                // This command performs the multi-track filter graph and AAC
                // encoding together. Keep it as one truthful timing bucket;
                // do not invent a separate encode duration that the process
                // does not expose independently.
                ScopedTimer t(metrics_, "mix_audio_ms");
                const std::string command = mixCmd.str();
                const file::CommandResult mixProfile = file::runCommandTimed(command);
                std::error_code outputEc;
                const auto outputBytes = fs::file_size(mixedAudio, outputEc);
                uintmax_t inputBytes = 0;
                for (const auto& track : downloadedTracks) {
                    std::error_code inputEc;
                    const auto bytes = fs::file_size(track.first, inputEc);
                    if (!inputEc) {
                        inputBytes += bytes;
                    }
                }
                std::cerr << "{\"metric\":\"ffmpeg.audio_mix_encode\",\"value\":" << mixProfile.wall_ms
                          << ",\"ok\":" << (mixProfile.ok ? "true" : "false")
                          << ",\"exit_code\":" << mixProfile.exit_code
                          << ",\"child_user_ms\":" << mixProfile.child_user_ms
                          << ",\"child_system_ms\":" << mixProfile.child_system_ms
                          << ",\"child_max_rss_kb\":" << mixProfile.child_max_rss_kb
                          << ",\"child_input_blocks\":" << mixProfile.child_input_blocks
                          << ",\"child_output_blocks\":" << mixProfile.child_output_blocks
                          << ",\"input_audio_bytes\":" << inputBytes
                          << ",\"output_bytes\":" << (outputEc ? 0 : outputBytes)
                          << ",\"command\":\"" << escapeJsonString(command) << "\"}"
                          << std::endl;
                mixOk = mixProfile.ok;
            }
            if (mixOk) {
                audioEncodePhase.Complete();
            } else {
                audioEncodePhase.Abort("audio_mix_failed", "failed to mix audio tracks");
            }
            if (mixOk) {
                fs::path finalMuxed = file::makePartialPath(outPath);
            partialOutputs.track(finalMuxed);
                bool muxOk;
                telemetry::ScopedPhase muxPhase(
                    recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
                    "engine.mux", "audio", "audio_mux");
                file::CommandResult muxProfile;
                media::FinalAudioDecision muxDecision;
                {
                    ScopedTimer t(metrics_, "mux_audio_ms");
                    muxOk = media::muxAudio(videoForMux, mixedAudio, finalMuxed, 1.0, 0.0, &muxProfile, true, duration_seconds_.load(), &muxDecision);
                }
                muxPhase.SetMetadataJSON(
                    std::string("{\"final_mux_audio_mode\":\"") + media::finalAudioModeName(muxDecision.mode) +
                    "\",\"final_mux_audio_encode_passes\":" +
                    (muxDecision.mode == media::FinalAudioMode::Copy ? "0" : "1") +
                    ",\"audio_metadata_verified\":" + (muxDecision.metadata.metadata_verified ? "true" : "false") +
                    ",\"audio_codec\":\"" + muxDecision.metadata.codec +
                    "\",\"audio_sample_rate\":" + std::to_string(muxDecision.metadata.sample_rate) +
                    ",\"audio_channels\":" + std::to_string(muxDecision.metadata.channels) +
                    ",\"audio_channel_layout\":\"" + muxDecision.metadata.channel_layout +
                    "\",\"audio_duration_seconds\":" + std::to_string(muxDecision.metadata.duration_seconds) +
                    ",\"audio_start_time_seconds\":" + std::to_string(muxDecision.metadata.start_time_seconds) +
                    ",\"audio_format_name\":\"" + muxDecision.metadata.format_name +
                    "\",\"audio_extradata_verified\":" + (muxDecision.metadata.extradata_verified ? "true" : "false") +
                    ",\"audio_container_verified\":" + (muxDecision.metadata.container_verified ? "true" : "false") +
                    ",\"decision_reason\":\"" + muxDecision.reason + "\"}");
                if (muxOk) {
                    const std::string publishError = publishOutput(finalMuxed);
                    if (!publishError.empty()) {
                        muxPhase.Abort("audio_publish_failed", publishError);
                        audioPhase.Abort("audio_publish_failed", publishError);
                        result.error = "failed to publish final output: " + publishError;
                        return failRender("audio_publish_failed");
                    }
                    result.success = true;
                } else {
                    muxPhase.Abort("audio_mux_failed", "failed to mux mixed audio");
                    audioPhase.Abort("audio_mux_failed", "failed to mux mixed audio");
                    result.error = "failed to mux mixed audio";
                    return failRender("audio_mux_failed");
                }
                muxPhase.Complete();
            } else {
                std::cerr << "warning: audio mix failed, exporting video without audio\n";                    const std::string publishError = publishOutput(videoForMux);
                    if (!publishError.empty()) {
                        audioPhase.Abort("audio_publish_failed", publishError);
                        result.error = "failed to publish final output (mix failed): " + publishError;
                        return failRender("audio_publish_failed");
                    }

                result.success = true;
            }
        }
    } else {
        const std::string publishError = publishOutput(videoForMux);
        if (!publishError.empty()) {
            result.error = "failed to publish final output (no audio tracks): " + publishError;
            return failRender("audio_publish_failed");
        }
        result.success = true;
    }

    reportProgress(100, "completed");
    // Segment-level FFmpeg progress is best-effort and may stop below 100
    // when the final concat/mux phases finish without another FFmpeg
    // progress callback. The render lifecycle is complete here, so make the
    // persisted sidecar reflect the canonical terminal state.
    last_progress_.progress_pct = 100.0;
    last_progress_.finished = true;

    return result;
}

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

    // ── Real I/O counters (process-scoped) ──────────────────────
    {
        const auto& io = services::ioCounters();
        s << ",\"io_counters\":{";
        s << "\"file_copy_count\":" << io.file_copy_count.load();
        s << ",\"file_copy_bytes\":" << io.file_copy_bytes.load();
        s << ",\"asset_bytes_copied\":" << io.asset_bytes_copied.load();
        s << ",\"input_open_count\":" << io.input_open_count.load();
        s << ",\"input_reopen_count\":" << io.input_reopen_count.load();
        s << "}";
    }

    // ── Engine-declared process counters ────────────────────────
    // The engine's OWN ledger of external tool spawns (disjoint from the
    // Go-side /proc sampler: the engine counts what it launched) and its
    // own getrusage usage (CPU user/system ms, voluntary/involuntary
    // context switches, minor/major page faults). The Phase-1 copy-only
    // invariant external_spawn_count == 0 is readable straight from this
    // block.
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

    // ── Phase-level timings ────────────────────────────────────
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

    // ── Per-segment timing records ──────────────────────────────
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

    // ── Block-1: detailed phase/event stream ─────────────────────
    s << ",\"phases\":[";
    {
        auto phases = recorder_.Snapshot();
        for (size_t i = 0; i < phases.size(); ++i) {
            if (i > 0) s << ",";
            phases[i].AppendJson(s);
        }
    }
    s << "]";

    // Category rollups are derived from the same append-only event stream.
    // They are optional for old readers and never replace phases[].
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
