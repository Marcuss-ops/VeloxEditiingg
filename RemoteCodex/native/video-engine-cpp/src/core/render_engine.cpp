#include "velox/core/render_engine.hpp"
#include "render_engine_helpers.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_utils.hpp"
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
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
    concat_mode_ = "reencode";
    last_progress_ = services::EngineProgress{};
    metrics_.reset();
    recorder_.Reset();

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
            recorder_, telemetry::kOriginWorker, telemetry::kScopeArtifact,
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

    fs::path outPath(plan.output_path);
    std::error_code ec_parents;
    fs::create_directories(outPath.parent_path(), ec_parents);

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
        services::ProgressCallback segmentCallback =
            [this, onProgress, &segmentFrames, &segmentProgress, i, totalSegments = plan.timeline.size(), renderStart](const services::EngineProgress& p) {
                segmentProgress = p;
                if (p.frame > segmentFrames) {
                    segmentFrames = p.frame;
                }
                last_progress_ = p;
                if (onProgress) {
                    onProgress(p);
                }
                reportDetailedProgress(
                    p, static_cast<int>(i + 1), static_cast<int>(totalSegments),
                    static_cast<int>(i + 1), static_cast<int>(totalSegments),
                    "building_segments", frames_encoded_.load() + segmentFrames,
                    frames_decoded_.load(), frames_composited_.load() + segmentFrames,
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
                recorder_, telemetry::kOriginEngine, telemetry::kScopeSegment,
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
                recorder_, telemetry::kOriginEngine, telemetry::kScopeSegment,
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
            // Block-1: per-segment encode event (engine origin, segment
            // scope). Completed with output bytes + cumulative frames.
            telemetry::ScopedPhase encodePhase(
                recorder_, telemetry::kOriginEngine, telemetry::kScopeSegment,
                "ffmpeg", "encode_segment_" + std::to_string(i), "encode");
            auto encStart = std::chrono::steady_clock::now();
            ScopedTimer t(metrics_, "segment_build_ms");
            bool built = runFfmpegSegmentWithProgress(
                composeSegmentCmd(args_only), segmentCallback, expected_us, segmentDecodedFrames);
            if (!built) {
                seg.status = telemetry::kStatusFailed;
                seg.error_code = "encode_failed";
                seg.error_message = "failed to build timeline segment " + std::to_string(i);
                encodePhase.Abort(seg.error_code, seg.error_message);
                result.error = seg.error_message;
                return failRender(seg.error_code);
            }
            seg.ffmpeg_encode_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - encStart).count();
            seg.frames_encoded = segmentFrames;
            seg.frames_decoded = segmentDecodedFrames;
            // FFmpeg reports the frame count after the filter graph, so it
            // is the exact composited/output frame count for this segment.
            seg.frames_composited = segmentFrames;
            seg.ffmpeg_speed_x = segmentProgress.speed_x;
            frames_encoded_.fetch_add(segmentFrames);
            frames_decoded_.fetch_add(segmentDecodedFrames);
            frames_composited_.fetch_add(segmentFrames);
            seg.status = telemetry::kStatusOk;
            seg.ffmpeg_threads = 0;
            encodePhase.SetDetailedMetrics(
                static_cast<int32_t>(i), "video", -1,
                seg.started_offset_ms,
                std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - renderStart).count(),
                0.0, 0.0, 0, segmentFrames);
            encodePhase.Complete(0, fileSize(segmentOut), segmentFrames,
                                 telemetry::kStatusOk);
        }

        encode_passes_.fetch_add(1);
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
                                   std::chrono::steady_clock::now() - renderStart).count());
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
    fs::path videoOnly = workDir / "video_only.mp4";
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
        fs::path subtitledVideo = workDir / "video_subtitled.mp4";
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
            {
                ScopedTimer t(metrics_, "copy_final_ms");
                fs::copy_file(videoForMux, outPath, fs::copy_options::overwrite_existing, ec);
            }
            if (ec) {
                audioPhase.Abort("audio_copy_failed", "failed to copy final output (no audio)");
                result.error = "failed to copy final output (no audio)";
                return failRender("audio_copy_failed");
            }
            result.success = true;
        } else if (downloadedTracks.size() == 1
                   && !downloadedTracks[0].second->loop) {
            // A plain finite track can use the fast mux path. Looped or
            // filtered tracks must use the bounded filter graph below so
            // `-stream_loop -1` can never outrun the rendered video.
            fs::path finalMuxed = workDir / "final_muxed.mp4";
            double vol = downloadedTracks[0].second->volume;
            double offset = downloadedTracks[0].second->start_time_offset;
            bool muxOk;
            telemetry::ScopedPhase muxPhase(
                recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
                "engine.mux", "audio", "encode");
            {
                ScopedTimer t(metrics_, "mux_audio_ms");
                muxOk = media::muxAudio(videoForMux, downloadedTracks[0].first, finalMuxed, vol, offset);
            }
            if (muxOk) {
                std::error_code ec;
                {
                    ScopedTimer tCopy(metrics_, "copy_final_ms");
                    fs::copy_file(finalMuxed, outPath, fs::copy_options::overwrite_existing, ec);
                }
                if (ec) {
                    muxPhase.Abort("audio_copy_failed", "failed to copy final output");
                    audioPhase.Abort("audio_copy_failed", "failed to copy final output");
                    result.error = "failed to copy final output";
                    return failRender("audio_copy_failed");
                }
                temp_bytes_written_.fetch_add(fileSize(finalMuxed));
                result.success = true;
            } else {
                muxPhase.Abort("audio_mux_failed", "failed to mux audio track");
                audioPhase.Abort("audio_mux_failed", "failed to mux audio track");
                result.error = "failed to mux audio track";
                return failRender("audio_mux_failed");
            }
            muxPhase.Complete();
        } else {
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
                    int delayMs = static_cast<int>(offset * 1000);
                    audioFilter << ",adelay=" << delayMs << "|" << delayMs;
                }
                audioFilter << "[a" << t << "]";
            }
            int n = static_cast<int>(downloadedTracks.size());
            audioFilter << ";";
            for (int t = 0; t < n; ++t) {
                audioFilter << "[a" << t << "]";
            }
            audioFilter << "amix=inputs=" << n << ":duration=longest[aout]";

            fs::path mixedAudio = workDir / "mixed_audio.m4a";
            std::ostringstream mixCmd;
            mixCmd << "ffmpeg -y -hide_banner -loglevel error"
                   << audioInputs.str()
                   << " -filter_complex " << file::shellQuote(audioFilter.str())
                   << " -map \"[aout]\" -t " << duration_seconds_.load()
                   << " -c:a aac "
                   << file::shellQuote(mixedAudio.string());

            if (file::runCommand(mixCmd.str())) {
                fs::path finalMuxed = workDir / "final_muxed.mp4";
                bool muxOk;
                telemetry::ScopedPhase muxPhase(
                    recorder_, telemetry::kOriginEngine, telemetry::kScopeAudioTrack,
                    "engine.mux", "audio", "encode");
                {
                    ScopedTimer t(metrics_, "mux_audio_ms");
                    muxOk = media::muxAudio(videoForMux, mixedAudio, finalMuxed);
                }
                if (muxOk) {
                    std::error_code ec;
                    {
                        ScopedTimer tCopy(metrics_, "copy_final_ms");
                        fs::copy_file(finalMuxed, outPath, fs::copy_options::overwrite_existing, ec);
                    }
                    if (ec) {
                        muxPhase.Abort("audio_copy_failed", "failed to copy final output");
                        audioPhase.Abort("audio_copy_failed", "failed to copy final output");
                        result.error = "failed to copy final output";
                        return failRender("audio_copy_failed");
                    }
                    temp_bytes_written_.fetch_add(fileSize(finalMuxed));
                    result.success = true;
                } else {
                    muxPhase.Abort("audio_mux_failed", "failed to mux mixed audio");
                    audioPhase.Abort("audio_mux_failed", "failed to mux mixed audio");
                    result.error = "failed to mux mixed audio";
                    return failRender("audio_mux_failed");
                }
                muxPhase.Complete();
            } else {
                std::cerr << "warning: audio mix failed, exporting video without audio\n";
                std::error_code ec;
                {
                    ScopedTimer t(metrics_, "copy_final_ms");
                    fs::copy_file(videoForMux, outPath, fs::copy_options::overwrite_existing, ec);
                }                    if (ec) {
                        audioPhase.Abort("audio_copy_failed", "failed to copy final output (mix failed)");
                        result.error = "failed to copy final output (mix failed)";
                        return failRender("audio_copy_failed");
                    }

                result.success = true;
            }
        }
    } else {
        std::error_code ec;
        {
            ScopedTimer t(metrics_, "copy_final_ms");
            fs::copy_file(videoForMux, outPath, fs::copy_options::overwrite_existing, ec);
        }
        if (ec) {
            result.error = "failed to copy final output (no audio tracks)";
            return failRender("audio_copy_failed");
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
    s << ",\"output_path\":\"" << escapeProgressJsonString(outPath.string()) << "\"";

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
