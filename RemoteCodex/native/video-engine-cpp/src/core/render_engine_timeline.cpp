#include "velox/core/render_engine.hpp"
#include "render_engine_helpers.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/frame_pipeline.hpp"
#include "velox/services/segment_scheduler.hpp"
#include "velox/services/media_utils.hpp"

#ifdef VELOX_ENABLE_LIBAV
#include "velox/services/segment_execution_libav.hpp"
#endif

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace velox::core {

namespace {
    using render_detail::composeSegmentCmd;
    using render_detail::extractColorHex;
    using render_detail::fileSize;
    using render_detail::makeParams;
    using render_detail::reportDetailedProgress;
    using render_detail::reportProgress;
    using render_detail::runFfmpegSegmentWithProgress;

    fs::path numberedWorkPath(const fs::path& workDir, const char* prefix,
                              const char* suffix, std::size_t index) {
        return workDir / (std::string(prefix) + std::to_string(index) + suffix);
    }

#ifdef VELOX_ENABLE_LIBAV
    struct NativeThreadConfig {
        int decoder_threads{0};
        int encoder_threads{0};
    };

    NativeThreadConfig nativeThreadConfig() {
        NativeThreadConfig config;
        if (const char* value = std::getenv("VELOX_NATIVE_DECODER_THREADS")) {
            try {
                const int parsed = std::stoi(value);
                if (parsed > 0) config.decoder_threads = parsed;
            } catch (...) {
            }
        }
        if (const char* value = std::getenv("VELOX_NATIVE_ENCODER_THREADS")) {
            try {
                const int parsed = std::stoi(value);
                if (parsed > 0) config.encoder_threads = parsed;
            } catch (...) {
            }
        }
        return config;
    }
#endif
}

bool RenderEngine::renderLegacyTimeline(
    const plan::RenderPlan& plan,
    const std::filesystem::path& workDir,
    const std::chrono::steady_clock::time_point& renderStart,
    std::vector<std::filesystem::path>& segmentPaths,
    double& total_duration_seconds,
    RenderResult& result,
    const std::function<RenderResult(const std::string&)>& failRender) {
    bool nativeBatchCompleted = false;

#ifdef VELOX_ENABLE_LIBAV
    bool nativeBatchEligible = plan.version == plan::kRenderPlanVersionV1 &&
        plan.subtitle_tracks.empty() && !plan.timeline.empty();
    for (const auto& item : plan.timeline) {
        if (!std::holds_alternative<plan::VideoSource>(item.source) ||
            item.include_audio || item.transform.slow_zoom) {
            nativeBatchEligible = false;
            break;
        }
    }
    if (nativeBatchEligible) {
        struct NativeSegmentJob {
            std::size_t index{0};
            fs::path input_path;
            fs::path output_path;
            std::string scene_id;
            int64_t duration_us{0};
            int64_t source_in_us{0};
        };
        struct NativeSegmentOutcome {
            media::FramePipelineResult pipeline;
            double started_offset_ms{0.0};
            double finished_offset_ms{0.0};
            double wall_ms{0.0};
        };

        std::vector<NativeSegmentJob> jobs;
        jobs.reserve(plan.timeline.size());
        for (std::size_t i = 0; i < plan.timeline.size(); ++i) {
            const auto& item = plan.timeline[i];
            const auto& source = std::get<plan::VideoSource>(item.source);
            const int64_t duration_us = item.source_duration_us > 0
                ? item.source_duration_us
                : item.duration_us > 0
                ? item.duration_us
                : static_cast<int64_t>(std::llround(item.duration_seconds * 1'000'000.0));
            if (duration_us <= 0) {
                result.error = "native segment requires positive duration for segment " +
                    std::to_string(i);
                failRender("native_segment_duration_invalid");
                return false;
            }
            const fs::path local_video = numberedWorkPath(workDir, "native_video_", ".mp4", i);
            telemetry::ScopedPhase assetPhase(
                recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                "worker.asset", "transfer", "download");
            const auto download_start = std::chrono::steady_clock::now();
            if (!file::downloadAsset(source.url, local_video, source.cache_key)) {
                assetPhase.Abort("asset_download_failed", "failed to download native video source");
                result.error = "failed to download video source for segment " + std::to_string(i);
                failRender("asset_download_failed");
                return false;
            }
            assetPhase.Complete();
            metrics_.addMs("asset_download_ms", std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - download_start).count());
            total_duration_seconds += static_cast<double>(duration_us) / 1'000'000.0;
            jobs.push_back(NativeSegmentJob{
                i,
                local_video,
                numberedWorkPath(workDir, "segment_", ".mp4", i),
                item.scene_id,
                duration_us,
                item.source_in_us,
            });
        }

        std::size_t parallelism = 1;
        if (const char* configured = std::getenv("VELOX_NATIVE_SEGMENT_WORKERS")) {
            try {
                const auto parsed = std::stoull(configured);
                if (parsed > 0) parallelism = std::min<std::size_t>(parsed, 8);
            } catch (...) {
                parallelism = 1;
            }
        }
        const NativeThreadConfig thread_config = nativeThreadConfig();
        media::SegmentScheduler scheduler(media::SegmentSchedulerConfig{
            media::ExecutionBudget{/*cpu_tokens*/0, /*memory_bytes*/0,
                                   parallelism, thread_config.encoder_threads}});
        std::vector<NativeSegmentOutcome> outcomes(jobs.size());
        const auto scheduled = scheduler.run(jobs.size(), [&](std::size_t index) {
            const auto& job = jobs[index];
            auto& outcome = outcomes[index];
            const auto start = std::chrono::steady_clock::now();
            outcome.started_offset_ms = std::chrono::duration<double, std::milli>(
                start - renderStart).count();
            telemetry::ScopedPhase encodePhase(
                recorder_, telemetry::kOriginEngine, telemetry::kScopeSegment,
                "engine.frame_pipeline", "encode_segment", "encode", "",
                "native_encode_segment_" + std::to_string(job.index));
            media::FramePipelineConfig config;
            config.input_path = job.input_path;
            config.output_path = job.output_path;
            config.width = plan.canvas.width;
            config.height = plan.canvas.height;
            config.fps_num = plan.canvas.fps_num > 0 ? plan.canvas.fps_num : plan.canvas.fps;
            config.fps_den = plan.canvas.fps_den > 0 ? plan.canvas.fps_den : 1;
            config.source_in_us = job.source_in_us;
            config.source_duration_us = job.duration_us;
            config.codec = "libx264";
            config.preset = "medium";
            config.decoder_threads = thread_config.decoder_threads;
            config.encoder_threads = thread_config.encoder_threads;
            const bool success = media::renderFrames(config, &outcome.pipeline);
            outcome.wall_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - start).count();
            outcome.finished_offset_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - renderStart).count();
            if (!success) {
                encodePhase.Abort("native_encode_failed", outcome.pipeline.error);
                return media::SegmentTaskResult{
                    false, outcome.pipeline.error.empty()
                        ? "native frame pipeline failed"
                        : outcome.pipeline.error};
            }
            encodePhase.SetDetailedMetrics(
                static_cast<int32_t>(job.index), "video", -1,
                outcome.started_offset_ms, outcome.finished_offset_ms,
                0.0, 0.0, 0, outcome.pipeline.frames_encoded);
            encodePhase.Complete(0, fileSize(job.output_path),
                                 outcome.pipeline.frames_encoded, telemetry::kStatusOk);
            return media::SegmentTaskResult{true, {}};
        });

        segmentPaths.reserve(jobs.size());
        for (std::size_t i = 0; i < jobs.size(); ++i) {
            if (!scheduled[i].success || !outcomes[i].pipeline.success) {
                result.error = "native segment " + std::to_string(i) + " failed: " +
                    (!scheduled[i].error.empty() ? scheduled[i].error : outcomes[i].pipeline.error);
                failRender("native_segment_failed");
                return false;
            }
            const auto& job = jobs[i];
            const auto& outcome = outcomes[i];
            recordFramePipeline(outcome.pipeline);
            const int64_t output_bytes = fileSize(job.output_path);
            SegmentTiming segment;
            segment.index = job.index;
            segment.worker_index = 0;
            segment.scene_id = job.scene_id;
            segment.source_type = "video";
            segment.total_ms = outcome.wall_ms;
            segment.ffmpeg_encode_ms = outcome.wall_ms;
            segment.source_bytes = fileSize(job.input_path);
            segment.output_bytes = output_bytes;
            segment.frames_encoded = outcome.pipeline.frames_encoded;
            segment.frames_decoded = outcome.pipeline.frames_decoded;
            segment.frames_composited = outcome.pipeline.frames_encoded;
            segment.status = telemetry::kStatusOk;
            segment.started_offset_ms = outcome.started_offset_ms;
            segment.finished_offset_ms = outcome.finished_offset_ms;
            metrics_.addMs("segment_build_ms", outcome.wall_ms);
            metrics_.addMs("native_encode_ms", outcome.wall_ms);
            metrics_.addSegment(segment);
            frames_encoded_.fetch_add(outcome.pipeline.frames_encoded);
            frames_decoded_.fetch_add(outcome.pipeline.frames_decoded);
            frames_composited_.fetch_add(outcome.pipeline.frames_encoded);
            encode_passes_.fetch_add(1);
            temp_bytes_written_.fetch_add(output_bytes);
            segmentPaths.push_back(job.output_path);
        }
        nativeBatchCompleted = true;
        reportProgress(70, "building_native_segments");
        reportDetailedProgress(last_progress_, static_cast<int>(jobs.size()),
                               static_cast<int>(jobs.size()), static_cast<int>(jobs.size()),
                               static_cast<int>(jobs.size()), "building_native_segments",
                               frames_encoded_.load(), frames_decoded_.load(),
                               frames_composited_.load(),
                               std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - renderStart).count(), true);
    }
#endif

    if (!nativeBatchCompleted) {
        for (std::size_t i = 0; i < plan.timeline.size(); ++i) {
            const auto& item = plan.timeline[i];
            fs::path segmentOut = numberedWorkPath(workDir, "segment_", ".mp4", i);
            auto params = makeParams(plan.canvas, item.transform, extractColorHex(item.source));
            params.copy_only = plan.copy_only;
            const int64_t expected_us = static_cast<int64_t>(item.duration_seconds * 1'000'000.0);
            total_duration_seconds += item.duration_seconds;

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
                [this, onProgress, &segmentFrames, &segmentProgress, copyOnly, i,
                 totalSegments = plan.timeline.size(), renderStart](
                    const services::EngineProgress& p) {
                    segmentProgress = p;
                    if (p.frame > segmentFrames) segmentFrames = p.frame;
                    last_progress_ = p;
                    if (onProgress) onProgress(p);
                    const int64_t reportedSegmentFrames = copyOnly ? 0 : segmentFrames;
                    reportDetailedProgress(
                        p, static_cast<int>(i + 1), static_cast<int>(totalSegments),
                        static_cast<int>(i + 1), static_cast<int>(totalSegments),
                        "building_segments", frames_encoded_.load() + reportedSegmentFrames,
                        frames_decoded_.load(), frames_composited_.load() + reportedSegmentFrames,
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - renderStart).count());
                };
            const auto segStart = std::chrono::steady_clock::now();
            seg.started_offset_ms = std::chrono::duration<double, std::milli>(
                segStart - renderStart).count();

            std::string args_only;
            bool useNativeTranscode = false;
            fs::path nativeTranscodeInput;
            if (std::holds_alternative<plan::ImageSource>(item.source)) {
                seg.source_type = "image";
                auto src = std::get<plan::ImageSource>(item.source);
                fs::path localImg = numberedWorkPath(workDir, "image_", ".jpg", i);
                bool gotImage;
                telemetry::ScopedPhase assetPhase(
                    recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                    "worker.asset", "transfer", "download");
                const auto dlStart = std::chrono::steady_clock::now();
                {
                    ScopedTimer t(metrics_, "asset_download_ms");
                    gotImage = file::downloadAsset(src.url, localImg, src.cache_key);
                }
                if (gotImage) assetPhase.Complete();
                else assetPhase.Abort("asset_download_failed", "image download failed; using fallback");
                seg.asset_download_ms = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - dlStart).count();
                if (gotImage) {
                    seg.source_bytes = fileSize(localImg);
                    args_only = media::buildSceneSegmentArgs(localImg, segmentOut, item.duration_seconds, params);
                } else {
                    args_only = media::buildColorSegmentArgs(
                        segmentOut, item.duration_seconds, params, extractColorHex(item.source));
                }
            } else if (std::holds_alternative<plan::VideoSource>(item.source)) {
                seg.source_type = "video";
                auto src = std::get<plan::VideoSource>(item.source);
                fs::path localVid = numberedWorkPath(workDir, "video_", ".mp4", i);
                bool gotVid;
                telemetry::ScopedPhase assetPhase(
                    recorder_, telemetry::kOriginWorker, telemetry::kScopeTask,
                    "worker.asset", "transfer", "download");
                const auto dlStart = std::chrono::steady_clock::now();
                {
                    ScopedTimer t(metrics_, "asset_download_ms");
                    gotVid = file::downloadAsset(src.url, localVid, src.cache_key);
                }
                seg.asset_download_ms = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - dlStart).count();
                if (!gotVid) {
                    assetPhase.Abort("asset_download_failed", "failed to download video source");
                    result.error = "failed to download video source for segment " + std::to_string(i);
                    failRender("asset_download_failed");
                    return false;
                }
                assetPhase.Complete();
                seg.source_bytes = fileSize(localVid);
#ifdef VELOX_ENABLE_LIBAV
                const bool legacyNeedsFfmpeg = item.include_audio ||
                    item.transform.slow_zoom || !plan.subtitle_tracks.empty();
                if (legacyNeedsFfmpeg) {
                    args_only = media::buildVideoSegmentArgs(
                        localVid, segmentOut, item.duration_seconds, params, item.include_audio);
                } else {
                    useNativeTranscode = true;
                    nativeTranscodeInput = localVid;
                    args_only = "native_frame_pipeline";
                }
#else
                args_only = media::buildVideoSegmentArgs(
                    localVid, segmentOut, item.duration_seconds, params, item.include_audio);
#endif
            } else if (std::holds_alternative<plan::ColorSource>(item.source)) {
                seg.source_type = "color";
                const auto color = std::get<plan::ColorSource>(item.source);
                args_only = media::buildColorSegmentArgs(
                    segmentOut, item.duration_seconds, params, color.color_hex);
            }

            if (args_only.empty()) {
                if (params.copy_only && std::holds_alternative<plan::VideoSource>(item.source)) {
                    result.error = "copy_only media contract rejected video segment " + std::to_string(i);
                    failRender("copy_only_media_incompatible");
                    return false;
                }
                result.error = "unknown segment source type for " + std::to_string(i);
                failRender("unknown_segment_source");
                return false;
            }

            {
                std::unique_ptr<telemetry::ScopedPhase> encodePhase;
                if (!params.copy_only) {
                    encodePhase = std::make_unique<telemetry::ScopedPhase>(
                        recorder_,
                        useNativeTranscode ? telemetry::kOriginEngine : telemetry::kOriginFFmpeg,
                        telemetry::kScopeSegment,
                        useNativeTranscode ? "engine.frame_pipeline" : "ffmpeg",
                        "encode_segment", "encode", "",
                        useNativeTranscode
                            ? "native_encode_segment_" + std::to_string(i)
                            : "encode_segment_" + std::to_string(i));
                }
                const auto encStart = std::chrono::steady_clock::now();
                ScopedTimer t(metrics_, "segment_build_ms");
                bool built = false;
#ifdef VELOX_ENABLE_LIBAV
                if (useNativeTranscode) {
                    media::FramePipelineConfig nativeConfig;
                    nativeConfig.input_path = nativeTranscodeInput;
                    nativeConfig.output_path = segmentOut;
                    nativeConfig.width = plan.canvas.width;
                    nativeConfig.height = plan.canvas.height;
                    nativeConfig.fps_num = plan.canvas.fps_num > 0
                        ? plan.canvas.fps_num : plan.canvas.fps;
                    nativeConfig.fps_den = plan.canvas.fps_den > 0
                        ? plan.canvas.fps_den : 1;
                    nativeConfig.source_in_us = item.source_in_us;
                    nativeConfig.source_duration_us = expected_us;
                    nativeConfig.codec = "libx264";
                    nativeConfig.preset = "medium";
                    const NativeThreadConfig fallback_threads = nativeThreadConfig();
                    nativeConfig.decoder_threads = fallback_threads.decoder_threads;
                    nativeConfig.encoder_threads = fallback_threads.encoder_threads;
                    media::FramePipelineResult nativeResult;
                    built = media::renderFrames(nativeConfig, &nativeResult);
                    recordFramePipeline(nativeResult);
                    segmentFrames = nativeResult.frames_encoded;
                    segmentDecodedFrames = nativeResult.frames_decoded;
                    if (!built) {
                        seg.error_message = nativeResult.error.empty()
                            ? "native frame pipeline failed" : nativeResult.error;
                    }
                } else {
                    built = runFfmpegSegmentWithProgress(
                        composeSegmentCmd(args_only), segmentCallback,
                        expected_us, segmentDecodedFrames);
                }
#else
                built = runFfmpegSegmentWithProgress(
                    composeSegmentCmd(args_only), segmentCallback,
                    expected_us, segmentDecodedFrames);
#endif
                if (!built) {
                    seg.status = telemetry::kStatusFailed;
                    seg.error_code = "encode_failed";
                    seg.error_message = "failed to build timeline segment " + std::to_string(i);
                    if (encodePhase) encodePhase->Abort(seg.error_code, seg.error_message);
                    result.error = seg.error_message;
                    failRender(seg.error_code);
                    return false;
                }
                const double ffmpegWallMs = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - encStart).count();
                seg.ffmpeg_encode_ms = params.copy_only ? 0.0 : ffmpegWallMs;
                seg.frames_encoded = params.copy_only ? 0 : segmentFrames;
                seg.frames_decoded = segmentDecodedFrames;
                seg.frames_composited = params.copy_only ? 0 : segmentFrames;
                seg.ffmpeg_speed_x = segmentProgress.speed_x;
                if (!params.copy_only) frames_encoded_.fetch_add(segmentFrames);
                frames_decoded_.fetch_add(segmentDecodedFrames);
                if (!params.copy_only) frames_composited_.fetch_add(segmentFrames);
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

            if (!params.copy_only) encode_passes_.fetch_add(1);
            const int64_t segBytes = fileSize(segmentOut);
            temp_bytes_written_.fetch_add(segBytes);
            segmentPaths.push_back(segmentOut);
            seg.output_bytes = segBytes;
            seg.total_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - segStart).count();
            seg.finished_offset_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - renderStart).count();
            metrics_.addSegment(seg);

            const int pct = 10 + static_cast<int>(
                (static_cast<double>(i + 1) / plan.timeline.size()) * 60);
            reportProgress(pct, "building_segments");
            reportDetailedProgress(last_progress_, static_cast<int>(i + 1),
                                   static_cast<int>(plan.timeline.size()), static_cast<int>(i + 1),
                                   static_cast<int>(plan.timeline.size()), "building_segments",
                                   frames_encoded_.load(), frames_decoded_.load(),
                                   frames_composited_.load(),
                                   std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now() - renderStart).count(), true);
        }
    }

    if (total_duration_seconds > 0.0) {
        duration_seconds_.store(total_duration_seconds);
    }
    return true;
}

} // namespace velox::core
