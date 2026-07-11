#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_utils.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace velox::core {

namespace {
    void reportProgress(int percent, const std::string& stage) {
        std::cerr << "{\"progress\":" << percent
                  << ",\"percent\":" << percent
                  << ",\"stage\":\"" << stage << "\"}" << std::endl;
    }

    media::SceneSegmentParams makeParams(const plan::CanvasSpec& canvas, const plan::TransformSpec& transform, const std::string& color_hex = "") {
        media::SceneSegmentParams p;
        p.width = canvas.width;
        p.height = canvas.height;
        p.fps = canvas.fps;
        p.slow_zoom = transform.slow_zoom;
        p.scale_mode = transform.scale_mode;
        p.color_hex = color_hex;
        return p;
    }

    std::string extractColorHex(const plan::MediaSource& source) {
        if (std::holds_alternative<plan::ColorSource>(source)) {
            return std::get<plan::ColorSource>(source).color_hex;
        }
        return "";
    }

    int64_t fileSize(const fs::path& p) {
        std::error_code ec;
        auto sz = fs::file_size(p, ec);
        return ec ? 0 : static_cast<int64_t>(sz);
    }

    bool runFfmpegSegmentWithProgress(
        const std::string& full_cmd,
        const services::ProgressCallback& cb,
        int64_t expected_duration_us
    ) {
        if (!cb) {
            return file::runCommand(full_cmd);
        }
        std::string stderr_out;
        int exit_code = 0;
        bool ok = services::runFfmpegCapturingProgress(
            full_cmd,
            fs::current_path(),
            cb,
            expected_duration_us,
            stderr_out,
            exit_code);
        if (!ok || exit_code != 0) {
            std::cerr << "ffmpeg failed (exit=" << exit_code << "): "
                      << stderr_out << std::endl;
        }
        return ok && exit_code == 0;
    }

    std::string composeSegmentCmd(const std::string& args_only) {
        return "ffmpeg -y -hide_banner -loglevel error -progress pipe:1 -nostats " + args_only;
    }
}

void RenderEngine::setProgressCallback(services::ProgressCallback cb) {
    progress_cb_ = std::move(cb);
}

RenderResult RenderEngine::render(const plan::RenderPlan& plan) {
    // Reset accumulators on every fresh render() call.
    frames_encoded_.store(0);
    encode_passes_.store(0);
    temp_bytes_written_.store(0);
    duration_seconds_.store(0.0);
    concat_mode_ = "reencode";
    last_progress_ = services::EngineProgress{};
    metrics_.reset();

    const auto onProgress = progress_cb_;
    auto recordProgress = [this](const services::EngineProgress& p) {
        last_progress_ = p;
        if (p.frame > 0) {
            int64_t cur = frames_encoded_.load();
            if (p.frame > cur) frames_encoded_.store(p.frame);
        }
    };
    services::ProgressCallback wrapped_cb;
    if (onProgress) {
        wrapped_cb = [onProgress, recordProgress](const services::EngineProgress& p) {
            recordProgress(p);
            onProgress(p);
        };
    }

    RenderResult result;
    result.output_path = plan.output_path;

    reportProgress(0, "starting");

    fs::path workBase = fs::temp_directory_path() / "velox_video_engine_plan";
    fs::path workDir;
    {
        ScopedTimer t(metrics_, "workdir_create_ms");
        workDir = file::makeTempDir(workBase, "plan_job_");
        if (workDir.empty()) {
            result.error = "failed to create temp work dir";
            return result;
        }
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

    std::vector<fs::path> segmentPaths;
    segmentPaths.reserve(plan.timeline.size());

    double total_duration_seconds = 0.0;

    for (size_t i = 0; i < plan.timeline.size(); ++i) {
        const auto& item = plan.timeline[i];
        fs::path segmentOut = workDir / ("segment_" + std::to_string(i) + ".mp4");
        auto params = makeParams(plan.canvas, item.transform, extractColorHex(item.source));

        const int64_t expected_us = static_cast<int64_t>(item.duration_seconds * 1'000'000.0);
        total_duration_seconds += item.duration_seconds;

        // Per-segment timing record
        SegmentTiming seg;
        seg.index = i;
        seg.worker_index = 0;
        auto segStart = std::chrono::steady_clock::now();

        std::string args_only;
        if (std::holds_alternative<plan::ImageSource>(item.source)) {
            seg.source_type = "image";
            auto src = std::get<plan::ImageSource>(item.source);
            fs::path localImg = workDir / ("image_" + std::to_string(i) + ".jpg");
            bool gotImage;
            auto dlStart = std::chrono::steady_clock::now();
            {
                ScopedTimer t(metrics_, "asset_download_ms");
                gotImage = file::downloadAsset(src.url, localImg, src.cache_key);
            }
            seg.asset_download_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - dlStart).count();
            if (gotImage) {
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
            auto dlStart = std::chrono::steady_clock::now();
            {
                ScopedTimer t(metrics_, "asset_download_ms");
                gotVid = file::downloadAsset(src.url, localVid, src.cache_key);
            }
            seg.asset_download_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - dlStart).count();
            if (!gotVid) {
                result.error = "failed to download video source for segment " + std::to_string(i);
                return result;
            }
            args_only = media::buildVideoSegmentArgs(localVid, segmentOut, item.duration_seconds, params);
        } else if (std::holds_alternative<plan::ColorSource>(item.source)) {
            seg.source_type = "color";
            auto color = std::get<plan::ColorSource>(item.source);
            args_only = media::buildColorSegmentArgs(segmentOut, item.duration_seconds, params, color.color_hex);
        }

        if (args_only.empty()) {
            result.error = "unknown segment source type for " + std::to_string(i);
            return result;
        }

        {
            auto encStart = std::chrono::steady_clock::now();
            ScopedTimer t(metrics_, "segment_build_ms");
            bool built = runFfmpegSegmentWithProgress(
                composeSegmentCmd(args_only), wrapped_cb, expected_us);
            if (!built) {
                result.error = "failed to build timeline segment " + std::to_string(i);
                return result;
            }
            seg.ffmpeg_encode_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - encStart).count();
        }

        encode_passes_.fetch_add(1);
        const int64_t segBytes = fileSize(segmentOut);
        temp_bytes_written_.fetch_add(segBytes);
        segmentPaths.push_back(segmentOut);

        seg.output_bytes = segBytes;
        seg.total_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - segStart).count();
        metrics_.addSegment(seg);

        int pct = 10 + static_cast<int>((static_cast<double>(i + 1) / plan.timeline.size()) * 60);
        reportProgress(pct, "building_segments");
    }

    if (total_duration_seconds > 0.0) {
        duration_seconds_.store(total_duration_seconds);
    }

    // 2. Concatenate video segments
    reportProgress(75, "concatenating");
    fs::path videoOnly = workDir / "video_only.mp4";
    {
        ScopedTimer t(metrics_, "concat_ms");
        if (!media::concatSegments(segmentPaths, videoOnly, workDir)) {
            result.error = "failed to concatenate video segments";
            return result;
        }
    }
    temp_bytes_written_.fetch_add(fileSize(videoOnly));
    concat_mode_ = "stream_copy";

    // 3. Mix audio tracks (supports multi-track with volume/offset)
    reportProgress(85, "muxing_audio");
    if (!plan.audio_tracks.empty()) {
        std::vector<std::pair<fs::path, const plan::AudioTrack*>> downloadedTracks;
        {
            ScopedTimer t(metrics_, "audio_download_ms");
            for (size_t t = 0; t < plan.audio_tracks.size(); ++t) {
                const auto& track = plan.audio_tracks[t];
                fs::path localAudio = workDir / ("audio_track_" + std::to_string(t) + ".m4a");
                if (file::downloadAsset(track.source_url, localAudio)) {
                    downloadedTracks.emplace_back(localAudio, &track);
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
                fs::copy_file(videoOnly, outPath, fs::copy_options::overwrite_existing, ec);
            }
            if (ec) {
                result.error = "failed to copy final output (no audio)";
                return result;
            }
            result.success = true;
        } else if (downloadedTracks.size() == 1) {
            fs::path finalMuxed = workDir / "final_muxed.mp4";
            double vol = downloadedTracks[0].second->volume;
            double offset = downloadedTracks[0].second->start_time_offset;
            bool muxOk;
            {
                ScopedTimer t(metrics_, "mux_audio_ms");
                muxOk = media::muxAudio(videoOnly, downloadedTracks[0].first, finalMuxed, vol, offset);
            }
            if (muxOk) {
                std::error_code ec;
                {
                    ScopedTimer tCopy(metrics_, "copy_final_ms");
                    fs::copy_file(finalMuxed, outPath, fs::copy_options::overwrite_existing, ec);
                }
                if (ec) {
                    result.error = "failed to copy final output";
                    return result;
                }
                temp_bytes_written_.fetch_add(fileSize(finalMuxed));
                result.success = true;
            } else {
                result.error = "failed to mux audio track";
                return result;
            }
        } else {
            std::ostringstream audioFilter;
            std::ostringstream audioInputs;
            for (size_t t = 0; t < downloadedTracks.size(); ++t) {
                audioInputs << " -i " << file::shellQuote(downloadedTracks[t].first.string());
                if (t > 0) audioFilter << ";";
                double vol = downloadedTracks[t].second->volume;
                double offset = downloadedTracks[t].second->start_time_offset;
                audioFilter << "[" << t << ":a]volume=" << vol;
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
                   << " -map \"[aout]\" -c:a aac "
                   << file::shellQuote(mixedAudio.string());

            if (file::runCommand(mixCmd.str())) {
                fs::path finalMuxed = workDir / "final_muxed.mp4";
                bool muxOk;
                {
                    ScopedTimer t(metrics_, "mux_audio_ms");
                    muxOk = media::muxAudio(videoOnly, mixedAudio, finalMuxed);
                }
                if (muxOk) {
                    std::error_code ec;
                    {
                        ScopedTimer tCopy(metrics_, "copy_final_ms");
                        fs::copy_file(finalMuxed, outPath, fs::copy_options::overwrite_existing, ec);
                    }
                    if (ec) {
                        result.error = "failed to copy final output";
                        return result;
                    }
                    temp_bytes_written_.fetch_add(fileSize(finalMuxed));
                    result.success = true;
                } else {
                    result.error = "failed to mux mixed audio";
                    return result;
                }
            } else {
                std::cerr << "warning: audio mix failed, exporting video without audio\n";
                std::error_code ec;
                {
                    ScopedTimer t(metrics_, "copy_final_ms");
                    fs::copy_file(videoOnly, outPath, fs::copy_options::overwrite_existing, ec);
                }
                if (ec) {
                    result.error = "failed to copy final output (mix failed)";
                    return result;
                }
                result.success = true;
            }
        }
    } else {
        std::error_code ec;
        {
            ScopedTimer t(metrics_, "copy_final_ms");
            fs::copy_file(videoOnly, outPath, fs::copy_options::overwrite_existing, ec);
        }
        if (ec) {
            result.error = "failed to copy final output (no audio tracks)";
            return result;
        }
        result.success = true;
    }

    reportProgress(100, "completed");

    if (result.success) {
        emitSidecar(outPath.string());
    }

    return result;
}

void RenderEngine::emitSidecar(const std::string& output_path) const {
    using services::escapeProgressJsonString;

    fs::path outPath(output_path);
    fs::path sidecar = outPath;
    sidecar += ".progress.json";

    const services::EngineProgress& last = last_progress_;

    std::ostringstream s;
    s << "{";
    s << "\"progress\":" << static_cast<int>(last.progress_pct);
    s << ",\"progress_pct\":" << static_cast<int>(last.progress_pct);
    s << ",\"frames\":" << frames_encoded_.load();
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
            s << ",\"source_type\":\"" << seg.source_type << "\"";
            s << ",\"total_ms\":" << seg.total_ms;
            s << ",\"asset_download_ms\":" << seg.asset_download_ms;
            s << ",\"ffmpeg_encode_ms\":" << seg.ffmpeg_encode_ms;
            s << ",\"output_bytes\":" << seg.output_bytes;
            s << "}";
        }
    }
    s << "]";

    s << "}";

    if (!services::SidecarWriter::writeAtomic(sidecar, s.str())) {
        std::cerr << "warning: failed to write progress sidecar at " << sidecar << "\n";
    }
}

} // namespace velox::core
