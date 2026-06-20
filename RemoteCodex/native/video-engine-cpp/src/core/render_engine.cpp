#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/media_utils.hpp"
#include <filesystem>
#include <iostream>
#include <sstream>
#include <thread>
#include <atomic>
#include <algorithm>

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
}

RenderResult RenderEngine::render(const plan::RenderPlan& plan) {
    RenderResult result;
    result.output_path = plan.output_path;

    reportProgress(0, "starting");

    fs::path workBase = fs::temp_directory_path() / "velox_video_engine_plan";
    fs::path workDir = file::makeTempDir(workBase, "plan_job_");
    if (workDir.empty()) {
        result.error = "failed to create temp work dir";
        return result;
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
    fs::create_directories(outPath.parent_path());

    // 1. Build timeline segments
    reportProgress(10, "resolving_assets");

    std::vector<fs::path> segmentPaths;
    segmentPaths.reserve(plan.timeline.size());

    for (size_t i = 0; i < plan.timeline.size(); ++i) {
        const auto& item = plan.timeline[i];
        fs::path segmentOut = workDir / ("segment_" + std::to_string(i) + ".mp4");
        auto params = makeParams(plan.canvas, item.transform, extractColorHex(item.source));

        bool built = false;
        if (std::holds_alternative<plan::ImageSource>(item.source)) {
            auto src = std::get<plan::ImageSource>(item.source);
            fs::path localImg = workDir / ("image_" + std::to_string(i) + ".jpg");
            if (file::downloadAsset(src.url, localImg, src.cache_key)) {
                built = media::buildSceneSegment(localImg, segmentOut, item.duration_seconds, params);
            } else {
                built = media::buildSceneSegment("", segmentOut, item.duration_seconds, params);
            }
        } else if (std::holds_alternative<plan::VideoSource>(item.source)) {
            auto src = std::get<plan::VideoSource>(item.source);
            fs::path localVid = workDir / ("video_" + std::to_string(i) + ".mp4");
            if (file::downloadAsset(src.url, localVid, src.cache_key)) {
                built = media::buildVideoSegment(localVid, segmentOut, item.duration_seconds, params);
            }
        } else if (std::holds_alternative<plan::ColorSource>(item.source)) {
            built = media::buildSceneSegment("", segmentOut, item.duration_seconds, params);
        }

        if (!built) {
            result.error = "failed to build timeline segment " + std::to_string(i);
            return result;
        }
        segmentPaths.push_back(segmentOut);

        int pct = 10 + static_cast<int>((static_cast<double>(i + 1) / plan.timeline.size()) * 60);
        reportProgress(pct, "building_segments");
    }

    // 2. Concatenate video segments
    reportProgress(75, "concatenating");
    fs::path videoOnly = workDir / "video_only.mp4";
    if (!media::concatSegments(segmentPaths, videoOnly, workDir)) {
        result.error = "failed to concatenate video segments";
        return result;
    }

    // 3. Mix audio tracks (supports multi-track with volume/offset)
    reportProgress(85, "muxing_audio");
    if (!plan.audio_tracks.empty()) {
        // Download all audio tracks first
        std::vector<std::pair<fs::path, const plan::AudioTrack*>> downloadedTracks;
        for (size_t t = 0; t < plan.audio_tracks.size(); ++t) {
            const auto& track = plan.audio_tracks[t];
            fs::path localAudio = workDir / ("audio_track_" + std::to_string(t) + ".m4a");
            if (file::downloadAsset(track.source_url, localAudio)) {
                downloadedTracks.emplace_back(localAudio, &track);
            } else {
                std::cerr << "warning: failed to download audio track " << t << "\n";
            }
        }

        if (downloadedTracks.empty()) {
            std::cerr << "warning: no audio tracks downloaded, exporting video without audio\n";
            if (!file::copyFile(videoOnly, outPath)) {
                result.error = "failed to copy final output (no audio)";
                return result;
            }
            result.success = true;
        } else if (downloadedTracks.size() == 1) {
            // Single track: apply volume + offset, mux directly
            fs::path finalMuxed = workDir / "final_muxed.mp4";
            double vol = downloadedTracks[0].second->volume;
            double offset = downloadedTracks[0].second->start_time_offset;
            if (media::muxAudio(videoOnly, downloadedTracks[0].first, finalMuxed, vol, offset)) {
                if (!file::copyFile(finalMuxed, outPath)) {
                    result.error = "failed to copy final output";
                    return result;
                }
                result.success = true;
            } else {
                result.error = "failed to mux audio track";
                return result;
            }
        } else {
            // Multiple tracks: use ffmpeg amix to merge them into a single .m4a
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
                if (media::muxAudio(videoOnly, mixedAudio, finalMuxed)) {
                    if (!file::copyFile(finalMuxed, outPath)) {
                        result.error = "failed to copy final output";
                        return result;
                    }
                    result.success = true;
                } else {
                    result.error = "failed to mux mixed audio";
                    return result;
                }
            } else {
                std::cerr << "warning: audio mix failed, exporting video without audio\n";
                if (!file::copyFile(videoOnly, outPath)) {
                    result.error = "failed to copy final output (mix failed)";
                    return result;
                }
                result.success = true;
            }
        }
    } else {
        if (!file::copyFile(videoOnly, outPath)) {
            result.error = "failed to copy final output (no audio tracks)";
            return result;
        }
        result.success = true;
    }

    reportProgress(100, "completed");
    return result;
}

} // namespace velox::core
