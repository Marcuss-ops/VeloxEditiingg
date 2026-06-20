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
        std::cerr << "{\"progress\":" << percent << ",\"stage\":\"" << stage << "\"}" << std::endl;
    }
}

RenderResult RenderEngine::render(const plan::RenderPlan& plan) {
    RenderResult result;
    result.output_path = plan.output_path;

    reportProgress(0, "starting");

    // Creazione workspace directory temporanea
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

    // Assicurarsi che la directory finale esista
    fs::path outPath(plan.output_path);
    fs::create_directories(outPath.parent_path());

    // 1. Download degli asset (Sia timeline che audio)
    reportProgress(10, "resolving_assets");
    
    // Per velocizzare, scarichiamo sequenzialmente gli asset per questa implementazione.
    // In produzione o refactoring successivi si può parallelizzare il download.
    std::vector<fs::path> segmentPaths;
    segmentPaths.reserve(plan.timeline.size());

    for (size_t i = 0; i < plan.timeline.size(); ++i) {
        const auto& item = plan.timeline[i];
        fs::path segmentOut = workDir / ("segment_" + std::to_string(i) + ".mp4");
        
        bool built = false;
        if (std::holds_alternative<plan::ImageSource>(item.source)) {
            auto src = std::get<plan::ImageSource>(item.source);
            fs::path localImg = workDir / ("image_" + std::to_string(i) + ".jpg");
            if (file::downloadAsset(src.url, localImg, src.cache_key)) {
                built = media::buildSceneSegment(localImg, segmentOut, item.duration_seconds);
            } else {
                // Fallback sul colore nero se l'immagine non si scarica
                built = media::buildSceneSegment("", segmentOut, item.duration_seconds);
            }
        } else if (std::holds_alternative<plan::VideoSource>(item.source)) {
            auto src = std::get<plan::VideoSource>(item.source);
            fs::path localVid = workDir / ("video_" + std::to_string(i) + ".mp4");
            if (file::downloadAsset(src.url, localVid, src.cache_key)) {
                built = media::buildVideoSegment(localVid, segmentOut, item.duration_seconds);
            }
        } else if (std::holds_alternative<plan::ColorSource>(item.source)) {
            // Genera segmento nero o da colore fisso (fallback nero in buildSceneSegment senza img)
            built = media::buildSceneSegment("", segmentOut, item.duration_seconds);
        }

        if (!built) {
            result.error = "failed to build timeline segment " + std::to_string(i);
            return result;
        }
        segmentPaths.push_back(segmentOut);
        
        int pct = 10 + static_cast<int>((static_cast<double>(i + 1) / plan.timeline.size()) * 60);
        reportProgress(pct, "building_segments");
    }

    // 2. Concatena i segmenti video
    reportProgress(75, "concatenating");
    fs::path videoOnly = workDir / "video_only.mp4";
    if (!media::concatSegments(segmentPaths, videoOnly, workDir)) {
        result.error = "failed to concatenate video segments";
        return result;
    }

    // 3. Scarica e muxa le tracce audio (Supporta al momento solo 1 traccia audio come da engine attuale)
    reportProgress(85, "muxing_audio");
    if (!plan.audio_tracks.empty()) {
        fs::path localAudio = workDir / "audio_track_0.mp3";
        const auto& track = plan.audio_tracks.front();
        if (file::downloadAsset(track.source_url, localAudio)) {
            fs::path finalMuxed = workDir / "final_muxed.mp4";
            if (media::muxAudio(videoOnly, localAudio, finalMuxed)) {
                file::copyFile(finalMuxed, outPath);
                result.success = true;
            } else {
                result.error = "failed to mux audio track";
                return result;
            }
        } else {
            std::cerr << "warning: failed to download audio track, exporting video without audio\n";
            file::copyFile(videoOnly, outPath);
            result.success = true;
        }
    } else {
        file::copyFile(videoOnly, outPath);
        result.success = true;
    }

    reportProgress(100, "completed");
    return result;
}

} // namespace velox::core
