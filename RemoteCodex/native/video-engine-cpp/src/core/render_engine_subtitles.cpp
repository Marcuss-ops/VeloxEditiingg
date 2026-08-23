#include "velox/core/render_engine.hpp"
#include "render_engine_helpers.hpp"
#include "velox/services/file_utils.hpp"

#include <chrono>
#include <filesystem>
#include <functional>
#include <string>

namespace fs = std::filesystem;

namespace velox::core {

namespace {
    using render_detail::burnSubtitleTrack;
    using render_detail::fileSize;
    using render_detail::reportDetailedProgress;
    using render_detail::reportProgress;
}

bool RenderEngine::burnSubtitles(
    const plan::RenderPlan& plan,
    const std::filesystem::path& workDir,
    const std::filesystem::path& outPath,
    const std::filesystem::path& videoOnly,
    const std::chrono::steady_clock::time_point& renderStart,
    RenderResult& result,
    const std::function<RenderResult(const std::string&)>& failRender,
    const std::function<void(const std::filesystem::path&)>& trackPartial,
    std::filesystem::path& videoForMux) {
    videoForMux = videoOnly;
    if (plan.subtitle_tracks.empty()) {
        return true;
    }
    telemetry::ScopedPhase subtitlePhase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeSubtitleTrack,
        "subtitle", "burn_in", "composite");
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
            failRender("subtitle_download_failed");
            return false;
        }
    }
    fs::path subtitledVideo = file::makePartialPath(outPath);
    trackPartial(subtitledVideo);
    if (!burnSubtitleTrack(videoOnly, localSubtitle, subtitledVideo)) {
        subtitlePhase.Abort("subtitle_burn_failed", "failed to burn subtitle track");
        result.error = "failed to burn subtitle track";
        failRender("subtitle_burn_failed");
        return false;
    }
    temp_bytes_written_.fetch_add(fileSize(subtitledVideo));
    videoForMux = subtitledVideo;
    return true;
}

} // namespace velox::core
