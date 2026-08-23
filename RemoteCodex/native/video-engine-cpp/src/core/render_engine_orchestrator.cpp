#include "velox/core/render_engine.hpp"
#include "render_engine_helpers.hpp"
#include "velox/services/file_utils.hpp"

#include <chrono>
#include <filesystem>
#include <functional>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

namespace velox::core {

RenderResult RenderEngine::render(const plan::RenderPlan& plan) {
    resetRenderState();

    RenderResult result;
    result.output_path = plan.output_path;
    SidecarGuard sidecarGuard(this, result.output_path);
    telemetry::ScopedPhase renderPhase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAttempt,
        "engine", "render", "render");
    auto failRender = [&renderPhase, &result](const std::string& error_code) -> RenderResult {
        renderPhase.Abort(error_code, result.error);
        return result;
    };
    const auto renderStart = std::chrono::steady_clock::now();

    render_detail::reportProgress(0, "starting");
    render_detail::reportDetailedProgress(
        last_progress_, 0, static_cast<int>(plan.timeline.size()), 0,
        static_cast<int>(plan.timeline.size()), "starting", 0, 0, 0, 0);

    const fs::path workBase = fs::temp_directory_path() / "velox_video_engine_plan";
    fs::path workDir;
    std::string workDirError;
    if (!createRenderWorkDir(workBase, workDir, workDirError)) {
        result.error = workDirError;
        return failRender("tempdir_create_failed");
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
            if (!path.empty()) paths.push_back(path);
        }
    } partialOutputs;

    const fs::path outPath(plan.output_path);
    const auto publishOutput = [this, &outPath](const fs::path& partial) {
        return publishRenderOutput(partial, outPath);
    };
    std::error_code parentError;
    fs::create_directories(outPath.parent_path(), parentError);

#ifdef VELOX_ENABLE_LIBAV
    if (plan.copy_only) {
        return renderCopyOnly(plan, workDir, outPath, result, failRender, renderStart);
    }
    if (plan.mixed && !plan.copy_only) {
        return renderMixed(plan, workDir, outPath, result, failRender);
    }
#endif

    render_detail::reportProgress(10, "resolving_assets");
    std::vector<fs::path> segmentPaths;
    segmentPaths.reserve(plan.timeline.size());
    double totalDurationSeconds = 0.0;
    if (!renderLegacyTimeline(plan, workDir, renderStart, segmentPaths,
                              totalDurationSeconds, result, failRender)) {
        return result;
    }

    render_detail::reportProgress(75, "concatenating");
    render_detail::reportDetailedProgress(
        last_progress_, static_cast<int>(plan.timeline.size()),
        static_cast<int>(plan.timeline.size()), static_cast<int>(plan.timeline.size()),
        static_cast<int>(plan.timeline.size()), "concatenating",
        frames_encoded_.load(), frames_decoded_.load(), frames_composited_.load(),
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - renderStart).count());

    const fs::path videoOnly = file::makePartialPath(outPath);
    partialOutputs.track(videoOnly);
    if (!concatenateTimelineSegments(segmentPaths, videoOnly, workDir,
                                     result, failRender)) {
        return result;
    }

    fs::path videoForMux;
    const auto trackPartial = [&partialOutputs](const fs::path& path) {
        partialOutputs.track(path);
    };
    if (!burnSubtitles(plan, workDir, outPath, videoOnly, renderStart,
                       result, failRender, trackPartial, videoForMux)) {
        return result;
    }

    render_detail::reportProgress(85, "muxing_audio");
    render_detail::reportDetailedProgress(
        last_progress_, static_cast<int>(plan.timeline.size()),
        static_cast<int>(plan.timeline.size()), static_cast<int>(plan.timeline.size()),
        static_cast<int>(plan.timeline.size()), "muxing_audio",
        frames_encoded_.load(), frames_decoded_.load(), frames_composited_.load(),
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - renderStart).count());

    if (!finalizeAudioTracks(plan, workDir, outPath, videoForMux, renderStart,
                             publishOutput, trackPartial, result, failRender)) {
        return result;
    }

    render_detail::reportProgress(100, "completed");
    last_progress_.progress_pct = 100.0;
    last_progress_.finished = true;
    return result;
}

} // namespace velox::core
