#include "velox/core/render_engine.hpp"
#include "render_engine_helpers.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "velox/services/media_utils.hpp"

#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>

namespace fs = std::filesystem;

namespace velox::core {

namespace {

fs::path numberedWorkPath(const fs::path& work_dir, const char* prefix,
                          const char* suffix, std::size_t index) {
    char name[64];
    std::snprintf(name, sizeof(name), "%s%zu%s", prefix, index, suffix);
    return work_dir / name;
}

using render_detail::fileSize;

} // namespace

void RenderEngine::setProgressCallback(services::ProgressCallback cb) {
    progress_cb_ = std::move(cb);
}

RenderEngine::SidecarGuard::~SidecarGuard() {
    if (engine_ != nullptr && !output_path_.empty()) {
        engine_->emitSidecar(output_path_);
    }
}

void RenderEngine::resetRenderState() {
    frames_encoded_.store(0);
    frames_decoded_.store(0);
    frames_composited_.store(0);
    encode_passes_.store(0);
    temp_bytes_written_.store(0);
    copy_segments_.store(0);
    transcode_segments_.store(0);
    duration_seconds_.store(0.0);
    output_durable_.store(false);
    concat_mode_ = "reencode";
    last_progress_ = services::EngineProgress{};
    metrics_.reset();
    recorder_.Reset();
    frame_pipeline_metrics_ = media::FramePipelineMetrics{};
    frame_pipeline_runs_ = 0;
    services::resetIOCounters();
}

bool RenderEngine::createRenderWorkDir(const std::filesystem::path& base,
                                       std::filesystem::path& work_dir,
                                       std::string& error) {
    telemetry::ScopedPhase temp_phase(
        recorder_, telemetry::kOriginWorker, telemetry::kScopeAttempt,
        "worker.temp", "create", "render");
    ScopedTimer timer(metrics_, "workdir_create_ms");
    work_dir = file::makeTempDir(base, "plan_job_");
    if (work_dir.empty()) {
        temp_phase.Abort("tempdir_create_failed", "failed to create temp work dir");
        error = "failed to create temp work dir";
        return false;
    }
    temp_phase.Complete();
    return true;
}

std::string RenderEngine::publishRenderOutput(
    const std::filesystem::path& partial,
    const std::filesystem::path& output) {
    std::string error;
    bool durable = false;
    {
        ScopedTimer timer(metrics_, "publish_atomic_ms");
        ScopedTimer legacy_timer(metrics_, "copy_final_ms");
        if (!file::publishAtomic(partial, output, &error, &durable)) {
            return error;
        }
    }
    output_durable_.store(durable);
    if (!durable) {
        std::cerr << "warning: output was atomically published but directory durability was not confirmed\n";
    }
    return {};
}

bool RenderEngine::concatenateTimelineSegments(
    const std::vector<std::filesystem::path>& segment_paths,
    const std::filesystem::path& video_only,
    const std::filesystem::path& work_dir,
    RenderResult& result,
    const std::function<RenderResult(const std::string&)>& fail_render) {
    telemetry::ScopedPhase concat_phase(
        recorder_, telemetry::kOriginEngine, telemetry::kScopeAttempt,
        "engine", "concat", "finalize");
    ScopedTimer timer(metrics_, "concat_ms");
    if (!media::concatSegments(segment_paths, video_only, work_dir)) {
        concat_phase.Abort("concat_failed", "failed to concatenate video segments");
        result.error = "failed to concatenate video segments";
        fail_render("concat_failed");
        return false;
    }
    concat_phase.Complete();
    temp_bytes_written_.fetch_add(fileSize(video_only));
    concat_mode_ = "stream_copy";
    return true;
}

std::vector<std::pair<std::filesystem::path, const plan::AudioTrack*>>
RenderEngine::downloadAudioTracks(
    const plan::RenderPlan& plan,
    const std::filesystem::path& work_dir) {
    std::vector<std::pair<std::filesystem::path, const plan::AudioTrack*>> tracks;
    tracks.reserve(plan.audio_tracks.size());
    ScopedTimer timer(metrics_, "audio_download_ms");
    for (std::size_t index = 0; index < plan.audio_tracks.size(); ++index) {
        const auto& track = plan.audio_tracks[index];
        const fs::path local_audio = numberedWorkPath(
            work_dir, "audio_track_", ".m4a", index);
        if (file::downloadAsset(track.source_url, local_audio)) {
            if (media::hasAudioStream(local_audio)) {
                tracks.emplace_back(local_audio, &track);
            } else {
                std::cerr << "warning: audio track " << index
                          << " contains no audio stream, skipping\n";
            }
        } else {
            std::cerr << "warning: failed to download audio track " << index << "\n";
        }
    }
    return tracks;
}

} // namespace velox::core
