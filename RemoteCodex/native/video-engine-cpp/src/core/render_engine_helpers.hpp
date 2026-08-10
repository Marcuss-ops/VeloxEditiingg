#pragma once

#include "velox/plan/render_plan.hpp"
#include "velox/services/ffmpeg_progress_parser.hpp"
#include "velox/services/media_utils.hpp"

#include <cstdint>
#include <filesystem>
#include <string>

namespace velox::core::render_detail {

namespace fs = std::filesystem;

void reportProgress(int percent, const std::string& stage);
void reportDetailedProgress(const services::EngineProgress& progress,
                            int scene, int total_scenes,
                            int segment, int total_segments,
                            const std::string& phase,
                            int64_t frames_encoded,
                            int64_t frames_decoded,
                            int64_t frames_composited,
                            int64_t elapsed_ms);

media::SceneSegmentParams makeParams(
    const plan::CanvasSpec& canvas,
    const plan::TransformSpec& transform,
    const std::string& color_hex = "");

std::string extractColorHex(const plan::MediaSource& source);
int64_t fileSize(const fs::path& path);
int64_t decodedFramesFromShowInfo(const std::string& stderr_out);

bool runFfmpegSegmentWithProgress(
    const std::string& full_cmd,
    const services::ProgressCallback& callback,
    int64_t expected_duration_us,
    int64_t& decoded_frames);

std::string composeSegmentCmd(const std::string& args_only);

bool burnSubtitleTrack(
    const fs::path& input_video,
    const fs::path& subtitle_file,
    const fs::path& output_video);

} // namespace velox::core::render_detail
