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
void reportArtifactWriteProgress(const std::string& artifact,
                                 const std::filesystem::path& path,
                                 int64_t high_watermark_bytes,
                                 int64_t safe_offset_bytes,
                                 bool finalized);

void reportDetailedProgress(const services::EngineProgress& progress,
                            int scene, int total_scenes,
                            int segment, int total_segments,
                            const std::string& phase,
                            int64_t frames_encoded,
                            int64_t frames_decoded,
                            int64_t frames_composited,
                            int64_t elapsed_ms,
                            bool segment_completed = false);

media::SceneSegmentParams makeParams(
    const plan::CanvasSpec& canvas,
    const plan::TransformSpec& transform,
    const std::string& color_hex = "");

std::string extractColorHex(const plan::MediaSource& source);
int64_t fileSize(const fs::path& path);

// Canonical numbered work-dir path builder ("<prefix><index><suffix>").
// The render engine TUs previously carried two divergent private copies
// (snprintf vs std::to_string); both now route through this one.
fs::path numberedWorkPath(const fs::path& work_dir, const char* prefix,
                          const char* suffix, std::size_t index);

int64_t decodedFramesFromShowInfo(const std::string& stderr_out);

bool runFfmpegSegmentWithProgress(
    const std::string& full_cmd,
    const services::ProgressCallback& callback,
    int64_t expected_duration_us,
    int64_t& decoded_frames);

std::string composeSegmentCmd(const std::string& args_only);

} // namespace velox::core::render_detail
