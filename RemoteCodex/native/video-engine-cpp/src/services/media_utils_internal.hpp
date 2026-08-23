#pragma once

#include "velox/services/media_utils.hpp"

#include <filesystem>
#include <sstream>
#include <string>

namespace velox::media::detail {

std::string ffmpegVideoCodec();
std::string ffmpegVideoPresetForCodec(const std::string& codec);
std::string ffmpegVideoTuneForCodec(const std::string& codec, bool still_image);
std::string ffmpegVideoExtraArgs();
bool ffmpegDecodeTelemetryEnabled();
std::string withDecodeTelemetry(const std::string& filter);
int ffmpegThreadCount();
std::string ffmpegRateControlArgsForCodec(const std::string& codec);
void appendFfmpegVideoEncodingArgs(
    std::ostringstream& command,
    const std::string& codec,
    const std::string& preset,
    const std::string& tune,
    int threads,
    const std::string& extra_args);
void canvasDims(const SceneSegmentParams& params, int& width, int& height, int& fps);
std::string scaleFilterString(
    const std::string& scale_mode,
    const std::string& size,
    const std::string& resolution);

bool nativeVideoStreamCopyCompatible(
    const std::filesystem::path& clip_path,
    int width,
    int height,
    int fps,
    double requested_duration);

} // namespace velox::media::detail
