#include "media_utils_internal.hpp"
#include "json_utils.hpp"

#include <cstdlib>

namespace velox::media::detail {
namespace {

std::string envString(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) return fallback;
    const std::string trimmed = json::trim(value);
    return trimmed.empty() ? fallback : trimmed;
}

int envInt(const char* name, int fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) return fallback;
    try {
        const int parsed = std::stoi(json::trim(value));
        return parsed > 0 ? parsed : fallback;
    } catch (...) {
        return fallback;
    }
}

bool codecContains(const std::string& codec, const std::string& needle) {
    return codec.find(needle) != std::string::npos;
}

} // namespace

std::string ffmpegVideoCodec() {
    return envString("VELOX_FFMPEG_VCODEC", "libx264");
}

std::string ffmpegVideoPresetForCodec(const std::string& codec) {
    const std::string override_value = envString("VELOX_FFMPEG_PRESET", "");
    if (!override_value.empty()) return override_value;
    if (codecContains(codec, "x264") || codecContains(codec, "x265")) return "veryfast";
    return {};
}

std::string ffmpegVideoTuneForCodec(const std::string& codec, bool still_image) {
    const std::string override_value = envString("VELOX_FFMPEG_TUNE", "");
    if (!override_value.empty()) return override_value;
    if (still_image && (codecContains(codec, "x264") || codecContains(codec, "x265"))) {
        return "stillimage";
    }
    return {};
}

std::string ffmpegVideoExtraArgs() {
    return envString("VELOX_FFMPEG_VENC_ARGS", "");
}

bool ffmpegDecodeTelemetryEnabled() {
    const char* value = std::getenv("VELOX_FFMPEG_DECODE_TELEMETRY");
    return value != nullptr && std::string(value) != "0" && std::string(value) != "false";
}

std::string withDecodeTelemetry(const std::string& filter) {
    return ffmpegDecodeTelemetryEnabled() ? "showinfo," + filter : filter;
}

int ffmpegThreadCount() {
    return envInt("VELOX_FFMPEG_THREADS", 0);
}

std::string ffmpegRateControlArgsForCodec(const std::string& codec) {
    if (codecContains(codec, "nvenc")) return " -rc constqp -qp 23";
    if (codecContains(codec, "vaapi")) return " -qp 23";
    if (codecContains(codec, "qsv")) return " -global_quality 23";
    return " -crf 20";
}

void appendFfmpegVideoEncodingArgs(
    std::ostringstream& command,
    const std::string& codec,
    const std::string& preset,
    const std::string& tune,
    int threads,
    const std::string& extra_args) {
    const std::string selected_codec = codec.empty() ? ffmpegVideoCodec() : codec;
    command << " -c:v " << selected_codec;
    if (!preset.empty()) command << " -preset " << preset;
    if (!tune.empty()) command << " -tune " << tune;
    if (threads > 0) command << " -threads " << threads;
    const std::string selected_extra = extra_args.empty() ? ffmpegVideoExtraArgs() : extra_args;
    if (!selected_extra.empty()) command << " " << selected_extra;
}

void canvasDims(const SceneSegmentParams& params, int& width, int& height, int& fps) {
    width = params.width > 0 ? params.width : 1920;
    height = params.height > 0 ? params.height : 1080;
    fps = params.fps > 0 ? params.fps : 24;
}

std::string scaleFilterString(
    const std::string& scale_mode,
    const std::string& size,
    const std::string& resolution) {
    std::string filter;
    if (scale_mode == "contain") {
        filter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" +
            size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
    } else if (scale_mode == "stretch") {
        filter = "scale=" + size + ",format=yuv420p";
    } else {
        filter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" +
            size + ",format=yuv420p";
    }
    (void)resolution;
    return withDecodeTelemetry(filter);
}

} // namespace velox::media::detail
