#include "velox/services/media_utils.hpp"
#include "velox/services/file_utils.hpp"
#include "media_utils_internal.hpp"

#include <sstream>

namespace fs = std::filesystem;

namespace velox::media {

std::string buildColorSegmentArgs(
    const fs::path& segment_path,
    double duration,
    const SceneSegmentParams& params,
    const std::string& color_hex) {
    int width, height, fps;
    detail::canvasDims(params, width, height, fps);
    const std::string resolution = std::to_string(width) + "x" + std::to_string(height);
    std::string background = "black";
    if (!color_hex.empty()) {
        std::string hex = color_hex;
        if (!hex.empty() && hex[0] == '#') hex = hex.substr(1);
        background = "0x" + hex;
    }
    const std::string codec = detail::ffmpegVideoCodec();
    std::ostringstream command;
    command << "-f lavfi -t " << duration
            << " -i " << file::shellQuote("color=c=" + background + ":s=" + resolution)
            << (detail::ffmpegDecodeTelemetryEnabled() ? " -vf showinfo" : "")
            << detail::ffmpegRateControlArgsForCodec(codec)
            << " -pix_fmt yuv420p -r " << fps;
    detail::appendFfmpegVideoEncodingArgs(
        command, codec, detail::ffmpegVideoPresetForCodec(codec),
        detail::ffmpegVideoTuneForCodec(codec, true), detail::ffmpegThreadCount(), {});
    command << " " << file::shellQuote(segment_path.string());
    return command.str();
}

std::string buildSceneSegmentArgs(
    const fs::path& image_path,
    const fs::path& segment_path,
    double duration,
    const SceneSegmentParams& params) {
    int width, height, fps;
    detail::canvasDims(params, width, height, fps);
    const std::string resolution = std::to_string(width) + "x" + std::to_string(height);
    const std::string size = std::to_string(width) + ":" + std::to_string(height);
    const int frames = detail::frameCountForDuration(duration, fps);
    std::string filter = detail::scaleFilterString(params.scale_mode, size, resolution);
    if (params.slow_zoom) {
        filter += ",zoompan=z='1+0.08*on/(" + std::to_string(frames) + ")'"
                  ":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
                  ":d=" + std::to_string(frames) + ":s=" + resolution +
                  ":fps=" + std::to_string(fps) + ",format=yuv420p";
    }
    filter = detail::withDecodeTelemetry(filter);
    const std::string codec = detail::ffmpegVideoCodec();
    std::ostringstream command;
    command << "-stream_loop -1 -i " << file::shellQuote(image_path.string())
            << " -vf " << file::shellQuote(filter)
            << " -frames:v " << frames
            << detail::ffmpegRateControlArgsForCodec(codec)
            << " -pix_fmt yuv420p -r " << fps;
    detail::appendFfmpegVideoEncodingArgs(
        command, codec, detail::ffmpegVideoPresetForCodec(codec),
        detail::ffmpegVideoTuneForCodec(codec, true), detail::ffmpegThreadCount(), {});
    command << " " << file::shellQuote(segment_path.string());
    return command.str();
}

std::string buildVideoSegmentArgs(
    const fs::path& clip_path,
    const fs::path& segment_path,
    double duration,
    const SceneSegmentParams& params,
    bool include_audio) {
    int width, height, fps;
    detail::canvasDims(params, width, height, fps);
    if (params.copy_only && !detail::nativeVideoStreamCopyCompatible(
            clip_path, width, height, fps, duration)) {
        return {};
    }
    if (params.copy_only) {
        std::ostringstream command;
        command << "-i " << file::shellQuote(clip_path.string())
                << " -t " << duration << " -map 0:v:0";
        if (include_audio) command << " -map 0:a:0? -c:a copy";
        else command << " -an";
        command << " -c:v copy -avoid_negative_ts make_zero -reset_timestamps 1 "
                << file::shellQuote(segment_path.string());
        return command.str();
    }

    const std::string size = std::to_string(width) + ":" + std::to_string(height);
    std::string scale_filter =
        "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" +
        size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
    if (params.scale_mode == "cover") {
        scale_filter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" +
            size + ",format=yuv420p";
    } else if (params.scale_mode == "stretch") {
        scale_filter = "scale=" + size + ",format=yuv420p";
    }
    scale_filter = detail::withDecodeTelemetry(scale_filter);
    const std::string codec = detail::ffmpegVideoCodec();
    std::ostringstream command;
    if (!include_audio) command << "-stream_loop -1 ";
    command << "-i " << file::shellQuote(clip_path.string())
            << " -t " << duration
            << " -vf " << file::shellQuote(scale_filter)
            << detail::ffmpegRateControlArgsForCodec(codec)
            << " -pix_fmt yuv420p -r " << fps;
    detail::appendFfmpegVideoEncodingArgs(
        command, codec, detail::ffmpegVideoPresetForCodec(codec),
        detail::ffmpegVideoTuneForCodec(codec, false), detail::ffmpegThreadCount(), {});
    if (include_audio) command << " -map 0:v:0 -map 0:a? -c:a aac -ar 48000 -ac 2";
    else command << " -an";
    command << " " << file::shellQuote(segment_path.string());
    return command.str();
}

} // namespace velox::media
