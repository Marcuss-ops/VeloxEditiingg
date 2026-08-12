#include "velox/services/media_utils.hpp"
#include "velox/services/file_utils.hpp"
#include "json_utils.hpp"

#ifdef VELOX_ENABLE_LIBAV
#include "velox/services/media_probe.hpp"

extern "C" {
#include <libavcodec/codec_id.h>
#include <libavcodec/avcodec.h>
#include <libavutil/pixfmt.h>
}
#endif
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <sstream>
#include <iostream>

namespace fs = std::filesystem;

namespace velox::media {

static std::string envString(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) {
        return fallback;
    }
    std::string trimmed = json::trim(value);
    return trimmed.empty() ? fallback : trimmed;
}

static int envInt(const char* name, int fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) {
        return fallback;
    }
    try {
        const int parsed = std::stoi(json::trim(value));
        return parsed > 0 ? parsed : fallback;
    } catch (...) {
        return fallback;
    }
}

static std::string ffmpegVideoCodec() {
    return envString("VELOX_FFMPEG_VCODEC", "libx264");
}

static bool codecContains(const std::string& codec, const std::string& needle) {
    return codec.find(needle) != std::string::npos;
}

static std::string ffmpegVideoPresetForCodec(const std::string& codec) {
    const std::string overrideValue = envString("VELOX_FFMPEG_PRESET", "");
    if (!overrideValue.empty()) {
        return overrideValue;
    }
    if (codecContains(codec, "x264") || codecContains(codec, "x265")) {
        return "veryfast";
    }
    return "";
}

static std::string ffmpegVideoTuneForCodec(const std::string& codec, bool stillImage) {
    const std::string overrideValue = envString("VELOX_FFMPEG_TUNE", "");
    if (!overrideValue.empty()) {
        return overrideValue;
    }
    if (stillImage && (codecContains(codec, "x264") || codecContains(codec, "x265"))) {
        return "stillimage";
    }
    return "";
}

static std::string ffmpegVideoExtraArgs() {
    return envString("VELOX_FFMPEG_VENC_ARGS", "");
}

static bool ffmpegDecodeTelemetryEnabled() {
    const char* value = std::getenv("VELOX_FFMPEG_DECODE_TELEMETRY");
    if (value == nullptr) {
        return false;
    }
    return std::string(value) != "0" && std::string(value) != "false";
}

static std::string withDecodeTelemetry(const std::string& filter) {
    if (!ffmpegDecodeTelemetryEnabled()) {
        return filter;
    }
    return "showinfo," + filter;
}

static int ffmpegThreadCount() {
    return envInt("VELOX_FFMPEG_THREADS", 0);
}

static std::string ffmpegRateControlArgsForCodec(const std::string& codec) {
    if (codecContains(codec, "nvenc")) {
        return " -rc constqp -qp 23";
    }
    if (codecContains(codec, "vaapi")) {
        return " -qp 23";
    }
    if (codecContains(codec, "qsv")) {
        return " -global_quality 23";
    }
    return " -crf 20";
}

static void appendFfmpegVideoEncodingArgs(
    std::ostringstream& cmd,
    const std::string& codec,
    const std::string& preset,
    const std::string& tune,
    int threads,
    const std::string& extraArgs
) {
    const std::string selectedCodec = codec.empty() ? ffmpegVideoCodec() : codec;
    cmd << " -c:v " << selectedCodec;
    if (!preset.empty()) {
        cmd << " -preset " << preset;
    }
    if (!tune.empty()) {
        cmd << " -tune " << tune;
    }
    if (threads > 0) {
        cmd << " -threads " << threads;
    }
    const std::string selectedExtra = extraArgs.empty() ? ffmpegVideoExtraArgs() : extraArgs;
    if (!selectedExtra.empty()) {
        cmd << " " << selectedExtra;
    }
}

// Resolution/canvas helpers shared by all three builders.
static void canvasDims(const SceneSegmentParams& p, int& w, int& h, int& fps) {
    w = p.width > 0 ? p.width : 1920;
    h = p.height > 0 ? p.height : 1080;
    fps = p.fps > 0 ? p.fps : 30;
}

static std::string scaleFilterString(const std::string& scale_mode,
                                      const std::string& size,
                                      const std::string& res) {
    std::string filter;
    if (scale_mode == "contain") {
        filter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" + size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
    } else if (scale_mode == "stretch") {
        filter = "scale=" + size + ",format=yuv420p";
    } else {
        // cover (default) — for image sources.
        filter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" + size + ",format=yuv420p";
    }
    (void)res;
    return withDecodeTelemetry(filter);
}

#ifdef VELOX_ENABLE_LIBAV

double probeMediaDurationSeconds(const fs::path& mediaPath) {
    const auto probe = probeMediaInProcess(mediaPath);
    if (!probe.has_value() || !probe->duration_verified) {
        return 0.0;
    }
    return probe->duration_seconds;
}

static bool nativeVideoStreamCopyCompatible(
    const fs::path& clipPath,
    int width,
    int height,
    int fps,
    double requestedDuration) {
    if (clipPath.empty() || !fs::exists(clipPath) || requestedDuration <= 0.0) {
        return false;
    }

    const auto probe = probeMediaInProcess(clipPath);
    if (!probe.has_value()) {
        return false;
    }

    const auto video = std::find_if(
        probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_video; });
    if (video == probe->streams.end()) {
        return false;
    }

    // The canonical clip corpus is H.264, yuv420p, 1920x1080 at 24 fps.
    // Keep this guard conservative: copy-only callers must reject incompatible
    // media instead of producing a mixed concat stream.
    if (video->codec_id != static_cast<int>(AV_CODEC_ID_H264) ||
        video->pixel_format != static_cast<int>(AV_PIX_FMT_YUV420P) ||
        video->width != width || video->height != height ||
        std::abs(video->average_frame_rate - static_cast<double>(fps)) > 0.001) {
        return false;
    }

    const double sourceDuration = probe->duration_verified
        ? probe->duration_seconds
        : video->duration_seconds;
    return sourceDuration > 0.0 && requestedDuration <= sourceDuration + 0.05;
}

bool hasAudioStream(const fs::path& mediaPath) {
    const auto probe = probeMediaInProcess(mediaPath);
    if (!probe.has_value()) {
        return false;
    }
    return std::any_of(
        probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_audio; });
}

static bool rawAacContainer(const std::string& formatName) {
    std::string normalized;
    normalized.reserve(formatName.size());
    for (const char value : formatName) {
        normalized.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(value))));
    }
    return normalized == "aac" || normalized.find("adts") != std::string::npos ||
           normalized.find("latm") != std::string::npos ||
           normalized.find("loas") != std::string::npos;
}

FinalAudioMetadata probeFinalAudioMetadata(const fs::path& audioPath) {
    FinalAudioMetadata metadata;
    const auto probe = probeMediaInProcess(audioPath);
    if (!probe.has_value()) {
        return metadata;
    }

    const auto audio = std::find_if(
        probe->streams.begin(), probe->streams.end(),
        [](const MediaProbeStream& stream) { return stream.is_audio; });
    if (audio == probe->streams.end()) {
        return metadata;
    }

    metadata.codec = avcodec_get_name(static_cast<AVCodecID>(audio->codec_id));
    metadata.sample_rate = audio->sample_rate;
    metadata.channels = audio->channels;
    metadata.channel_layout = audio->channel_layout;
    metadata.duration_seconds = audio->duration_seconds;
    metadata.duration_verified = audio->duration_verified;
    metadata.start_time_seconds = audio->start_time_seconds;
    metadata.start_time_verified = audio->start_time_verified;
    metadata.format_name = probe->format_name;
    metadata.extradata_verified = audio->extradata_present;
    metadata.container_verified = !rawAacContainer(metadata.format_name);
    metadata.metadata_verified =
        !metadata.codec.empty() &&
        metadata.sample_rate > 0 &&
        metadata.channels > 0 &&
        !metadata.channel_layout.empty() &&
        metadata.duration_verified && std::isfinite(metadata.duration_seconds) && metadata.duration_seconds > 0.0 &&
        metadata.start_time_verified && std::isfinite(metadata.start_time_seconds);
    return metadata;
}

#else  // !VELOX_ENABLE_LIBAV

// ── ffprobe CLI fallback (pre-LibAV behavior) ────────────────────────────
// Default build without libav: every probe spawns a short ffprobe child.
// FINAL_AUDIO_COPY transport verification (extradata/container) is not
// reproducible through ffprobe, so extradata_verified/container_verified
// stay false and resolveFinalAudioMode() always falls back to AAC encode —
// the same safe outcome the legacy mux had before the LibAV guards landed.

double probeMediaDurationSeconds(const fs::path& mediaPath) {
    if (mediaPath.empty() || !fs::exists(mediaPath)) {
        return 0.0;
    }
    std::ostringstream cmd;
    cmd << "ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "
        << file::shellQuote(mediaPath.string());
    const std::string output = json::trim(file::captureCommandOutput(cmd.str()));
    if (output.empty() || output == "N/A") {
        return 0.0;
    }
    try {
        const double duration = std::stod(output);
        return duration > 0.0 ? duration : 0.0;
    } catch (...) {
        return 0.0;
    }
}

static bool parseFrameRate(const std::string& value, double& out) {
    const auto slash = value.find('/');
    try {
        if (slash == std::string::npos) {
            out = std::stod(value);
        } else {
            const double numerator = std::stod(value.substr(0, slash));
            const double denominator = std::stod(value.substr(slash + 1));
            if (denominator == 0.0) {
                return false;
            }
            out = numerator / denominator;
        }
    } catch (...) {
        return false;
    }
    return out > 0.0;
}

static bool nativeVideoStreamCopyCompatible(
    const fs::path& clipPath,
    int width,
    int height,
    int fps,
    double requestedDuration) {
    if (clipPath.empty() || !fs::exists(clipPath) || requestedDuration <= 0.0) {
        return false;
    }

    std::ostringstream probe;
    probe << "ffprobe -v error -select_streams v:0 "
          << "-show_entries stream=codec_name,width,height,avg_frame_rate,pix_fmt "
          << "-of csv=p=0 " << file::shellQuote(clipPath.string());
    const std::string output = json::trim(file::captureCommandOutput(probe.str()));
    if (output.empty()) {
        return false;
    }

    std::istringstream fields(output);
    std::string codec;
    std::string widthText;
    std::string heightText;
    std::string pixelFormat;
    std::string frameRateText;
    if (!std::getline(fields, codec, ',') ||
        !std::getline(fields, widthText, ',') ||
        !std::getline(fields, heightText, ',') ||
        !std::getline(fields, pixelFormat, ',') ||
        !std::getline(fields, frameRateText, ',')) {
        return false;
    }

    double sourceFPS = 0.0;
    int sourceWidth = 0;
    int sourceHeight = 0;
    try {
        sourceWidth = std::stoi(widthText);
        sourceHeight = std::stoi(heightText);
    } catch (...) {
        return false;
    }
    if (!parseFrameRate(frameRateText, sourceFPS)) {
        return false;
    }

    // The canonical clip corpus is H.264, yuv420p, 1920x1080 at 24 fps.
    // Keep this guard conservative: copy-only callers must reject incompatible
    // media instead of producing a mixed concat stream.
    if (json::trim(codec) != "h264" || json::trim(pixelFormat) != "yuv420p" ||
        sourceWidth != width || sourceHeight != height ||
        std::abs(sourceFPS - static_cast<double>(fps)) > 0.001) {
        return false;
    }

    const double sourceDuration = probeMediaDurationSeconds(clipPath);
    return sourceDuration > 0.0 && requestedDuration <= sourceDuration + 0.05;
}

bool hasAudioStream(const fs::path& mediaPath) {
    if (mediaPath.empty() || !fs::exists(mediaPath)) {
        return false;
    }
    std::ostringstream cmd;
    cmd << "ffprobe -v error -select_streams a:0"
        << " -show_entries stream=index -of csv=p=0 "
        << file::shellQuote(mediaPath.string());
    return !json::trim(file::captureCommandOutput(cmd.str())).empty();
}

FinalAudioMetadata probeFinalAudioMetadata(const fs::path& audioPath) {
    FinalAudioMetadata metadata;
    if (audioPath.empty() || !fs::exists(audioPath)) {
        return metadata;
    }

    std::ostringstream cmd;
    cmd << "ffprobe -v error -select_streams a:0"
        << " -show_entries stream=codec_name,sample_rate,channels,channel_layout,duration,start_time"
        << " -of default=noprint_wrappers=1 "
        << file::shellQuote(audioPath.string());
    const std::string output = file::captureCommandOutput(cmd.str());
    std::istringstream lines(output);
    std::string line;
    while (std::getline(lines, line)) {
        line = json::trim(line);
        const auto separator = line.find('=');
        if (separator == std::string::npos) {
            continue;
        }
        const std::string key = line.substr(0, separator);
        const std::string value = json::trim(line.substr(separator + 1));
        try {
            if (key == "codec_name") metadata.codec = value;
            else if (key == "sample_rate") metadata.sample_rate = std::stoi(value);
            else if (key == "channels") metadata.channels = std::stoi(value);
            else if (key == "channel_layout") metadata.channel_layout = value;
            else if (key == "duration" && value != "N/A") {
                metadata.duration_seconds = std::stod(value);
                metadata.duration_verified = true;
            } else if (key == "start_time" && value != "N/A") {
                metadata.start_time_seconds = std::stod(value);
                metadata.start_time_verified = true;
            }
        } catch (...) {
            return FinalAudioMetadata{};
        }
    }

    metadata.metadata_verified =
        !metadata.codec.empty() &&
        metadata.sample_rate > 0 &&
        metadata.channels > 0 &&
        !metadata.channel_layout.empty() &&
        metadata.duration_verified && std::isfinite(metadata.duration_seconds) && metadata.duration_seconds > 0.0 &&
        metadata.start_time_verified && std::isfinite(metadata.start_time_seconds);
    return metadata;
}

#endif // VELOX_ENABLE_LIBAV

// Shared FINAL_AUDIO_COPY transport guards used by BOTH resolvers (the
// legacy FFmpeg mux and the in-process packet mux). Keeping them in one
// place prevents the two decisions from drifting: metadata verification,
// MP4 container safety (no raw ADTS/LATM) and the AAC codec requirement
// are the same contract on every path.
static std::string finalAudioTransportReason(const FinalAudioMetadata& metadata) {
    if (!metadata.metadata_verified || !metadata.duration_verified ||
        !metadata.start_time_verified) {
        return "audio_metadata_unverified";
    }
    if (!metadata.extradata_verified || !metadata.container_verified) {
        return "audio_transport_unverified";
    }
    if (metadata.codec != "aac") {
        return "audio_codec_not_aac";
    }
    return {};
}

FinalAudioDecision resolveFinalAudioMode(
    const FinalAudioMetadata& metadata,
    bool isFinalMix,
    double expectedDurationSeconds,
    double volume,
    double startOffset) {
    FinalAudioDecision decision;
    decision.metadata = metadata;

    if (!isFinalMix) {
        decision.reason = "not_final_mix";
        return decision;
    }
    if (volume != 1.0 || startOffset != 0.0) {
        decision.reason = "final_audio_filter_required";
        return decision;
    }
    const std::string transportReason = finalAudioTransportReason(metadata);
    if (!transportReason.empty()) {
        decision.reason = transportReason;
        return decision;
    }
    if (expectedDurationSeconds <= 0.0 ||
        std::abs(metadata.duration_seconds - expectedDurationSeconds) > 0.25) {
        decision.reason = "audio_duration_mismatch";
        return decision;
    }
    if (std::abs(metadata.start_time_seconds) > 0.05) {
        decision.reason = "audio_start_time_mismatch";
        return decision;
    }

    decision.mode = FinalAudioMode::Copy;
    decision.reason = "verified_final_mix";
    return decision;
}

FinalAudioDecision resolveFinalAudioModePacket(
    const FinalAudioMetadata& metadata,
    bool isFinalMix,
    double expectedDurationSeconds) {
    FinalAudioDecision decision;
    decision.metadata = metadata;

    if (!isFinalMix) {
        decision.reason = "not_final_mix";
        return decision;
    }
    const std::string transportReason = finalAudioTransportReason(metadata);
    if (!transportReason.empty()) {
        decision.reason = transportReason;
        return decision;
    }
    // Coverage semantics: the packet mux trims trailing audio to the video
    // timeline, so a longer prepared track is still COPY; a shorter one is
    // rejected here (and again by the muxer's own duration guard).
    if (expectedDurationSeconds <= 0.0 ||
        metadata.duration_seconds + 0.05 < expectedDurationSeconds) {
        decision.reason = "audio_duration_mismatch";
        return decision;
    }

    decision.mode = FinalAudioMode::Copy;
    decision.reason = "verified_final_mix";
    return decision;
}

const char* finalAudioModeName(FinalAudioMode mode) {
    return mode == FinalAudioMode::Copy ? "COPY" : "ENCODE";
}

// ─── F5: args-only builders (canonical, the others are wrappers) ──────

std::string buildColorSegmentArgs(
    const fs::path& segmentPath,
    double duration,
    const SceneSegmentParams& params,
    const std::string& color_hex
) {
    int w, h, fps;
    canvasDims(params, w, h, fps);
    const std::string res = std::to_string(w) + "x" + std::to_string(h);

    std::string bgColor = "black";
    if (!color_hex.empty()) {
        std::string hex = color_hex;
        if (!hex.empty() && hex[0] == '#') hex = hex.substr(1);
        bgColor = "0x" + hex;
    }

    std::ostringstream cmd;
    cmd << "-f lavfi -t " << duration
        << " -i " << file::shellQuote("color=c=" + bgColor + ":s=" + res)
        << (ffmpegDecodeTelemetryEnabled() ? " -vf showinfo" : "")
        << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
        << " -pix_fmt yuv420p -r " << fps;
    appendFfmpegVideoEncodingArgs(
        cmd,
        ffmpegVideoCodec(),
        ffmpegVideoPresetForCodec(ffmpegVideoCodec()),
        ffmpegVideoTuneForCodec(ffmpegVideoCodec(), true),
        ffmpegThreadCount(),
        "");
    cmd << " " << file::shellQuote(segmentPath.string());
    return cmd.str();
}

std::string buildSceneSegmentArgs(
    const fs::path& imagePath,
    const fs::path& segmentPath,
    double duration,
    const SceneSegmentParams& params
) {
    int w, h, fps;
    canvasDims(params, w, h, fps);
    const std::string res = std::to_string(w) + "x" + std::to_string(h);
    const std::string size = std::to_string(w) + ":" + std::to_string(h);
    const int frames = std::max(1, static_cast<int>(std::round(duration * fps)));

    std::string scaleFilter = scaleFilterString(params.scale_mode, size, res);

    std::string filter;
    if (params.slow_zoom) {
        // Slow gradual zoom-in: starts at 1.0x, ends at ~1.08x.
        filter = scaleFilter
            + ",zoompan=z='1+0.08*on/(" + std::to_string(frames) + ")'"
              ":x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
              ":d=" + std::to_string(frames)
              + ":s=" + res + ":fps=" + std::to_string(fps)
            + ",format=yuv420p";
    } else {
        filter = scaleFilter;
    }
    filter = withDecodeTelemetry(filter);

    std::ostringstream cmd;
    cmd << "-stream_loop -1 -i " << file::shellQuote(imagePath.string())
        << " -vf " << file::shellQuote(filter)
        << " -frames:v " << frames
        << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
        << " -pix_fmt yuv420p -r " << fps;
    appendFfmpegVideoEncodingArgs(
        cmd,
        ffmpegVideoCodec(),
        ffmpegVideoPresetForCodec(ffmpegVideoCodec()),
        ffmpegVideoTuneForCodec(ffmpegVideoCodec(), true),
        ffmpegThreadCount(),
        "");
    cmd << " " << file::shellQuote(segmentPath.string());
    return cmd.str();
}

std::string buildVideoSegmentArgs(
    const fs::path& clipPath,
    const fs::path& segmentPath,
    double duration,
    const SceneSegmentParams& params,
    bool includeAudio
) {
    int w, h, fps;
    canvasDims(params, w, h, fps);

    if (params.copy_only && !nativeVideoStreamCopyCompatible(clipPath, w, h, fps, duration)) {
        // copy_only is a strict contract. The caller must reject or repair
        // the asset upstream; this renderer must never silently normalize it.
        return {};
    }

    if (params.copy_only) {
        std::ostringstream copyCmd;
        copyCmd << "-i " << file::shellQuote(clipPath.string())
                << " -t " << duration
                << " -map 0:v:0";
        if (includeAudio) {
            copyCmd << " -map 0:a:0? -c:a copy";
        } else {
            copyCmd << " -an";
        }
        copyCmd << " -c:v copy -avoid_negative_ts make_zero"
                << " -reset_timestamps 1 "
                << file::shellQuote(segmentPath.string());
        return copyCmd.str();
    }

    const std::string size = std::to_string(w) + ":" + std::to_string(h);

    // contain (default for clips) — fit + pad
    std::string scale_filter = "scale=" + size + ":force_original_aspect_ratio=decrease,pad=" + size + ":(ow-iw)/2:(oh-ih)/2,format=yuv420p";
    if (params.scale_mode == "cover") {
        scale_filter = "scale=" + size + ":force_original_aspect_ratio=increase,crop=" + size + ",format=yuv420p";
    } else if (params.scale_mode == "stretch") {
        scale_filter = "scale=" + size + ",format=yuv420p";
    }
    scale_filter = withDecodeTelemetry(scale_filter);

    std::ostringstream cmd;
    if (!includeAudio) {
        // Narrated scenes can be much longer than the source stock clip. Loop
        // the source so the visual bed covers the whole requested scene; -t
        // still bounds the generated segment exactly to that scene duration.
        cmd << "-stream_loop -1 ";
    }
    cmd << "-i " << file::shellQuote(clipPath.string())
        << " -t " << duration
        << " -vf " << file::shellQuote(scale_filter)
        << ffmpegRateControlArgsForCodec(ffmpegVideoCodec())
        << " -pix_fmt yuv420p -r " << fps;
    appendFfmpegVideoEncodingArgs(
        cmd,
        ffmpegVideoCodec(),
        ffmpegVideoPresetForCodec(ffmpegVideoCodec()),
        ffmpegVideoTuneForCodec(ffmpegVideoCodec(), false),
        ffmpegThreadCount(),
        "");
    if (includeAudio) {
        cmd << " -map 0:v:0 -map 0:a? -c:a aac -ar 48000 -ac 2 -shortest";
    } else {
        cmd << " -an";
    }
    cmd << " " << file::shellQuote(segmentPath.string());
    return cmd.str();
}

// ─── Execution wrappers (preserve legacy surface used by cmd_full_video) ──

bool buildSceneSegment(const fs::path& imagePath, const fs::path& segmentPath, double duration, const SceneSegmentParams& params) {
    if (!imagePath.empty() && fs::exists(imagePath)) {
        const std::string args = buildSceneSegmentArgs(imagePath, segmentPath, duration, params);
        const std::string cmd = "ffmpeg -y -hide_banner -loglevel error " + args;
        file::CommandResult r = file::runCommandTimed(cmd);
        std::cerr << "{\"metric\":\"ffmpeg.scene_segment_ms\",\"value\":" << r.wall_ms
                  << ",\"ok\":" << (r.ok ? "true" : "false")
                  << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
        return r.ok;
    }
    const std::string args = buildColorSegmentArgs(segmentPath, duration, params, params.color_hex);
    const std::string cmd = "ffmpeg -y -hide_banner -loglevel error " + args;
    file::CommandResult r = file::runCommandTimed(cmd);
    std::cerr << "{\"metric\":\"ffmpeg.color_segment_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
    return r.ok;
}

bool buildVideoSegment(const fs::path& clipPath, const fs::path& segmentPath, double duration, const SceneSegmentParams& params) {
    if (clipPath.empty() || !fs::exists(clipPath)) {
        return false;
    }
    const std::string args = buildVideoSegmentArgs(clipPath, segmentPath, duration, params);
    if (args.empty()) {
        std::cerr << "copy-only media contract rejected video segment: "
                  << clipPath << "\n";
        return false;
    }
    const std::string cmd = "ffmpeg -y -hide_banner -loglevel error " + args;
    file::CommandResult r = file::runCommandTimed(cmd);
    std::cerr << "{\"metric\":\"ffmpeg.clip_segment_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
    return r.ok;
}

bool concatSegments(const std::vector<fs::path>& segments, const fs::path& outputPath, const fs::path& workDir) {
    auto listPath = workDir / "segments.txt";
    std::ostringstream list;
    for (const auto& segment : segments) {
        list << "file " << file::shellQuote(segment.string()) << "\n";
    }
    if (!file::writeFile(listPath, list.str())) {
        return false;
    }
    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error -f concat -safe 0 -i " << file::shellQuote(listPath.string())
        << " -c copy " << file::shellQuote(outputPath.string());
    file::CommandResult r = file::runCommandTimed(cmd.str());
    std::cerr << "{\"metric\":\"ffmpeg.concat_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code << "}" << std::endl;
    return r.ok;
}

static std::string escapeJsonString(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
        switch (c) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default: out += c; break;
        }
    }
    return out;
}

bool muxAudio(const fs::path& videoPath, const fs::path& audioPath, const fs::path& outputPath, double volume, double startOffset, file::CommandResult* profile, bool isFinalMix, double expectedDurationSeconds, FinalAudioDecision* decisionOut) {
    const FinalAudioDecision decision = resolveFinalAudioMode(
        probeFinalAudioMetadata(audioPath), isFinalMix, expectedDurationSeconds, volume, startOffset);
    if (decisionOut != nullptr) {
        *decisionOut = decision;
    }

    std::ostringstream cmd;
    cmd << "ffmpeg -y -hide_banner -loglevel error -i " << file::shellQuote(videoPath.string())
        << " -i " << file::shellQuote(audioPath.string())
        << " -map 0:v:0 -map 1:a:0 -c:v copy -c:a "
        << (decision.mode == FinalAudioMode::Copy ? "copy" : "aac");

    std::ostringstream af;
    bool hasFilter = false;
    if (volume > 0.0 && volume != 1.0) {
        af << "volume=" << volume;
        hasFilter = true;
    }
    if (startOffset > 0.0) {
        int delayMs = static_cast<int>(startOffset * 1000);
        if (hasFilter) af << ",";
        af << "adelay=" << delayMs << "|" << delayMs;
        hasFilter = true;
    }
    if (hasFilter) {
        cmd << " -af " << file::shellQuote(af.str());
    }

    // FINAL_AUDIO_COPY has already validated that the AAC track covers the
    // final video timeline and has neutral timestamps. Do not let -shortest
    // truncate copied packets at an arbitrary muxer boundary. The encode
    // fallback retains the legacy -shortest behavior.
    if (decision.mode != FinalAudioMode::Copy) {
        cmd << " -shortest";
    }
    cmd << " -movflags +faststart "
        << file::shellQuote(outputPath.string());
    const std::string command = cmd.str();
    file::CommandResult r = file::runCommandTimed(command);
    if (profile != nullptr) {
        *profile = r;
    }
    std::error_code videoEc;
    std::error_code audioEc;
    std::error_code outputEc;
    const auto videoBytes = fs::file_size(videoPath, videoEc);
    const auto audioBytes = fs::file_size(audioPath, audioEc);
    const auto outputBytes = fs::file_size(outputPath, outputEc);
    std::cerr << "{\"metric\":\"ffmpeg.mux_audio_ms\",\"value\":" << r.wall_ms
              << ",\"ok\":" << (r.ok ? "true" : "false")
              << ",\"exit_code\":" << r.exit_code
              << ",\"child_user_ms\":" << r.child_user_ms
              << ",\"child_system_ms\":" << r.child_system_ms
              << ",\"child_max_rss_kb\":" << r.child_max_rss_kb
              << ",\"child_input_blocks\":" << r.child_input_blocks
              << ",\"child_output_blocks\":" << r.child_output_blocks
              << ",\"input_video_bytes\":" << (videoEc ? 0 : videoBytes)
              << ",\"input_audio_bytes\":" << (audioEc ? 0 : audioBytes)
              << ",\"output_bytes\":" << (outputEc ? 0 : outputBytes)
              << ",\"final_mux_audio_mode\":\"" << finalAudioModeName(decision.mode) << "\""
              << ",\"final_mux_audio_encode_passes\":"
              << (decision.mode == FinalAudioMode::Copy ? 0 : 1)
              << ",\"audio_metadata_verified\":"
              << (decision.metadata.metadata_verified ? "true" : "false")
              << ",\"audio_codec\":\"" << decision.metadata.codec << "\""
              << ",\"audio_sample_rate\":" << decision.metadata.sample_rate
              << ",\"audio_channels\":" << decision.metadata.channels
              << ",\"audio_channel_layout\":\"" << decision.metadata.channel_layout << "\""
              << ",\"audio_duration_seconds\":" << decision.metadata.duration_seconds
              << ",\"audio_start_time_seconds\":" << decision.metadata.start_time_seconds
              << ",\"audio_format_name\":\"" << decision.metadata.format_name << "\""
              << ",\"audio_extradata_verified\":"
              << (decision.metadata.extradata_verified ? "true" : "false")
              << ",\"audio_container_verified\":"
              << (decision.metadata.container_verified ? "true" : "false")
              << ",\"decision_reason\":\"" << decision.reason << "\""
              << ",\"command\":\"" << escapeJsonString(command) << "\"}"
              << std::endl;
    return r.ok;
}

} // namespace velox::media
