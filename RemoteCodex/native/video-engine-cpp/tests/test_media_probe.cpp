#include "velox/services/file_utils.hpp"
#include "velox/services/media_probe.hpp"
#include "velox/services/media_utils.hpp"

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

extern "C" {
#include <libavcodec/codec_id.h>
#include <libavutil/pixfmt.h>
}

namespace fs = std::filesystem;

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

std::string uniqueStem() {
    return "velox_media_probe_" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeVideo(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote("color=c=black:s=64x64:r=5")
            << " -t 1.2 -an -c:v libx264 -pix_fmt yuv420p -r 5 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

bool makeAudio(const fs::path& output) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i "
            << velox::file::shellQuote("sine=frequency=440:sample_rate=48000")
            << " -t 1.2 -c:a aac -ar 48000 -ac 2 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

} // namespace

int main() {
    const fs::path root = fs::temp_directory_path() / uniqueStem();
    std::error_code ec;
    fs::create_directories(root, ec);
    expect(!ec, "temporary directory can be created");
    if (ec) return 1;

    struct Cleanup {
        fs::path root;
        ~Cleanup() {
            std::error_code ec;
            fs::remove_all(root, ec);
        }
    } cleanup{root};

    const fs::path video = root / "video.mp4";
    const fs::path audio = root / "audio.m4a";
    expect(makeVideo(video), "video fixture can be created");
    expect(makeAudio(audio), "audio fixture can be created");
    if (failures != 0) return 1;

    const char* previousPath = std::getenv("PATH");
    const std::string previousPathValue = previousPath == nullptr ? "" : previousPath;
    const bool hadPath = previousPath != nullptr;
    const fs::path emptyPath = root / "empty-bin";
    fs::create_directory(emptyPath, ec);
    expect(!ec, "empty PATH directory can be created");
    const fs::path ffprobeSentinel = root / "ffprobe-invoked";
    const fs::path fakeFfprobe = emptyPath / "ffprobe";
    expect(velox::file::writeFile(
               fakeFfprobe,
               "#!/bin/sh\ntouch " + velox::file::shellQuote(ffprobeSentinel.string()) + "\nexit 1\n"),
           "ffprobe sentinel can be written");
    fs::permissions(
        fakeFfprobe,
        fs::perms::owner_read | fs::perms::owner_write | fs::perms::owner_exec,
        fs::perm_options::replace,
        ec);
    expect(!ec, "ffprobe sentinel can be made executable");
    setenv("PATH", emptyPath.c_str(), 1);

    const auto videoProbe = velox::media::probeMediaInProcess(video);
    expect(videoProbe.has_value(), "LibAV opens video without ffprobe in PATH");
    if (videoProbe.has_value()) {
        expect(videoProbe->duration_verified, "video duration is verified in-process");
        expect(videoProbe->duration_seconds > 1.0 && videoProbe->duration_seconds < 1.4,
               "video duration is approximately 1.2 seconds");
        expect(!videoProbe->streams.empty(), "video stream metadata is present");
        if (!videoProbe->streams.empty()) {
            const auto& stream = videoProbe->streams.front();
            expect(stream.is_video, "first fixture stream is video");
            expect(stream.codec_id == static_cast<int>(AV_CODEC_ID_H264),
                   "video codec is H.264");
            expect(stream.pixel_format == static_cast<int>(AV_PIX_FMT_YUV420P),
                   "video pixel format is yuv420p");
            expect(stream.width == 64 && stream.height == 64,
                   "video dimensions are preserved");
            expect(std::abs(stream.average_frame_rate - 5.0) < 0.001,
                   "video frame rate is preserved");
        }
    }

    expect(velox::media::probeMediaDurationSeconds(video) > 1.0,
           "duration helper uses LibAV without ffprobe");
    expect(velox::media::hasAudioStream(audio),
           "audio stream helper uses LibAV without ffprobe");
    const auto audioMetadata = velox::media::probeFinalAudioMetadata(audio);
    expect(audioMetadata.codec == "aac", "audio codec is AAC");
    expect(audioMetadata.sample_rate == 48000, "audio sample rate is 48 kHz");
    expect(audioMetadata.channels == 2, "audio channel count is stereo");
    expect(!audioMetadata.channel_layout.empty(), "audio channel layout is present");
    expect(audioMetadata.duration_verified && audioMetadata.duration_seconds > 1.0,
           "audio duration is verified in-process");

    velox::media::SceneSegmentParams copyParams;
    copyParams.width = 64;
    copyParams.height = 64;
    copyParams.fps = 5;
    copyParams.copy_only = true;
    const auto compatibleArgs = velox::media::buildVideoSegmentArgs(
        video, root / "segment.mp4", 1.0, copyParams, false);
    expect(!compatibleArgs.empty(),
           "copy-only compatibility accepts the LibAV-probed fixture");
    copyParams.width = 128;
    const auto incompatibleArgs = velox::media::buildVideoSegmentArgs(
        video, root / "incompatible.mp4", 1.0, copyParams, false);
    expect(incompatibleArgs.empty(),
           "copy-only compatibility rejects mismatched dimensions");

    expect(!fs::exists(ffprobeSentinel),
           "LibAV probing never attempts to execute ffprobe");

    const auto missingProbe = velox::media::probeMediaInProcess(root / "missing.mp4");
    expect(!missingProbe.has_value(), "missing media fails closed");
    expect(!velox::media::hasAudioStream(root / "missing.m4a"),
           "missing audio fails closed");

    if (hadPath) {
        setenv("PATH", previousPathValue.c_str(), 1);
    } else {
        unsetenv("PATH");
    }

    std::cerr << "summary: fail=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}
