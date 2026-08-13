#include "velox/services/file_utils.hpp"
#include "velox/services/segment_execution_libav.hpp"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

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
    return "velox_segment_execution_libav_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

bool makeVideo(const fs::path& output, const std::string& size) {
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error"
            << " -f lavfi -i " << velox::file::shellQuote(
                "testsrc=size=" + size + ":rate=5:duration=1.2")
            << " -an -c:v libx264 -preset ultrafast -pix_fmt yuv420p -r 5 "
            << " -g 2 -keyint_min 2 -sc_threshold 0 "
            << velox::file::shellQuote(output.string());
    return velox::file::runCommand(command.str());
}

} // namespace

int main() {
    const fs::path root = fs::temp_directory_path() / uniqueStem();
    fs::create_directories(root);
    const fs::path video = root / "probe.mp4";
    if (!makeVideo(video, "64x64")) {
        std::cerr << "FAIL: video fixture could not be created\n";
        fs::remove_all(root);
        return 1;
    }

    using velox::media::MediaKind;
    using velox::media::probeSegmentForExecution;

    velox::media::SegmentProbe probe;
    std::string error;

    expect(probeSegmentForExecution(video, 0, MediaKind::Video, &probe, &error),
           "probe succeeds for a video asset");
    expect(probe.signature.kind == velox::media::MediaKind::Video,
           "probe reports the video media kind");
    expect(probe.signature.codec_id == 27,
           "probe reads the H.264 stream codec id");
    expect(probe.signature.width == 64 && probe.signature.height == 64,
           "probe reads the fixture geometry");
    expect(probe.signature.pixel_format == 0,
           "probe reads the yuv420p pixel format");
    expect(probe.signature.frame_rate_num == 5 && probe.signature.frame_rate_den == 1,
           "probe reads the rational frame rate");

    expect(probeSegmentForExecution(video, 0, MediaKind::Video, &probe, &error) &&
               probe.source_window_keyframe_safe,
           "zero offset always starts on a keyframe");
    expect(probeSegmentForExecution(video, 400'000, MediaKind::Video, &probe, &error) &&
               probe.source_window_keyframe_safe,
           "exact keyframe offset is reported keyframe-safe");
    expect(probeSegmentForExecution(video, 200'000, MediaKind::Video, &probe, &error) &&
               !probe.source_window_keyframe_safe,
           "non-keyframe offset is reported unsafe (not a probe failure)");

    expect(!probeSegmentForExecution(video, 0, MediaKind::Audio, &probe, &error),
           "probe fails when the requested media kind is absent");
    expect(error.find("missing") != std::string::npos,
           "missing-stream probe failure explains the missing stream");

    expect(!probeSegmentForExecution(root / "nope.mp4", 0, MediaKind::Video, &probe, &error),
           "probe fails for a missing asset");

    fs::remove_all(root);
    return failures == 0 ? 0 : 1;
}
