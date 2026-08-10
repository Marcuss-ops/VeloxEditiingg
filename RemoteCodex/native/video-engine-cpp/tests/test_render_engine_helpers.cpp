#include "render_engine_helpers.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

namespace {

int failures = 0;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        ++failures;
    }
}

void testDecodedFrameCount() {
    expect(velox::core::render_detail::decodedFramesFromShowInfo("") == 0,
           "empty showinfo output has no decoded frames");
    expect(velox::core::render_detail::decodedFramesFromShowInfo("[Parsed_showinfo] n: 0 pts:0\n") == 1,
           "single frame is counted");
    expect(velox::core::render_detail::decodedFramesFromShowInfo(
               " n: 0\n n: 4\n n: 2\n") == 5,
           "highest showinfo frame index determines decoded frame count");
}

void testComposeSegmentCommand() {
    unsetenv("VELOX_FFMPEG_DECODE_TELEMETRY");
    const auto default_command =
        velox::core::render_detail::composeSegmentCmd("-i input -f mp4 output");
    expect(default_command.find("-loglevel error") != std::string::npos,
           "telemetry disabled uses error log level");
    expect(default_command.find("-progress pipe:1 -nostats") != std::string::npos,
           "segment command keeps progress flags");

    setenv("VELOX_FFMPEG_DECODE_TELEMETRY", "1", 1);
    const auto telemetry_command =
        velox::core::render_detail::composeSegmentCmd("-i input -f mp4 output");
    expect(telemetry_command.find("-loglevel info") != std::string::npos,
           "telemetry enabled uses info log level");
    unsetenv("VELOX_FFMPEG_DECODE_TELEMETRY");
}

} // namespace

int main() {
    testDecodedFrameCount();
    testComposeSegmentCommand();
    if (failures != 0) {
        std::cerr << "render_engine_helpers tests failed: " << failures << "\n";
        return 1;
    }
    std::cout << "render_engine_helpers tests passed\n";
    return 0;
}
