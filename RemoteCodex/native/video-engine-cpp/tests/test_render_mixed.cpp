#include "velox/core/render_engine.hpp"
#include "velox/plan/render_plan.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <variant>

namespace {
int failures = 0;
void expect(bool condition, const std::string& message) {
    if (!condition) { std::cerr << "FAIL: " << message << "\n"; ++failures; }
}
}

int main() {
    const auto output = std::filesystem::temp_directory_path() / "velox-video-reencode-forbidden.mp4";
    std::error_code ec;
    std::filesystem::remove(output, ec);

    for (const bool mixed : {false, true}) {
        velox::core::RenderEngine engine;
        velox::plan::RenderPlan plan;
        plan.job_id = mixed ? "mixed-video-rejected" : "editorial-video-rejected";
        plan.output_path = output.string();
        plan.mixed = mixed;
        plan.requires_editorial_render = !mixed;
        plan.timeline.push_back({velox::plan::VideoSource{"missing-source.mp4", ""}, 1.0, false});
        const auto result = engine.render(plan);
        expect(!result.success, "existing video outside explicit copy-only assembly must fail");
        expect(result.error.find("video_reencode_forbidden") != std::string::npos,
               "failure identifies the zero-reencode invariant: " + result.error);
        expect(engine.framesEncoded() == 0 && engine.encodePasses() == 0,
               "rejected video plan invokes no encoder");
        expect(!std::filesystem::exists(output), "rejected video plan publishes no output");
    }
    return failures == 0 ? 0 : 1;
}
