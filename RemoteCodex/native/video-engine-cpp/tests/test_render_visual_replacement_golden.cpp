#include "velox/core/render_engine.hpp"
#include "velox/plan/render_plan.hpp"

#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const auto output = std::filesystem::temp_directory_path() / "velox-overlay-zero-reencode-golden.mp4";
    std::error_code ec;
    std::filesystem::remove(output, ec);
    velox::core::RenderEngine engine;
    velox::plan::RenderPlan plan;
    plan.job_id = "visual-overlay-existing-video-rejected";
    plan.output_path = output.string();
    plan.requires_editorial_render = true;
    plan.timeline.push_back({velox::plan::VideoSource{"base.mp4", ""}, 1.0, false});
    plan.timeline.push_back({velox::plan::ImageSource{"overlay.png", ""}, 1.0, false});
    const auto result = engine.render(plan);
    if (result.success || result.error.find("video_reencode_forbidden") == std::string::npos ||
        engine.framesEncoded() != 0 || engine.encodePasses() != 0 || std::filesystem::exists(output)) {
        std::cerr << "existing-video overlay did not fail before encoding: " << result.error << "\n";
        return 1;
    }
    return 0;
}
