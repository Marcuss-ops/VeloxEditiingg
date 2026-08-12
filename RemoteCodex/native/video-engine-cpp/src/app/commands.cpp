#include "velox/plan/render_plan_parser.hpp"
#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
#include "velox/services/frame_pipeline.hpp"
#include <cstdlib>
#include <iostream>
#include <string>

int cmdRenderPlan(int argc, char** argv) {
    std::string planPath;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--plan" && i + 1 < argc) {
            planPath = argv[++i];
        }
    }
    if (planPath.empty()) {
        std::cerr << "errore: --render richiede --plan <path>\n";
        return 1;
    }

    std::string content = velox::file::readFile(planPath);
    if (content.empty()) {
        std::cerr << "errore: impossibile leggere il file del piano di render: " << planPath << "\n";
        return 1;
    }

    auto planOpt = velox::plan::parseRenderPlan(content);
    if (!planOpt.has_value()) {
        std::cerr << "errore: parsing o validazione del RenderPlan fallita\n";
        return 1;
    }

    velox::core::RenderEngine engine;
    // The CLI has no caller-side progress consumer, but the engine still
    // needs a callback attached so it parses FFmpeg -progress blocks and
    // persists the final snapshot in <output>.progress.json. The worker
    // attaches its own callback through RenderClient; this no-op keeps the
    // direct canonical CLI path observably equivalent for smoke/tests.
    engine.setProgressCallback([](const velox::services::EngineProgress&) {});
    auto result = engine.render(planOpt.value());

    if (result.success) {
        std::cout << "{\"success\":true,\"job_id\":\"" << planOpt->job_id 
                  << "\",\"output_path\":\"" << result.output_path << "\"}" << std::endl;
        return 0;
    } else {
        std::cerr << "errore rendering: " << result.error << "\n";
        std::cout << "{\"success\":false,\"error\":\"" << result.error << "\"}" << std::endl;
        return 1;
    }
}

// --render-frames: the explicit in-process AVFrame producer-consumer encode
// path. Only jobs that genuinely need encoding invoke this; copy-only jobs
// stay on the zero-spawn packet path and never reach this command.
int cmdRenderFrames(int argc, char** argv) {
    velox::media::FramePipelineConfig config;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--input" && i + 1 < argc) {
            config.input_path = argv[++i];
        } else if (arg == "--output" && i + 1 < argc) {
            config.output_path = argv[++i];
        } else if (arg == "--width" && i + 1 < argc) {
            config.width = std::atoi(argv[++i]);
        } else if (arg == "--height" && i + 1 < argc) {
            config.height = std::atoi(argv[++i]);
        } else if (arg == "--fps" && i + 1 < argc) {
            config.fps_num = std::atoi(argv[++i]);
        } else if (arg == "--codec" && i + 1 < argc) {
            config.codec = argv[++i];
        } else if (arg == "--preset" && i + 1 < argc) {
            config.preset = argv[++i];
        } else if (arg == "--pool" && i + 1 < argc) {
            config.pool_capacity = std::atoi(argv[++i]);
        }
    }
    if (config.input_path.empty() || config.output_path.empty()) {
        std::cerr << "errore: --render-frames richiede --input e --output\n";
        return 1;
    }

    velox::media::FramePipelineResult pipeline;
    if (!velox::media::renderFrames(config, &pipeline)) {
        std::cerr << "errore frame pipeline: " << pipeline.error << "\n";
        std::cout << "{\"success\":false,\"error\":\"" << pipeline.error << "\"}" << std::endl;
        return 1;
    }
    std::cout << "{\"success\":true,\"output_path\":\""
              << config.output_path.string()
              << "\",\"frames_decoded\":" << pipeline.frames_decoded
              << ",\"frames_encoded\":" << pipeline.frames_encoded
              << ",\"encode_contexts\":" << pipeline.encode_contexts_created
              << ",\"peak_pool_usage\":" << pipeline.peak_pool_usage
              << ",\"output_durable\":"
              << (pipeline.output_durable ? "true" : "false") << "}" << std::endl;
    return 0;
}
