#include "velox/plan/render_plan_parser.hpp"
#include "velox/core/render_engine.hpp"
#include "velox/services/file_utils.hpp"
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
