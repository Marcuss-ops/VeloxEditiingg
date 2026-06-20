#pragma once
#include "velox/plan/render_plan.hpp"

namespace velox::core {

struct RenderResult {
    bool success{false};
    std::string error;
    std::string output_path;
};

class RenderEngine {
public:
    RenderEngine() = default;
    
    // Esegue il rendering completo del RenderPlan dato
    RenderResult render(const plan::RenderPlan& plan);
};

} // namespace velox::core
