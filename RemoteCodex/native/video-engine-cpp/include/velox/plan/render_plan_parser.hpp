#pragma once
#include <string>
#include "velox/plan/render_plan.hpp"

namespace velox::plan {

// Legge e fa il parsing di un RenderPlan da una stringa JSON.
// Ritorna std::nullopt se il parsing o la validazione falliscono.
std::optional<RenderPlan> parseRenderPlan(const std::string& jsonStr);

} // namespace velox::plan
