#pragma once

#include "velox/plan/render_plan.hpp"

#include <optional>
#include <string>

namespace velox::plan::detail {

std::string extractObjectBlock(const std::string& json, const std::string& key);
std::string bindingPathFor(const std::string& bindingsBlock, const std::string& assetId);

std::optional<RenderPlan> parseRenderPlanV1(
    const std::string& jsonStr,
    RenderPlan plan);

std::optional<RenderPlan> parseRenderPlanV2(
    const std::string& jsonStr,
    RenderPlan plan);

} // namespace velox::plan::detail
