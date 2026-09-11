#include "velox/plan/render_plan_parser.hpp"
#include "json_utils.hpp"
#include "render_plan_parser_internal.hpp"

#include <atomic>
#include <iostream>

namespace velox::plan {

namespace {
std::atomic<std::uint64_t> g_v1RejectedEntryCount{0};
}

std::optional<RenderPlan> parseRenderPlan(const std::string& jsonStr) {
    namespace ju = velox::json;
    RenderPlan plan;
    plan.version = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "plan_version", 0.0));
    if (plan.version == 0) {
        plan.version = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "version", 0.0));
    }
    if (plan.version != kRenderPlanVersionV1 && plan.version != kRenderPlanVersionV2) {
        std::cerr << "errore: versione del piano non supportata o mancante: " << plan.version << "\n";
        return std::nullopt;
    }

    plan.job_id = ju::extractJsonStringValue(jsonStr, "job_id");
    plan.output_path = ju::extractJsonStringValue(jsonStr, "output_path");
    if (plan.job_id.empty() || plan.output_path.empty()) {
        std::cerr << "errore: job_id o output_path mancanti nel RenderPlan\n";
        return std::nullopt;
    }
    if (plan.version == kRenderPlanVersionV2) {
        return detail::parseRenderPlanV2(jsonStr, std::move(plan));
    }
    return detail::parseRenderPlanV1(jsonStr, std::move(plan));
}

std::uint64_t v1RejectedEntryCount() {
    return g_v1RejectedEntryCount.load(std::memory_order_relaxed);
}

void recordV1RejectedEntry() {
    g_v1RejectedEntryCount.fetch_add(1, std::memory_order_relaxed);
}

} // namespace velox::plan
