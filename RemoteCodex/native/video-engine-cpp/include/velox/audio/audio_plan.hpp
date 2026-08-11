#pragma once

#include "velox/plan/render_plan.hpp"
#include <cstddef>
#include <string>
#include <vector>

namespace velox::audio {

enum class AudioMixStrategy {
    LegacyAmix,
    OptimizedTimeline,
    Auto,
};

const char* audioMixStrategyName(AudioMixStrategy strategy);
AudioMixStrategy requestedAudioMixStrategy();

// A downloaded input is deliberately represented by a value copy of the
// plan fields. The compiler must stay independent from RenderEngine's file
// and lifetime management so it can be tested with synthetic timelines.
struct AudioPlanInput {
    std::string path;
    std::string role;
    double volume{1.0};
    double start_time_offset{0.0};
    double duration_seconds{0.0};
    bool loop{false};
};

struct CompiledAudioPlan {
    std::vector<std::size_t> primary_indices;
    std::vector<std::size_t> overlay_indices;
    std::size_t sequential_count{0};
    std::size_t overlapping_count{0};
    std::size_t max_concurrent_inputs{0};
    bool safe_for_optimized_timeline{false};
    std::string fallback_reason;
};

CompiledAudioPlan compileAudioPlan(const std::vector<AudioPlanInput>& inputs,
                                   double render_duration_seconds,
                                   double tolerance_seconds = 0.002);

// Resolves the operator-selected strategy without silently changing the
// certified default. The environment accepts legacy, optimized, or auto;
// the latter selects optimized only when the compiler proves it safe.
AudioMixStrategy resolveAudioMixStrategy(AudioMixStrategy requested,
                                         const CompiledAudioPlan& plan);

} // namespace velox::audio
