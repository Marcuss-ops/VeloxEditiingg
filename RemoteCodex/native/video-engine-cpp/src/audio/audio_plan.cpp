#include "velox/audio/audio_plan.hpp"
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <limits>

namespace velox::audio {

namespace {

struct Span {
    std::size_t index;
    double start;
    double end;
};

bool nearlyEqual(double lhs, double rhs, double tolerance) {
    return std::abs(lhs - rhs) <= tolerance;
}

} // namespace

const char* audioMixStrategyName(AudioMixStrategy strategy) {
    switch (strategy) {
    case AudioMixStrategy::OptimizedTimeline:
        return "OPTIMIZED_TIMELINE";
    case AudioMixStrategy::Auto:
        return "AUTO";
    case AudioMixStrategy::LegacyAmix:
    default:
        return "LEGACY_AMIX";
    }
}

AudioMixStrategy requestedAudioMixStrategy() {
    const char* value = std::getenv("VELOX_AUDIO_MIX_STRATEGY");
    if (value == nullptr) {
        return AudioMixStrategy::LegacyAmix;
    }
    const std::string requested(value);
    if (requested == "optimized" || requested == "OPTIMIZED_TIMELINE") {
        return AudioMixStrategy::OptimizedTimeline;
    }
    if (requested == "auto" || requested == "AUTO") {
        return AudioMixStrategy::Auto;
    }
    return AudioMixStrategy::LegacyAmix;
}

CompiledAudioPlan compileAudioPlan(const std::vector<AudioPlanInput>& inputs,
                                   double render_duration_seconds,
                                   double tolerance_seconds) {
    CompiledAudioPlan result;
    if (inputs.empty()) {
        result.fallback_reason = "no_inputs";
        return result;
    }

    std::vector<Span> spans;
    spans.reserve(inputs.size());
    for (std::size_t i = 0; i < inputs.size(); ++i) {
        const auto& input = inputs[i];
        const double start = std::max(0.0, input.start_time_offset);
        const double duration = input.duration_seconds;
        if (!std::isfinite(start) || !std::isfinite(duration) || duration <= 0.0) {
            result.fallback_reason = "missing_or_invalid_duration";
            return result;
        }
        spans.push_back({i, start, start + duration});
    }

    std::vector<Span> ordered = spans;
    std::sort(ordered.begin(), ordered.end(), [](const Span& lhs, const Span& rhs) {
        if (lhs.start != rhs.start) return lhs.start < rhs.start;
        return lhs.index < rhs.index;
    });

    std::vector<double> boundaries;
    boundaries.reserve(ordered.size() * 2);
    for (const auto& span : ordered) {
        boundaries.push_back(span.start);
        boundaries.push_back(span.end);
    }
    std::sort(boundaries.begin(), boundaries.end());
    boundaries.erase(std::unique(boundaries.begin(), boundaries.end(),
                                 [tolerance_seconds](double lhs, double rhs) {
                                     return nearlyEqual(lhs, rhs, tolerance_seconds);
                                 }),
                     boundaries.end());

    std::vector<std::size_t> overlappingInputs;
    for (std::size_t i = 0; i + 1 < boundaries.size(); ++i) {
        const double midpoint = (boundaries[i] + boundaries[i + 1]) / 2.0;
        std::size_t concurrent = 0;
        for (const auto& span : spans) {
            if (span.start <= midpoint && midpoint < span.end) {
                ++concurrent;
                if (std::find(overlappingInputs.begin(), overlappingInputs.end(), span.index) ==
                    overlappingInputs.end()) {
                    overlappingInputs.push_back(span.index);
                }
            }
        }
        result.max_concurrent_inputs = std::max(result.max_concurrent_inputs, concurrent);
    }
    result.overlapping_count = overlappingInputs.size();

    // A looped track is an overlay by contract (normally background music),
    // never part of the finite primary sequence.
    std::vector<Span> primary;
    for (const auto& span : ordered) {
        if (!inputs[span.index].loop) {
            primary.push_back(span);
        } else {
            result.overlay_indices.push_back(span.index);
        }
    }

    double cursor = 0.0;
    bool primary_is_contiguous = !primary.empty();
    for (const auto& span : primary) {
        if (!nearlyEqual(span.start, cursor, tolerance_seconds)) {
            primary_is_contiguous = false;
        }
        cursor = std::max(cursor, span.end);
    }

    // A finite non-loop input not in the primary chain is a genuine overlay.
    // It is safe only if it does not overlap the primary chain in a way the
    // current compiler cannot represent. The current implementation supports
    // these as delayed overlays, so retain them explicitly.
    for (const auto& span : primary) {
        result.primary_indices.push_back(span.index);
    }
    for (const auto& span : spans) {
        if (!inputs[span.index].loop &&
            std::find(result.primary_indices.begin(), result.primary_indices.end(), span.index)
                == result.primary_indices.end()) {
            result.overlay_indices.push_back(span.index);
        }
    }

    result.sequential_count = 0;
    for (const auto index : result.primary_indices) {
        if (std::find(overlappingInputs.begin(), overlappingInputs.end(), index) ==
            overlappingInputs.end()) {
            ++result.sequential_count;
        }
    }
    result.overlapping_count = std::max(result.overlapping_count,
                                        result.overlay_indices.size());

    const bool covers_render = render_duration_seconds <= 0.0 ||
        nearlyEqual(cursor, render_duration_seconds, std::max(tolerance_seconds, 0.05));
    // The first optimized implementation is intentionally limited to a
    // pure sequential timeline. Overlay compilation is represented in the
    // plan for telemetry and future work, but must not be enabled until its
    // filtergraph has its own equivalence tests.
    result.safe_for_optimized_timeline = primary_is_contiguous &&
        covers_render && result.overlay_indices.empty();
    if (!result.safe_for_optimized_timeline) {
        if (!primary_is_contiguous) {
            result.fallback_reason = "primary_timeline_has_gap_or_overlap";
        } else if (!covers_render) {
            result.fallback_reason = "primary_timeline_does_not_cover_render";
        } else if (!result.overlay_indices.empty()) {
            result.fallback_reason = "overlay_filtergraph_not_enabled";
        }
    }
    return result;
}

AudioMixStrategy resolveAudioMixStrategy(AudioMixStrategy requested,
                                         const CompiledAudioPlan& plan) {
    if (requested == AudioMixStrategy::OptimizedTimeline &&
        plan.safe_for_optimized_timeline) {
        return AudioMixStrategy::OptimizedTimeline;
    }
    if (requested == AudioMixStrategy::Auto && plan.safe_for_optimized_timeline) {
        return AudioMixStrategy::OptimizedTimeline;
    }
    return AudioMixStrategy::LegacyAmix;
}

} // namespace velox::audio
