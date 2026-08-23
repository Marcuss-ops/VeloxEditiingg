#include "velox/audio/audio_plan.hpp"
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

void expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        std::exit(1);
    }
}

velox::audio::AudioPlanInput input(double start, double duration,
                                   bool loop = false,
                                   const char* role = "voiceover") {
    velox::audio::AudioPlanInput value;
    value.path = "track.m4a";
    value.start_time_offset = start;
    value.duration_seconds = duration;
    value.loop = loop;
    value.role = role;
    return value;
}

} // namespace

int main() {
    using velox::audio::AudioMixStrategy;
    using velox::audio::compileAudioPlan;
    using velox::audio::resolveAudioMixStrategy;

    const auto sequential = compileAudioPlan({
        input(0.0, 10.0), input(10.0, 20.0), input(30.0, 5.0)
    }, 35.0);
    expect(sequential.safe_for_optimized_timeline, "contiguous timeline is optimized-safe");
    expect(sequential.primary_indices.size() == 3, "all finite tracks are primary");
    expect(sequential.overlay_indices.empty(), "sequential timeline has no overlays");
    expect(sequential.max_concurrent_inputs == 1, "sequential timeline has no overlap");
    expect(resolveAudioMixStrategy(AudioMixStrategy::OptimizedTimeline, sequential) ==
               AudioMixStrategy::OptimizedTimeline,
           "optimized request is honored for safe plan");
    expect(resolveAudioMixStrategy(AudioMixStrategy::Auto, sequential) ==
               AudioMixStrategy::OptimizedTimeline,
           "auto request selects optimized for safe plan");

    const auto overlap = compileAudioPlan({
        input(0.0, 35.0), input(0.0, 10.0, false, "sfx")
    }, 35.0);
    expect(!overlap.safe_for_optimized_timeline, "overlap is not treated as sequential");
    expect(overlap.max_concurrent_inputs == 2, "overlap concurrency is measured");
    expect(overlap.sequential_count == 0, "overlapping inputs are not counted as sequential");
    expect(resolveAudioMixStrategy(AudioMixStrategy::OptimizedTimeline, overlap) ==
               AudioMixStrategy::LegacyAmix,
           "unsafe overlap falls back to legacy");

    const auto looped = compileAudioPlan({input(0.0, 35.0), input(0.0, 35.0, true, "bgm")}, 35.0);
    expect(looped.overlay_indices.size() == 1, "looped track is an overlay");
    expect(!looped.safe_for_optimized_timeline, "looped overlay is conservative for now");

    const auto gap = compileAudioPlan({
        input(0.0, 10.0), input(10.1, 5.0)
    }, 15.1);
    expect(!gap.safe_for_optimized_timeline, "timeline gap falls back");
    expect(gap.fallback_reason == "primary_timeline_has_gap_or_overlap",
           "timeline gap has an explicit fallback reason");
    expect(resolveAudioMixStrategy(AudioMixStrategy::Auto, gap) ==
               AudioMixStrategy::LegacyAmix,
           "auto strategy never optimizes a gapped timeline");

    const auto uncovered = compileAudioPlan({input(0.0, 4.0)}, 5.0);
    expect(!uncovered.safe_for_optimized_timeline,
           "timeline shorter than the render falls back");
    expect(uncovered.fallback_reason == "primary_timeline_does_not_cover_render",
           "uncovered render has an explicit fallback reason");

    const auto invalid = compileAudioPlan({input(0.0, 0.0)}, 1.0);
    expect(!invalid.safe_for_optimized_timeline, "invalid duration falls back");
    expect(invalid.fallback_reason == "missing_or_invalid_duration",
           "invalid duration has an explicit reason");

    const auto negativeOffset = compileAudioPlan({input(-2.0, 5.0)}, 5.0);
    expect(negativeOffset.safe_for_optimized_timeline,
           "negative start offsets are clamped to the timeline origin");
    expect(negativeOffset.primary_indices.size() == 1,
           "clamped negative offset remains a primary input");

    std::cout << "audio_plan tests passed\n";
    return 0;
}
