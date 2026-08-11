#pragma once

#include "velox/audio/audio_plan.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace velox::audio {

struct AudioMixBenchmarkResult {
    bool enabled{false};
    bool ran{false};
    std::string method{"disabled"};
    std::string failure_reason;
    double inputs_open_ms{0.0};
    double decode_ms{0.0};
    double filtergraph_ms{0.0};
    double encode_ms{0.0};
    double output_write_ms{0.0};
    double wall_ms{0.0};
    double user_cpu_ms{0.0};
    double system_cpu_ms{0.0};
    long peak_rss_kb{0};
    std::uintmax_t input_bytes{0};
    std::uintmax_t output_bytes{0};
};

// Runs only when VELOX_AUDIO_MIX_PROFILE=1. The benchmark is deliberately
// separate from the production command: it estimates the filtergraph and
// encoder components using null-output controls, then leaves the certified
// render path unchanged.
AudioMixBenchmarkResult runAudioMixBenchmark(
    const std::vector<AudioPlanInput>& inputs,
    const std::string& filter_graph,
    double render_duration_seconds,
    const std::string& work_directory);

} // namespace velox::audio
