#include "velox/audio/audio_benchmark.hpp"
#include "velox/services/file_utils.hpp"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace fs = std::filesystem;

namespace velox::audio {

namespace {

bool enabledByEnvironment() {
    const char* value = std::getenv("VELOX_AUDIO_MIX_PROFILE");
    return value != nullptr && std::string(value) == "1";
}

file::CommandResult run(const std::string& command) {
    return file::runCommandTimed(command);
}

void addCommandStats(AudioMixBenchmarkResult& result, const file::CommandResult& profile) {
    result.wall_ms += profile.wall_ms;
    result.user_cpu_ms += profile.child_user_ms;
    result.system_cpu_ms += profile.child_system_ms;
    result.peak_rss_kb = std::max(result.peak_rss_kb, profile.child_max_rss_kb);
}

std::string inputArguments(const std::vector<AudioPlanInput>& inputs) {
    std::ostringstream args;
    for (const auto& input : inputs) {
        args << " -i " << file::shellQuote(input.path);
    }
    return args.str();
}

} // namespace

AudioMixBenchmarkResult runAudioMixBenchmark(
    const std::vector<AudioPlanInput>& inputs,
    const std::string& filter_graph,
    double render_duration_seconds,
    const std::string& work_directory) {
    AudioMixBenchmarkResult result;
    result.enabled = enabledByEnvironment();
    if (!result.enabled) return result;
    result.method = "controlled_ffmpeg_null_output";
    if (inputs.empty()) {
        result.failure_reason = "no_inputs";
        return result;
    }
    for (const auto& input : inputs) {
        if (input.loop) {
            result.failure_reason = "loop_input_not_supported_by_controlled_benchmark";
            return result;
        }
    }

    const fs::path workDir(work_directory);
    const fs::path pcmPath = workDir / "audio_profile_filtered.wav";
    const fs::path encodedPath = workDir / "audio_profile_encoded.m4a";
    struct TempCleanup {
        fs::path pcm;
        fs::path encoded;
        ~TempCleanup() {
            std::error_code ec;
            fs::remove(pcm, ec);
            fs::remove(encoded, ec);
        }
    } cleanup{pcmPath, encodedPath};
    const std::string inputsArg = inputArguments(inputs);
    const std::string quotedFilter = file::shellQuote(filter_graph);
    const std::string duration = std::to_string(render_duration_seconds);

    const auto openStart = std::chrono::steady_clock::now();
    for (const auto& input : inputs) {
        std::ifstream stream(input.path, std::ios::binary);
        if (!stream) {
            result.failure_reason = "input_open_failed";
            return result;
        }
        char byte = 0;
        stream.read(&byte, 1);
    }
    result.inputs_open_ms = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - openStart).count();

    std::ostringstream decodeCommand;
    decodeCommand << "ffmpeg -y -hide_banner -loglevel error" << inputsArg
                  << " -t " << duration;
    for (std::size_t i = 0; i < inputs.size(); ++i) decodeCommand << " -map " << i << ":a";
    decodeCommand << " -f null -";
    const auto decode = run(decodeCommand.str());
    addCommandStats(result, decode);
    if (!decode.ok) {
        result.failure_reason = "decode_control_failed";
        return result;
    }
    result.decode_ms = decode.wall_ms;

    std::ostringstream filterCommand;
    filterCommand << "ffmpeg -y -hide_banner -loglevel error" << inputsArg
                  << " -filter_complex " << quotedFilter
                  << " -map \"[aout]\" -t " << duration << " -f null -";
    const auto filtered = run(filterCommand.str());
    addCommandStats(result, filtered);
    if (!filtered.ok) {
        result.failure_reason = "filtergraph_control_failed";
        return result;
    }
    result.filtergraph_ms = std::max(0.0, filtered.wall_ms - decode.wall_ms);

    std::ostringstream pcmCommand;
    pcmCommand << "ffmpeg -y -hide_banner -loglevel error" << inputsArg
               << " -filter_complex " << quotedFilter
               << " -map \"[aout]\" -t " << duration
               << " -c:a pcm_s16le " << file::shellQuote(pcmPath.string());
    const auto pcm = run(pcmCommand.str());
    addCommandStats(result, pcm);
    if (!pcm.ok) {
        result.failure_reason = "pcm_control_failed";
        return result;
    }

    std::ostringstream encodeNullCommand;
    encodeNullCommand << "ffmpeg -y -hide_banner -loglevel error -i "
                      << file::shellQuote(pcmPath.string()) << " -t " << duration
                      << " -c:a aac -f null -";
    const auto encodeNull = run(encodeNullCommand.str());
    addCommandStats(result, encodeNull);
    if (!encodeNull.ok) {
        result.failure_reason = "encode_null_control_failed";
        return result;
    }
    result.encode_ms = encodeNull.wall_ms;

    std::ostringstream encodeFileCommand;
    encodeFileCommand << "ffmpeg -y -hide_banner -loglevel error -i "
                      << file::shellQuote(pcmPath.string()) << " -t " << duration
                      << " -c:a aac " << file::shellQuote(encodedPath.string());
    const auto encodeFile = run(encodeFileCommand.str());
    addCommandStats(result, encodeFile);
    if (!encodeFile.ok) {
        result.failure_reason = "encode_file_control_failed";
        return result;
    }
    result.output_write_ms = std::max(0.0, encodeFile.wall_ms - encodeNull.wall_ms);

    for (const auto& input : inputs) {
        std::error_code ec;
        const auto bytes = fs::file_size(input.path, ec);
        if (!ec) result.input_bytes += bytes;
    }
    {
        std::error_code ec;
        const auto bytes = fs::file_size(encodedPath, ec);
        if (!ec) result.output_bytes = bytes;
    }
    result.ran = true;
    return result;
}

} // namespace velox::audio
