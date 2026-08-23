#include "velox/services/media_utils.hpp"
#include "velox/services/file_utils.hpp"

#include <cmath>
#include <filesystem>
#include <iostream>
#include <sstream>

namespace fs = std::filesystem;

namespace velox::media {
namespace {

std::string escapeJsonString(const std::string& value) {
    std::string output;
    output.reserve(value.size() + 8);
    for (char value_char : value) {
        switch (value_char) {
        case '"': output += "\\\""; break;
        case '\\': output += "\\\\"; break;
        case '\n': output += "\\n"; break;
        case '\r': output += "\\r"; break;
        case '\t': output += "\\t"; break;
        default: output += value_char; break;
        }
    }
    return output;
}

} // namespace

bool buildSceneSegment(
    const fs::path& image_path,
    const fs::path& segment_path,
    double duration,
    const SceneSegmentParams& params) {
    if (!image_path.empty() && fs::exists(image_path)) {
        const std::string args = buildSceneSegmentArgs(
            image_path, segment_path, duration, params);
        const file::CommandResult result = file::runCommandTimed(
            "ffmpeg -y -hide_banner -loglevel error " + args);
        std::cerr << "{\"metric\":\"ffmpeg.scene_segment_ms\",\"value\":"
                  << result.wall_ms << ",\"ok\":" << (result.ok ? "true" : "false")
                  << ",\"exit_code\":" << result.exit_code << "}" << std::endl;
        return result.ok;
    }
    const std::string args = buildColorSegmentArgs(
        segment_path, duration, params, params.color_hex);
    const file::CommandResult result = file::runCommandTimed(
        "ffmpeg -y -hide_banner -loglevel error " + args);
    std::cerr << "{\"metric\":\"ffmpeg.color_segment_ms\",\"value\":"
              << result.wall_ms << ",\"ok\":" << (result.ok ? "true" : "false")
              << ",\"exit_code\":" << result.exit_code << "}" << std::endl;
    return result.ok;
}

bool buildVideoSegment(
    const fs::path& clip_path,
    const fs::path& segment_path,
    double duration,
    const SceneSegmentParams& params) {
    if (clip_path.empty() || !fs::exists(clip_path)) return false;
    const std::string args = buildVideoSegmentArgs(
        clip_path, segment_path, duration, params);
    if (args.empty()) {
        std::cerr << "copy-only media contract rejected video segment: "
                  << clip_path << "\n";
        return false;
    }
    const file::CommandResult result = file::runCommandTimed(
        "ffmpeg -y -hide_banner -loglevel error " + args);
    std::cerr << "{\"metric\":\"ffmpeg.clip_segment_ms\",\"value\":"
              << result.wall_ms << ",\"ok\":" << (result.ok ? "true" : "false")
              << ",\"exit_code\":" << result.exit_code << "}" << std::endl;
    return result.ok;
}

bool concatSegments(
    const std::vector<fs::path>& segments,
    const fs::path& output_path,
    const fs::path& work_dir) {
    const fs::path list_path = work_dir / "segments.txt";
    std::ostringstream list;
    for (const auto& segment : segments) {
        list << "file " << file::shellQuote(segment.string()) << "\n";
    }
    if (!file::writeFile(list_path, list.str())) return false;
    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error -f concat -safe 0 -i "
             << file::shellQuote(list_path.string()) << " -c copy "
             << file::shellQuote(output_path.string());
    const file::CommandResult result = file::runCommandTimed(command.str());
    std::cerr << "{\"metric\":\"ffmpeg.concat_ms\",\"value\":"
              << result.wall_ms << ",\"ok\":" << (result.ok ? "true" : "false")
              << ",\"exit_code\":" << result.exit_code << "}" << std::endl;
    return result.ok;
}

bool muxAudio(
    const fs::path& video_path,
    const fs::path& audio_path,
    const fs::path& output_path,
    double volume,
    double start_offset,
    file::CommandResult* profile,
    bool is_final_mix,
    double expected_duration_seconds,
    FinalAudioDecision* decision_out) {
    FinalAudioDecision decision = resolveFinalAudioMode(
        probeFinalAudioMetadata(audio_path), is_final_mix,
        expected_duration_seconds, volume, start_offset);
    if (decision_out != nullptr) *decision_out = decision;

    std::ostringstream command;
    command << "ffmpeg -y -hide_banner -loglevel error -i "
            << file::shellQuote(video_path.string()) << " -i "
            << file::shellQuote(audio_path.string())
            << " -map 0:v:0 -map 1:a:0 -c:v copy -c:a "
            << (decision.mode == FinalAudioMode::Copy ? "copy" : "aac");

    std::ostringstream audio_filter;
    bool has_filter = false;
    if (volume > 0.0 && volume != 1.0) {
        audio_filter << "volume=" << volume;
        has_filter = true;
    }
    if (start_offset > 0.0) {
        const int delay_ms = static_cast<int>(std::llround(start_offset * 1000.0));
        if (has_filter) audio_filter << ",";
        audio_filter << "adelay=" << delay_ms << "|" << delay_ms;
        has_filter = true;
    }
    if (has_filter) command << " -af " << file::shellQuote(audio_filter.str());

    if (expected_duration_seconds <= 0.0) {
        expected_duration_seconds = probeMediaDurationSeconds(video_path);
    }
    if (expected_duration_seconds > 0.0) {
        command << " -t " << expected_duration_seconds;
    }
    command << " -movflags +faststart " << file::shellQuote(output_path.string());
    const std::string command_text = command.str();
    const file::CommandResult result = file::runCommandTimed(command_text);
    if (profile != nullptr) *profile = result;

    std::error_code video_error;
    std::error_code audio_error;
    std::error_code output_error;
    const auto video_bytes = fs::file_size(video_path, video_error);
    const auto audio_bytes = fs::file_size(audio_path, audio_error);
    const auto output_bytes = fs::file_size(output_path, output_error);
    std::cerr << "{\"metric\":\"ffmpeg.mux_audio_ms\",\"value\":" << result.wall_ms
              << ",\"ok\":" << (result.ok ? "true" : "false")
              << ",\"exit_code\":" << result.exit_code
              << ",\"child_user_ms\":" << result.child_user_ms
              << ",\"child_system_ms\":" << result.child_system_ms
              << ",\"child_max_rss_kb\":" << result.child_max_rss_kb
              << ",\"child_input_blocks\":" << result.child_input_blocks
              << ",\"child_output_blocks\":" << result.child_output_blocks
              << ",\"input_video_bytes\":" << (video_error ? 0 : video_bytes)
              << ",\"input_audio_bytes\":" << (audio_error ? 0 : audio_bytes)
              << ",\"output_bytes\":" << (output_error ? 0 : output_bytes)
              << ",\"final_mux_audio_mode\":\"" << finalAudioModeName(decision.mode) << "\""
              << ",\"final_mux_audio_encode_passes\":"
              << (decision.mode == FinalAudioMode::Copy ? 0 : 1)
              << ",\"audio_metadata_verified\":"
              << (decision.metadata.metadata_verified ? "true" : "false")
              << ",\"audio_codec\":\"" << decision.metadata.codec << "\""
              << ",\"audio_sample_rate\":" << decision.metadata.sample_rate
              << ",\"audio_channels\":" << decision.metadata.channels
              << ",\"audio_channel_layout\":\"" << decision.metadata.channel_layout << "\""
              << ",\"audio_duration_seconds\":" << decision.metadata.duration_seconds
              << ",\"audio_start_time_seconds\":" << decision.metadata.start_time_seconds
              << ",\"audio_format_name\":\"" << decision.metadata.format_name << "\""
              << ",\"audio_extradata_verified\":"
              << (decision.metadata.extradata_verified ? "true" : "false")
              << ",\"audio_container_verified\":"
              << (decision.metadata.container_verified ? "true" : "false")
              << ",\"decision_reason\":\"" << decision.reason << "\""
              << ",\"command\":\"" << escapeJsonString(command_text) << "\"}"
              << std::endl;
    return result.ok;
}

} // namespace velox::media
