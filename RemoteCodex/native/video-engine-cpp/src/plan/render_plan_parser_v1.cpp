#include "render_plan_parser_internal.hpp"
#include "json_utils.hpp"

#include <iostream>

namespace velox::plan::detail {

std::optional<RenderPlan> parseRenderPlanV1(
    const std::string& jsonStr,
    RenderPlan plan) {
    namespace ju = velox::json;

    plan.canvas.width = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "width", 1920.0));
    plan.canvas.height = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "height", 1080.0));
    plan.canvas.fps = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "fps", 24.0));
    plan.copy_only = ju::extractJsonBoolValue(jsonStr, "copy_only", false);
    plan.watermark_already_applied = ju::extractJsonBoolValue(jsonStr, "watermark_already_applied", false);
    plan.watermark_requested = ju::extractJsonBoolValue(jsonStr, "watermark_requested", false);
    plan.mixed = ju::extractJsonBoolValue(jsonStr, "mixed", false);

    const std::string timelineBlock = ju::extractArrayBlock(jsonStr, "timeline");
    if (!timelineBlock.empty()) {
        for (const auto& itemStr : ju::splitTopLevelObjects(timelineBlock)) {
            TimelineItem item;
            item.scene_id = ju::extractJsonStringValue(itemStr, "scene_id");
            item.duration_seconds = ju::extractJsonNumberValue(itemStr, "duration_seconds", 0.0);
            item.include_audio = ju::extractJsonBoolValue(itemStr, "include_audio", false);
            item.transform.scale_mode = ju::extractJsonStringValue(itemStr, "scale_mode");
            if (item.transform.scale_mode.empty()) item.transform.scale_mode = "cover";
            item.transform.explicit_request =
                itemStr.find("\"slow_zoom\"") != std::string::npos ||
                itemStr.find("\"scale_mode\"") != std::string::npos;
            item.transform.slow_zoom = ju::extractJsonBoolValue(
                itemStr, "slow_zoom", !plan.copy_only);

            std::string sourceBlock = ju::extractArrayBlock(itemStr, "source");
            if (sourceBlock.empty()) {
                const size_t sourcePos = itemStr.find("\"source\"");
                if (sourcePos != std::string::npos) {
                    const size_t startBrace = itemStr.find('{', sourcePos);
                    if (startBrace != std::string::npos) {
                        int depth = 0;
                        for (size_t k = startBrace; k < itemStr.size(); ++k) {
                            if (itemStr[k] == '{') ++depth;
                            else if (itemStr[k] == '}' && --depth == 0) {
                                sourceBlock = itemStr.substr(startBrace, k - startBrace + 1);
                                break;
                            }
                        }
                    }
                }
            }

            const std::string& sourceJson = sourceBlock.empty() ? itemStr : sourceBlock;
            const std::string sourceType = ju::extractJsonStringValue(sourceJson, "type");
            const std::string url = ju::extractJsonStringValue(sourceJson, "url");
            const std::string cacheKey = ju::extractJsonStringValue(sourceJson, "cache_key");
            const std::string colorHex = ju::extractJsonStringValue(sourceJson, "color_hex");
            if (sourceType == "image") item.source = ImageSource{url, cacheKey};
            else if (sourceType == "video") item.source = VideoSource{url, cacheKey};
            else if (sourceType == "color") item.source = ColorSource{colorHex};
            else {
                std::cerr << "warning: tipo sorgente sconosciuto: " << sourceType << "\n";
                continue;
            }
            plan.timeline.push_back(std::move(item));
        }
    }

    const std::string audioBlock = ju::extractArrayBlock(jsonStr, "audio_tracks");
    if (!audioBlock.empty()) {
        for (const auto& audioStr : ju::splitTopLevelObjects(audioBlock)) {
            AudioTrack track;
            track.source_url = ju::extractJsonStringValue(audioStr, "source_url");
            track.volume = ju::extractJsonNumberValue(audioStr, "volume", 1.0);
            track.start_time_offset = ju::extractJsonNumberValue(audioStr, "start_time_offset", 0.0);
            track.duration_seconds = ju::extractJsonNumberValue(audioStr, "duration_seconds", 0.0);
            track.role = ju::extractJsonStringValue(audioStr, "role");
            track.loop = ju::extractJsonBoolValue(audioStr, "loop", false);
            if (!track.source_url.empty()) plan.audio_tracks.push_back(std::move(track));
        }
    }

    const std::string subtitleBlock = ju::extractArrayBlock(jsonStr, "subtitle_tracks");
    if (!subtitleBlock.empty()) {
        for (const auto& subtitleStr : ju::splitTopLevelObjects(subtitleBlock)) {
            SubtitleTrack track;
            track.source = ju::extractJsonStringValue(subtitleStr, "source");
            track.preset = ju::extractJsonStringValue(subtitleStr, "preset");
            track.font = ju::extractJsonStringValue(subtitleStr, "font");
            if (!track.source.empty()) plan.subtitle_tracks.push_back(std::move(track));
        }
    }

    return plan;
}

} // namespace velox::plan::detail
