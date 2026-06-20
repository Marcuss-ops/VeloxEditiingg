#include "velox/plan/render_plan_parser.hpp"
#include "json_utils.hpp"
#include <iostream>

namespace velox::plan {

std::optional<RenderPlan> parseRenderPlan(const std::string& jsonStr) {
    namespace ju = velox::json;

    RenderPlan plan;
    
    // Versione
    plan.version = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "version", 0.0));
    if (plan.version != 1) {
        std::cerr << "errore: versione del piano non supportata o mancante: " << plan.version << "\n";
        return std::nullopt;
    }

    // Job ID e Output Path
    plan.job_id = ju::extractJsonStringValue(jsonStr, "job_id");
    plan.output_path = ju::extractJsonStringValue(jsonStr, "output_path");

    if (plan.job_id.empty() || plan.output_path.empty()) {
        std::cerr << "errore: job_id o output_path mancanti nel RenderPlan\n";
        return std::nullopt;
    }

    // Canvas
    // Poiché canvas è un oggetto nidificato, lo estraiamo come sotto-blocco (usiamo extractArrayBlock per analogia di profondità o parsing semplice)
    // Per semplicità facciamo estrazione diretta o estraiamo le chiavi globalmente sapendo che appartengono a canvas
    // (le utility json correnti cercano regex globali, quindi extractJsonNumberValue su width/height/fps funzionerà finché non ci sono duplicati)
    plan.canvas.width = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "width", 1920.0));
    plan.canvas.height = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "height", 1080.0));
    plan.canvas.fps = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "fps", 30.0));

    // Timeline
    std::string timelineBlock = ju::extractArrayBlock(jsonStr, "timeline");
    if (!timelineBlock.empty()) {
        for (const auto& itemStr : ju::splitTopLevelObjects(timelineBlock)) {
            TimelineItem item;
            item.duration_seconds = ju::extractJsonNumberValue(itemStr, "duration_seconds", 0.0);
            
            // Transform
            item.transform.scale_mode = ju::extractJsonStringValue(itemStr, "scale_mode");
            if (item.transform.scale_mode.empty()) {
                item.transform.scale_mode = "cover";
            }
            // Per i booleani, controlliamo se la stringa contiene "true"
            std::string kbStr = ju::extractJsonString(itemStr, "ken_burns_effect"); // usiamo extractJsonString per prenderlo grezzo
            // Se non trova con string, proviamo ad estrarre grezzo o facciamo fallback
            item.transform.ken_burns_effect = (itemStr.find("\"ken_burns_effect\"\\s*:\\s*true") != std::string::npos || 
                                                itemStr.find("\"ken_burns_effect\":true") != std::string::npos);

            // Source
            std::string sourceBlock = ju::extractArrayBlock(itemStr, "source");
            if (sourceBlock.empty()) {
                // Se non è un array ma un oggetto, extractArrayBlock non funzionerà se non trova '['.
                // Le utility esistenti non hanno "extractObjectBlock". Implementiamo una ricerca semplice dell'oggetto "source":
                size_t sPos = itemStr.find("\"source\"");
                if (sPos != std::string::npos) {
                    size_t startBrace = itemStr.find('{', sPos);
                    if (startBrace != std::string::npos) {
                        int depth = 0;
                        for (size_t k = startBrace; k < itemStr.size(); ++k) {
                            if (itemStr[k] == '{') depth++;
                            else if (itemStr[k] == '}') {
                                depth--;
                                if (depth == 0) {
                                    sourceBlock = itemStr.substr(startBrace, k - startBrace + 1);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            std::string sourceType = ju::extractJsonStringValue(sourceBlock.empty() ? itemStr : sourceBlock, "type");
            std::string url = ju::extractJsonStringValue(sourceBlock.empty() ? itemStr : sourceBlock, "url");
            std::string cacheKey = ju::extractJsonStringValue(sourceBlock.empty() ? itemStr : sourceBlock, "cache_key");
            std::string colorHex = ju::extractJsonStringValue(sourceBlock.empty() ? itemStr : sourceBlock, "color_hex");

            if (sourceType == "image") {
                item.source = ImageSource{url, cacheKey};
            } else if (sourceType == "video") {
                item.source = VideoSource{url, cacheKey};
            } else if (sourceType == "color") {
                item.source = ColorSource{colorHex};
            } else {
                std::cerr << "warning: tipo sorgente sconosciuto: " << sourceType << "\n";
                continue;
            }
            plan.timeline.push_back(item);
        }
    }

    // Audio tracks
    std::string audioBlock = ju::extractArrayBlock(jsonStr, "audio_tracks");
    if (!audioBlock.empty()) {
        for (const auto& audioStr : ju::splitTopLevelObjects(audioBlock)) {
            AudioTrack track;
            track.source_url = ju::extractJsonStringValue(audioStr, "source_url");
            track.volume = ju::extractJsonNumberValue(audioStr, "volume", 1.0);
            track.start_time_offset = ju::extractJsonNumberValue(audioStr, "start_time_offset", 0.0);
            if (!track.source_url.empty()) {
                plan.audio_tracks.push_back(track);
            }
        }
    }

    return plan;
}

} // namespace velox::plan
