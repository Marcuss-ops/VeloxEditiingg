#include "velox/plan/render_plan_parser.hpp"
#include "json_utils.hpp"
#include <cstdint>
#include <iostream>
#include <regex>
#include <vector>

namespace velox::plan {

namespace {

// Local mirror of velox::json::unescapeJsonString.
std::string ju_unescape(std::string s) {
    std::string out;
    out.reserve(s.size());
    bool escape = false;
    for (char c : s) {
        if (escape) {
            switch (c) {
                case 'n': out.push_back('\n'); break;
                case 't': out.push_back('\t'); break;
                case 'r': out.push_back('\r'); break;
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                default: out.push_back(c); break;
            }
            escape = false;
            continue;
        }
        if (c == '\\') {
            escape = true;
            continue;
        }
        out.push_back(c);
    }
    return out;
}

// Extracts the balanced JSON object block for a named key: the first
// `"key"` followed by `{ ... }`. Mirrors extractArrayBlock but for objects.
std::string extractObjectBlock(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) {
        return {};
    }
    pos = json.find('{', pos);
    if (pos == std::string::npos) {
        return {};
    }
    int depth = 0;
    bool inString = false;
    bool escape = false;
    for (size_t i = pos; i < json.size(); ++i) {
        char c = json[i];
        if (inString) {
            if (escape) {
                escape = false;
                continue;
            }
            if (c == '\\') {
                escape = true;
                continue;
            }
            if (c == '"') {
                inString = false;
            }
            continue;
        }
        if (c == '"') {
            inString = true;
            continue;
        }
        if (c == '{') {
            ++depth;
        } else if (c == '}') {
            --depth;
            if (depth == 0) {
                return json.substr(pos, i - pos + 1);
            }
        }
    }
    return {};
}

// Escapes regex metacharacters in a producer-controlled asset id before it
// is embedded in the bindings lookup pattern.
std::string regexEscape(const std::string& value) {
    static const std::string specials = "\\^$.|?*+()[]{}";
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
        if (specials.find(c) != std::string::npos) {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    return out;
}

// Looks up `"asset_id":"path"` inside a bindings object block. The V2
// document stays path-free; the worker injects the resolved local paths in
// a runtime bindings map so libavformat can open them in place.
std::string bindingPathFor(const std::string& bindingsBlock, const std::string& assetId) {
    if (bindingsBlock.empty() || assetId.empty()) {
        return {};
    }
    const std::regex re("\"" + regexEscape(assetId) + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
    std::smatch match;
    if (std::regex_search(bindingsBlock, match, re) && match.size() > 1) {
        return ju_unescape(match[1].str());
    }
    return {};
}

} // namespace

std::optional<RenderPlan> parseRenderPlan(const std::string& jsonStr) {
    namespace ju = velox::json;

    RenderPlan plan;

    // Versione: V1 uses "version"; CompiledRenderPlanV2 uses "plan_version".
    plan.version = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "plan_version", 0.0));
    if (plan.version == 0) {
        plan.version = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "version", 0.0));
    }
    if (plan.version != kRenderPlanVersionV1 && plan.version != kRenderPlanVersionV2) {
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

    if (plan.version == kRenderPlanVersionV2) {
        // ── CompiledRenderPlanV2 ─────────────────────────────────────────
        // The V2 document carries integer timeline placement (frames) and
        // source trimming (microseconds). Floating seconds are forbidden:
        // the parser fails closed when a V1 float timing key is present.
        // Runtime asset paths never belong in the canonical V2 document;
        // they arrive in a worker-injected "bindings" object.

        const std::string outputBlock = extractObjectBlock(jsonStr, "output");
        if (outputBlock.empty()) {
            std::cerr << "errore: CompiledRenderPlanV2 requires an output block\n";
            return std::nullopt;
        }
        plan.canvas.width = static_cast<int>(ju::extractJsonNumberValue(outputBlock, "width", 0.0));
        plan.canvas.height = static_cast<int>(ju::extractJsonNumberValue(outputBlock, "height", 0.0));
        plan.canvas.fps_num = static_cast<int>(ju::extractJsonNumberValue(outputBlock, "fps_num", 0.0));
        plan.canvas.fps_den = static_cast<int>(ju::extractJsonNumberValue(outputBlock, "fps_den", 0.0));
        if (plan.canvas.width <= 0 || plan.canvas.height <= 0 ||
            plan.canvas.fps_num <= 0 || plan.canvas.fps_den <= 0) {
            std::cerr << "errore: CompiledRenderPlanV2 output must carry positive "
                         "width/height/fps_num/fps_den\n";
            return std::nullopt;
        }
        // fps = fps_num / fps_den as an integer when divisible (the common
        // CFR case); otherwise the exact rational pair is kept and the frame
        // math below uses the rational form so no float is ever consulted.
        plan.canvas.fps = plan.canvas.fps_den == 1
            ? plan.canvas.fps_num
            : (plan.canvas.fps_num % plan.canvas.fps_den == 0
                   ? plan.canvas.fps_num / plan.canvas.fps_den
                   : plan.canvas.fps_num); // non-CFR; fps field unused by V2

        const int64_t duration_us = static_cast<int64_t>(
            ju::extractJsonNumberValue(jsonStr, "duration_us", 0.0));
        if (duration_us <= 0) {
            std::cerr << "errore: CompiledRenderPlanV2 requires positive duration_us\n";
            return std::nullopt;
        }

        const std::string bindingsBlock = extractObjectBlock(jsonStr, "bindings");
        if (bindingsBlock.empty()) {
            std::cerr << "errore: CompiledRenderPlanV2 requires a bindings object "
                         "(asset_id -> local path)\n";
            return std::nullopt;
        }

        // copy-only is the only contract the in-process packet pipeline
        // implements; V2 documents are frame-exact by construction and never
        // request a re-encode pass.
        plan.copy_only = true;

        const std::string tracksBlock = ju::extractArrayBlock(jsonStr, "video_tracks");
        if (tracksBlock.empty()) {
            std::cerr << "errore: CompiledRenderPlanV2 requires video_tracks\n";
            return std::nullopt;
        }
        const auto trackObjects = ju::splitTopLevelObjects(tracksBlock);
        if (trackObjects.empty()) {
            std::cerr << "errore: CompiledRenderPlanV2 requires at least one video track\n";
            return std::nullopt;
        }
        if (trackObjects.size() > 1) {
            // The copy-only packet pipeline is single-track by construction;
            // silently dropping extra tracks would be semantic guessing.
            std::cerr << "errore: CompiledRenderPlanV2 multiple video_tracks are not "
                         "supported by the copy-only packet pipeline\n";
            return std::nullopt;
        }
        const std::string segmentsBlock = ju::extractArrayBlock(trackObjects.front(), "segments");
        if (segmentsBlock.empty()) {
            std::cerr << "errore: CompiledRenderPlanV2 track requires segments\n";
            return std::nullopt;
        }

        // One frame in microseconds, rounded UP so the tolerance is never
        // stricter than a true frame for rational frame rates (e.g. 30000/1001).
        const int64_t one_frame_us =
            (static_cast<int64_t>(plan.canvas.fps_den) * 1'000'000LL +
             plan.canvas.fps_num - 1) / plan.canvas.fps_num;
        if (one_frame_us <= 0) {
            std::cerr << "errore: CompiledRenderPlanV2 output fps_num/fps_den produce "
                         "an invalid frame duration\n";
            return std::nullopt;
        }

        // The Go validation guards against int64 overflow in the frame sums;
        // mirror it here before any multiplication (signed overflow is UB).
        const int64_t max_frames = INT64_MAX / (1'000'000LL * plan.canvas.fps_den);

        int64_t expected_start_frame = 0;
        int64_t running_frames = 0;
        for (const auto& segmentStr : ju::splitTopLevelObjects(segmentsBlock)) {
            // Float seconds are forbidden in V2 (no semantic guessing, no
            // float source of truth).
            if (ju::extractJsonNumberValue(segmentStr, "duration_seconds", 0.0) != 0.0) {
                std::cerr << "errore: CompiledRenderPlanV2 segment must not carry "
                             "float duration_seconds\n";
                return std::nullopt;
            }
            TimelineItem item;
            item.scene_id = ju::extractJsonStringValue(segmentStr, "segment_id");
            item.timeline_start_frame = static_cast<int64_t>(
                ju::extractJsonNumberValue(segmentStr, "timeline_start_frame", -1.0));
            item.frame_count = static_cast<int64_t>(
                ju::extractJsonNumberValue(segmentStr, "frame_count", 0.0));
            item.source_in_us = static_cast<int64_t>(
                ju::extractJsonNumberValue(segmentStr, "source_in_us", -1.0));
            item.source_duration_us = static_cast<int64_t>(
                ju::extractJsonNumberValue(segmentStr, "source_duration_us", 0.0));
            const std::string assetId = ju::extractJsonStringValue(segmentStr, "asset_id");
            if (item.timeline_start_frame < 0 || item.frame_count <= 0 ||
                item.source_in_us < 0 || item.source_duration_us <= 0 || assetId.empty()) {
                std::cerr << "errore: CompiledRenderPlanV2 segment requires non-negative "
                             "timeline_start_frame/source_in_us and positive "
                             "frame_count/source_duration_us/asset_id\n";
                return std::nullopt;
            }

            // Frame placement must be contiguous for a copy-only pipeline:
            // the packet muxer appends segments sequentially, so gaps or
            // overlaps would silently shift the timeline. Reject them
            // instead of guessing.
            if (item.timeline_start_frame != expected_start_frame) {
                std::cerr << "errore: CompiledRenderPlanV2 segment \"" << item.scene_id
                          << "\" timeline_start_frame=" << item.timeline_start_frame
                          << " is not contiguous (expected " << expected_start_frame << ")\n";
                return std::nullopt;
            }
            if (item.frame_count > max_frames ||
                expected_start_frame > INT64_MAX - item.frame_count) {
                std::cerr << "errore: CompiledRenderPlanV2 segment \"" << item.scene_id
                          << "\" frame counts overflow int64\n";
                return std::nullopt;
            }
            expected_start_frame += item.frame_count;
            running_frames += item.frame_count;

            // Frame-exactness: source_duration_us must match frame_count at
            // the rational output rate within one frame (the same tolerance
            // the Go compiler enforces). The mux trim uses the integer
            // source_duration_us; the frame math must agree to keep the
            // timeline deterministic.
            const int64_t frame_us_num = item.frame_count *
                static_cast<int64_t>(plan.canvas.fps_den) * 1'000'000LL;
            const int64_t frame_us = frame_us_num / plan.canvas.fps_num;
            const int64_t diff = item.source_duration_us > frame_us
                ? item.source_duration_us - frame_us
                : frame_us - item.source_duration_us;
            if (diff > one_frame_us) {
                std::cerr << "errore: CompiledRenderPlanV2 segment \"" << item.scene_id
                          << "\" source_duration_us=" << item.source_duration_us
                          << " does not match frame_count=" << item.frame_count
                          << " at " << plan.canvas.fps_num << "/" << plan.canvas.fps_den
                          << " fps (frame_us=" << frame_us << ")\n";
                return std::nullopt;
            }

            item.duration_us = item.source_duration_us;
            const std::string path = bindingPathFor(bindingsBlock, assetId);
            if (path.empty()) {
                std::cerr << "errore: CompiledRenderPlanV2 asset \"" << assetId
                          << "\" has no binding path\n";
                return std::nullopt;
            }
            item.source = VideoSource{path, ""};
            plan.timeline.push_back(std::move(item));
        }

        // The declared total must agree with the running frame sum (again in
        // exact integers) — reject drift instead of repairing it.
        const int64_t total_frames_us = (running_frames *
            static_cast<int64_t>(plan.canvas.fps_den) * 1'000'000LL) / plan.canvas.fps_num;
        const int64_t frame_tolerance = duration_us > total_frames_us
            ? duration_us - total_frames_us : total_frames_us - duration_us;
        if (frame_tolerance > one_frame_us) {
            std::cerr << "errore: CompiledRenderPlanV2 duration_us=" << duration_us
                      << " does not match the segment frame sum " << total_frames_us
                      << "\n";
            return std::nullopt;
        }

        const std::string audioBlock = extractObjectBlock(jsonStr, "final_audio");
        if (!audioBlock.empty()) {
            if (ju::extractJsonNumberValue(audioBlock, "duration_seconds", 0.0) != 0.0 ||
                ju::extractJsonNumberValue(audioBlock, "start_time_offset", 0.0) != 0.0) {
                std::cerr << "errore: CompiledRenderPlanV2 final_audio must not carry "
                             "float timing\n";
                return std::nullopt;
            }
            const std::string mode = ju::extractJsonStringValue(audioBlock, "mode");
            if (mode != "FINAL_AUDIO_COPY") {
                std::cerr << "errore: CompiledRenderPlanV2 final_audio mode must be "
                             "FINAL_AUDIO_COPY, got \"" << mode << "\"\n";
                return std::nullopt;
            }
            const std::string assetId = ju::extractJsonStringValue(audioBlock, "asset_id");
            const int64_t audioDurationUS = static_cast<int64_t>(
                ju::extractJsonNumberValue(audioBlock, "duration_us", 0.0));
            const std::string path = bindingPathFor(bindingsBlock, assetId);
            if (assetId.empty() || path.empty() || audioDurationUS <= 0) {
                std::cerr << "errore: CompiledRenderPlanV2 final_audio requires "
                             "asset_id with a binding and positive duration_us\n";
                return std::nullopt;
            }
            AudioTrack track;
            track.source_url = path;
            track.volume = 1.0;
            track.start_offset_us = 0;
            track.duration_us = audioDurationUS;
            track.role = "music";
            track.loop = false;
            plan.audio_tracks.push_back(std::move(track));
        }

        return plan;
    }

    // ── RenderPlan V1 (legacy floating-second contract) ────────────────
    plan.canvas.width = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "width", 1920.0));
    plan.canvas.height = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "height", 1080.0));
    plan.canvas.fps = static_cast<int>(ju::extractJsonNumberValue(jsonStr, "fps", 30.0));
    plan.copy_only = ju::extractJsonBoolValue(jsonStr, "copy_only", false);
    plan.watermark_already_applied = ju::extractJsonBoolValue(jsonStr, "watermark_already_applied", false);
    plan.watermark_requested = ju::extractJsonBoolValue(jsonStr, "watermark_requested", false);
    plan.mixed = ju::extractJsonBoolValue(jsonStr, "mixed", false);

    // Timeline
    std::string timelineBlock = ju::extractArrayBlock(jsonStr, "timeline");
    if (!timelineBlock.empty()) {
        for (const auto& itemStr : ju::splitTopLevelObjects(timelineBlock)) {
            TimelineItem item;
            item.scene_id = ju::extractJsonStringValue(itemStr, "scene_id");
            item.duration_seconds = ju::extractJsonNumberValue(itemStr, "duration_seconds", 0.0);
            item.include_audio = ju::extractJsonBoolValue(itemStr, "include_audio", false);

            // Transform
            item.transform.scale_mode = ju::extractJsonStringValue(itemStr, "scale_mode");
            if (item.transform.scale_mode.empty()) {
                item.transform.scale_mode = "cover";
            }
            item.transform.slow_zoom = ju::extractJsonBoolValue(itemStr, "slow_zoom", true);

            // Source
            std::string sourceBlock = ju::extractArrayBlock(itemStr, "source");
            if (sourceBlock.empty()) {
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
            track.duration_seconds = ju::extractJsonNumberValue(audioStr, "duration_seconds", 0.0);
            track.role = ju::extractJsonStringValue(audioStr, "role");
            track.loop = ju::extractJsonBoolValue(audioStr, "loop", false);
            if (!track.source_url.empty()) {
                plan.audio_tracks.push_back(track);
            }
        }
    }

    // Subtitle tracks
    std::string subtitleBlock = ju::extractArrayBlock(jsonStr, "subtitle_tracks");
    if (!subtitleBlock.empty()) {
        for (const auto& subtitleStr : ju::splitTopLevelObjects(subtitleBlock)) {
            SubtitleTrack track;
            track.source = ju::extractJsonStringValue(subtitleStr, "source");
            track.preset = ju::extractJsonStringValue(subtitleStr, "preset");
            track.font = ju::extractJsonStringValue(subtitleStr, "font");
            if (!track.source.empty()) {
                plan.subtitle_tracks.push_back(track);
            }
        }
    }

    return plan;
}

} // namespace velox::plan
