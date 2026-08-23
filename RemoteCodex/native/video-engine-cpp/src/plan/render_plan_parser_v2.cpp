#include "render_plan_parser_internal.hpp"
#include "json_utils.hpp"

#include <cstdint>
#include <iostream>

namespace velox::plan::detail {

std::optional<RenderPlan> parseRenderPlanV2(
    const std::string& jsonStr,
    RenderPlan plan) {
    namespace ju = velox::json;

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
    plan.canvas.fps = plan.canvas.fps_den == 1
        ? plan.canvas.fps_num
        : (plan.canvas.fps_num % plan.canvas.fps_den == 0
               ? plan.canvas.fps_num / plan.canvas.fps_den
               : plan.canvas.fps_num);

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
        std::cerr << "errore: CompiledRenderPlanV2 multiple video_tracks are not "
                     "supported by the copy-only packet pipeline\n";
        return std::nullopt;
    }
    const std::string segmentsBlock = ju::extractArrayBlock(trackObjects.front(), "segments");
    if (segmentsBlock.empty()) {
        std::cerr << "errore: CompiledRenderPlanV2 track requires segments\n";
        return std::nullopt;
    }

    const int64_t one_frame_us =
        (static_cast<int64_t>(plan.canvas.fps_den) * 1'000'000LL +
         plan.canvas.fps_num - 1) / plan.canvas.fps_num;
    if (one_frame_us <= 0) {
        std::cerr << "errore: CompiledRenderPlanV2 output fps_num/fps_den produce "
                     "an invalid frame duration\n";
        return std::nullopt;
    }
    const int64_t max_frames = INT64_MAX /
        (1'000'000LL * plan.canvas.fps_den);

    int64_t expected_start_frame = 0;
    int64_t running_frames = 0;
    for (const auto& segmentStr : ju::splitTopLevelObjects(segmentsBlock)) {
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

    const int64_t total_frames_us = (running_frames *
        static_cast<int64_t>(plan.canvas.fps_den) * 1'000'000LL) / plan.canvas.fps_num;
    const int64_t frame_tolerance = duration_us > total_frames_us
        ? duration_us - total_frames_us
        : total_frames_us - duration_us;
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

} // namespace velox::plan::detail
