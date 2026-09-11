#include "render_plan_parser_internal.hpp"
#include "json_utils.hpp"

#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace velox::plan::detail {

namespace ju = velox::json;

namespace {

struct CertifiedAsset {
    std::string asset_id;
    std::string sha256;
    std::string kind;
    std::string mime;
    std::string profile_id;
    std::string timeline_sha256;
    int64_t duration_us{0};
    int64_t frame_count{0};
    int64_t timeline_revision{0};
    int64_t timeline_start_frame{0};
    bool first_frame_keyframe{false};
    bool closed_gop{false};
};

bool canonicalPacketCopyOutput(const std::string& output) {
    return ju::extractJsonStringValue(output, "container") == "mp4" &&
        ju::extractJsonStringValue(output, "video_codec") == "h264" &&
        ju::extractJsonNumberValue(output, "width") == 1920 &&
        ju::extractJsonNumberValue(output, "height") == 1080 &&
        ju::extractJsonNumberValue(output, "fps_num") == 24 &&
        ju::extractJsonNumberValue(output, "fps_den") == 1 &&
        ju::extractJsonStringValue(output, "pixel_format") == "yuv420p" &&
        ju::extractJsonStringValue(output, "profile_id") == "VELOX_ASSEMBLY_READY_V1" &&
        ju::extractJsonStringValue(output, "codec_profile") == "high" &&
        ju::extractJsonStringValue(output, "codec_level") == "4.0" &&
        ju::hasJsonKey(output, "gop_size") &&
        ju::extractJsonNumberValue(output, "gop_size") == 48 &&
        ju::hasJsonKey(output, "b_frames") &&
        ju::extractJsonNumberValue(output, "b_frames") == 0 &&
        ju::hasJsonKey(output, "closed_gop") &&
        ju::extractJsonBoolValue(output, "closed_gop") &&
        ju::extractJsonNumberValue(output, "time_base_num") == 1 &&
        ju::extractJsonNumberValue(output, "time_base_den") == 90000;
}

std::vector<CertifiedAsset> parseAssetCertificates(const std::string& json) {
    std::vector<CertifiedAsset> assets;
    const auto block = ju::extractArrayBlock(json, "assets");
    for (const auto& object : ju::splitTopLevelObjects(block)) {
        CertifiedAsset asset;
        asset.asset_id = ju::extractJsonStringValue(object, "asset_id");
        asset.sha256 = ju::extractJsonStringValue(object, "sha256");
        asset.kind = ju::extractJsonStringValue(object, "kind");
        asset.mime = ju::extractJsonStringValue(object, "mime");
        asset.profile_id = ju::extractJsonStringValue(object, "profile_id");
        asset.timeline_sha256 = ju::extractJsonStringValue(object, "timeline_sha256");
        asset.duration_us = static_cast<int64_t>(
            ju::extractJsonNumberValue(object, "duration_us", 0.0));
        asset.frame_count = static_cast<int64_t>(
            ju::extractJsonNumberValue(object, "frame_count", 0.0));
        asset.timeline_revision = static_cast<int64_t>(
            ju::extractJsonNumberValue(object, "timeline_revision", 0.0));
        asset.timeline_start_frame = static_cast<int64_t>(
            ju::extractJsonNumberValue(object, "timeline_start_frame", -1.0));
        asset.first_frame_keyframe = ju::hasJsonKey(object, "first_frame_keyframe") &&
            ju::extractJsonBoolValue(object, "first_frame_keyframe");
        asset.closed_gop = ju::hasJsonKey(object, "closed_gop") &&
            ju::extractJsonBoolValue(object, "closed_gop");
        if (!asset.asset_id.empty()) assets.push_back(std::move(asset));
    }
    return assets;
}

const CertifiedAsset* findAsset(const std::vector<CertifiedAsset>& assets,
                                const std::string& asset_id) {
    for (const auto& asset : assets) {
        if (asset.asset_id == asset_id) return &asset;
    }
    return nullptr;
}

bool certifiedVideoAsset(const CertifiedAsset* asset,
                         const std::string& segment_sha256,
                         const std::string& timeline_sha256,
                         int64_t timeline_revision,
                         int64_t timeline_start_frame,
                         int64_t frame_count,
                         int64_t duration_us,
                         int64_t source_in_us) {
    return asset != nullptr &&
        (asset->kind == "video" || asset->kind == "prepared_video_fragment") &&
        asset->sha256 == segment_sha256 &&
        asset->profile_id == "VELOX_ASSEMBLY_READY_V1" &&
        asset->frame_count == frame_count &&
        asset->timeline_revision == timeline_revision &&
        asset->timeline_sha256 == timeline_sha256 &&
        asset->timeline_start_frame == timeline_start_frame &&
        asset->duration_us == duration_us &&
        asset->first_frame_keyframe && asset->closed_gop && source_in_us == 0;
}

} // namespace

std::optional<RenderPlan> parseRenderPlanV2(
    const std::string& jsonStr,
    RenderPlan plan) {
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
    plan.canvas.fps = static_cast<int>(std::lround(
        static_cast<double>(plan.canvas.fps_num) /
        static_cast<double>(plan.canvas.fps_den)));
    if (plan.canvas.fps <= 0) {
        std::cerr << "errore: CompiledRenderPlanV2 output frame rate is invalid\n";
        return std::nullopt;
    }

    const bool canonical_output = canonicalPacketCopyOutput(outputBlock);
    const std::string timeline_sha256 = ju::extractJsonStringValue(
        jsonStr, "timeline_sha256");
    const int64_t timeline_revision = static_cast<int64_t>(
        ju::extractJsonNumberValue(jsonStr, "timeline_revision", 0.0));
    const auto certified_assets = parseAssetCertificates(jsonStr);

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

    // Fail-closed backstop (same contract as the V1 parser): subtitle
    // burn-in is retired; a plan carrying subtitle_tracks is rejected.
    if (!ju::extractArrayBlock(jsonStr, "subtitle_tracks").empty()) {
        std::cerr
            << "subtitle_tracks are not supported by the video renderer: "
            << "rejecting RenderPlan\n";
        return std::nullopt;
    }
    if (ju::hasJsonKey(jsonStr, "layers")) {
        std::cerr << "layers are not supported by the native video renderer: "
                     "use the Chronon compositor backend; rejecting RenderPlan\n";
        return std::nullopt;
    }

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
        item.metadata_certified = canonical_output &&
            ju::hasJsonKey(jsonStr, "timeline_revision") &&
            !timeline_sha256.empty() && ju::hasJsonKey(segmentStr, "sha256") &&
            certifiedVideoAsset(
            findAsset(certified_assets, assetId),
            ju::extractJsonStringValue(segmentStr, "sha256"), timeline_sha256,
            timeline_revision, item.timeline_start_frame, item.frame_count,
            item.source_duration_us, item.source_in_us);
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
        const CertifiedAsset* audio_asset = findAsset(certified_assets, assetId);
        track.metadata_certified = canonical_output &&
            ju::hasJsonKey(audioBlock, "sha256") &&
            ju::hasJsonKey(audioBlock, "codec") &&
            ju::hasJsonKey(audioBlock, "sample_rate_hz") &&
            ju::hasJsonKey(audioBlock, "channels") &&
            ju::hasJsonKey(audioBlock, "timeline_revision") &&
            ju::hasJsonKey(audioBlock, "timeline_sha256") &&
            ju::extractJsonStringValue(audioBlock, "codec") == "aac" &&
            ju::extractJsonNumberValue(audioBlock, "sample_rate_hz") == 48000 &&
            ju::extractJsonNumberValue(audioBlock, "channels") == 2 &&
            ju::extractJsonNumberValue(audioBlock, "timeline_revision") == timeline_revision &&
            ju::extractJsonStringValue(audioBlock, "timeline_sha256") == timeline_sha256 &&
            audio_asset != nullptr && audio_asset->kind == "final_audio" &&
            !audio_asset->sha256.empty() &&
            audio_asset->mime == "audio/mp4" &&
            audio_asset->sha256 == ju::extractJsonStringValue(audioBlock, "sha256") &&
            audio_asset->duration_us == audioDurationUS;
        plan.audio_tracks.push_back(std::move(track));
    }

    return plan;
}

} // namespace velox::plan::detail
