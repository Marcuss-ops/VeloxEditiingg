#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "file_utils.hpp"
#include "json_utils.hpp"
#include "media_utils.hpp"
#include "video_contract.hpp"

namespace fs = std::filesystem;
namespace json = velox::json;
namespace file = velox::file;
namespace media = velox::media;

namespace {

struct SceneRuntime {
    std::string text;
    std::string image_link;
    std::vector<std::string> image_links;
    double duration_seconds{0.0};
};

struct ClipRuntime {
    std::string text;
    std::string clip_link;
    std::vector<std::string> clip_links;
    double duration_seconds{0.0};
    std::string kind;
};

double extractDurationValue(const std::string& json, const std::string& key, double fallback) {
    return json::extractJsonNumberValue(json, key, fallback);
}

ClipRuntime parseClipObject(const std::string& obj) {
    ClipRuntime clip;
    clip.text = json::extractJsonStringValue(obj, "text");
    clip.clip_link = json::extractJsonStringValue(obj, "clip_link");
    clip.clip_links = json::extractArrayStrings(obj, "clip_links");
    clip.duration_seconds = extractDurationValue(obj, "duration_seconds", 0.0);
    clip.kind = json::extractJsonStringValue(obj, "kind");
    if (clip.clip_link.empty() && !clip.clip_links.empty()) {
        clip.clip_link = clip.clip_links.front();
    }
    if (clip.clip_links.empty() && !clip.clip_link.empty()) {
        clip.clip_links.push_back(clip.clip_link);
    }
    return clip;
}

std::vector<SceneRuntime> parseScenes(const std::string& requestJson) {
    std::vector<SceneRuntime> scenes;
    auto arrayBlock = json::extractArrayBlock(requestJson, "scenes");
    if (arrayBlock.empty()) {
        return scenes;
    }
    for (const auto& obj : json::splitTopLevelObjects(arrayBlock)) {
        SceneRuntime scene;
        scene.text = json::extractJsonStringValue(obj, "text");
        scene.image_link = json::extractJsonStringValue(obj, "image_link");
        scene.image_links = json::extractArrayStrings(obj, "image_links");
        scene.duration_seconds = json::extractJsonNumberValue(obj, "duration_seconds", 0.0);
        if (scene.image_link.empty() && !scene.image_links.empty()) {
            scene.image_link = scene.image_links.front();
        }
        if (scene.image_links.empty() && !scene.image_link.empty()) {
            scene.image_links.push_back(scene.image_link);
        }
        scenes.push_back(scene);
    }
    return scenes;
}

std::vector<ClipRuntime> parseClipSegments(const std::string& requestJson) {
    std::vector<ClipRuntime> clips;
    auto arrayBlock = json::extractArrayBlock(requestJson, "clip_segments");
    if (arrayBlock.empty()) {
        arrayBlock = json::extractArrayBlock(requestJson, "segments");
    }
    if (arrayBlock.empty()) {
        return clips;
    }
    for (const auto& obj : json::splitTopLevelObjects(arrayBlock)) {
        clips.push_back(parseClipObject(obj));
    }
    return clips;
}

std::vector<std::string> parseStringListField(const std::string& requestJson, const std::string& key) {
    auto values = json::extractArrayStrings(requestJson, key);
    if (!values.empty()) {
        return values;
    }
    auto raw = json::extractJsonStringValue(requestJson, key);
    if (!raw.empty()) {
        return {raw};
    }
    return {};
}

fs::path firstAvailableImage(const SceneRuntime& scene, const fs::path& workDir, size_t index) {
    const auto imagePath = workDir / ("scene_" + std::to_string(index) + ".jpg");
    std::vector<std::string> candidates = scene.image_links;
    if (candidates.empty() && !scene.image_link.empty()) {
        candidates.push_back(scene.image_link);
    }
    for (const auto& candidate : candidates) {
        if (file::downloadAsset(candidate, imagePath)) {
            return imagePath;
        }
    }
    return {};
}

fs::path firstAvailableClip(const std::vector<std::string>& candidates, const fs::path& workDir, size_t index) {
    const auto clipPath = workDir / ("clip_" + std::to_string(index) + ".mp4");
    for (const auto& candidate : candidates) {
        if (json::trim(candidate).empty()) {
            continue;
        }
        if (file::isDriveFolderUrl(candidate)) {
            continue;
        }
        if (file::downloadAsset(candidate, clipPath)) {
            return clipPath;
        }
    }
    return {};
}

} // namespace

int main(int argc, char** argv) {
    std::string requestPath;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--request" && i + 1 < argc) {
            requestPath = argv[++i];
        }
    }

    if (requestPath.empty()) {
        std::cerr << "missing --request argument\n";
        return 1;
    }

    const auto requestJson = file::readFile(requestPath);
    if (requestJson.empty()) {
        std::cerr << "failed to read request file\n";
        return 1;
    }

    const auto outputPathStr = json::trim(json::extractJsonStringValue(requestJson, "output_path"));
    const auto scriptText = json::extractJsonStringValue(requestJson, "script_text");
    const auto videoName = json::extractJsonStringValue(requestJson, "video_name");
    const auto audioLanguage = json::extractJsonStringValue(requestJson, "audio_language_for_srt");
    const auto jobId = json::extractJsonStringValue(requestJson, "job_id");
    const auto videoMode = json::trim(json::extractJsonStringValue(requestJson, "video_mode"));
    const auto driveOutputFolder = json::trim(json::extractJsonStringValue(requestJson, "drive_output_folder"));
    const auto voiceoverPaths = json::extractArrayStrings(requestJson, "voiceover_paths");
    const auto sceneImagePaths = parseStringListField(requestJson, "scene_image_paths");
    const auto introClipPaths = parseStringListField(requestJson, "intro_clip_paths");
    auto stockClipPaths = parseStringListField(requestJson, "stock_clip_paths");
    if (stockClipPaths.empty()) {
        stockClipPaths = parseStringListField(requestJson, "stock_clip_sources");
    }
    const auto scenes = parseScenes(requestJson);
    const auto clipSegments = parseClipSegments(requestJson);
    double voiceoverDurationSeconds = 0.0;
    fs::path downloadedVoiceoverPath;

    if (outputPathStr.empty()) {
        std::cerr << "missing output_path in request\n";
        return 1;
    }

    fs::path outputPath(outputPathStr);
    fs::create_directories(outputPath.parent_path());

    fs::path workBase = fs::temp_directory_path() / "velox_video_engine";
    fs::path workDir = file::makeTempDir(workBase, "job_");
    if (workDir.empty()) {
        std::cerr << "failed to create temp work dir\n";
        return 1;
    }

    const bool clipMode = videoMode == "clip_stock"
        || !clipSegments.empty()
        || !introClipPaths.empty()
        || !stockClipPaths.empty();

    if (!voiceoverPaths.empty()) {
        fs::path audioPath = workDir / "voiceover_audio";
        bool downloaded = false;
        for (const auto& candidate : voiceoverPaths) {
            if (file::downloadAsset(candidate, audioPath)) {
                downloaded = true;
                downloadedVoiceoverPath = audioPath;
                break;
            }
        }
        if (!downloaded) {
            std::cerr << "failed to download voiceover audio\n";
            return 1;
        }
        voiceoverDurationSeconds = media::probeMediaDurationSeconds(downloadedVoiceoverPath);
    }

    std::vector<fs::path> segments;
    if (clipMode) {
        size_t segmentIndex = 0;
        for (size_t i = 0; i < introClipPaths.size(); ++i) {
            std::vector<std::string> candidates = {introClipPaths[i]};
            fs::path clipPath = firstAvailableClip(candidates, workDir, segmentIndex);
            if (clipPath.empty()) {
                std::cerr << "failed to resolve intro clip segment " << i << "\n";
                return 1;
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(segmentIndex) + ".mp4");
            if (!media::buildVideoSegment(clipPath, segmentPath, 4.0)) {
                std::cerr << "failed to build intro clip segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
            ++segmentIndex;
        }

        for (size_t i = 0; i < clipSegments.size(); ++i) {
            const auto& clip = clipSegments[i];
            std::vector<std::string> candidates = clip.clip_links;
            if (candidates.empty() && !clip.clip_link.empty()) {
                candidates.push_back(clip.clip_link);
            }
            fs::path clipPath = firstAvailableClip(candidates, workDir, segmentIndex);
            if (clipPath.empty()) {
                std::cerr << "failed to resolve clip segment " << i << "\n";
                return 1;
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(segmentIndex) + ".mp4");
            if (!media::buildVideoSegment(clipPath, segmentPath, clip.duration_seconds > 0.0 ? clip.duration_seconds : 4.0)) {
                std::cerr << "failed to build clip segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
            ++segmentIndex;
        }

        for (size_t i = 0; i < stockClipPaths.size(); ++i) {
            std::vector<std::string> candidates = {stockClipPaths[i]};
            fs::path clipPath = firstAvailableClip(candidates, workDir, segmentIndex);
            if (clipPath.empty()) {
                std::cerr << "failed to resolve stock clip segment " << i << "\n";
                return 1;
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(segmentIndex) + ".mp4");
            if (!media::buildVideoSegment(clipPath, segmentPath, 5.0)) {
                std::cerr << "failed to build stock clip segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
            ++segmentIndex;
        }
    } else {
        const size_t renderCount = !sceneImagePaths.empty()
            ? sceneImagePaths.size()
            : std::max<size_t>(1, scenes.size());
        segments.reserve(renderCount);

        double perSceneDuration = 0.0;
        if (voiceoverDurationSeconds > 0.0 && renderCount > 0) {
            perSceneDuration = voiceoverDurationSeconds / static_cast<double>(renderCount);
            std::cerr << "voiceover_duration=" << voiceoverDurationSeconds << "s, scenes=" << renderCount << ", per_scene=" << perSceneDuration << "s\n";
        }

        for (size_t i = 0; i < renderCount; ++i) {
            fs::path imagePath;
            if (i < sceneImagePaths.size()) {
                const auto imagePathStr = sceneImagePaths[i];
                if (!json::trim(imagePathStr).empty()) {
                    const auto candidatePath = workDir / ("scene_" + std::to_string(i) + ".jpg");
                    if (file::downloadAsset(imagePathStr, candidatePath)) {
                        imagePath = candidatePath;
                    }
                }
            } else if (i < scenes.size()) {
                imagePath = firstAvailableImage(scenes[i], workDir, i);
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(i) + ".mp4");

            double duration = 0.0;
            if (perSceneDuration > 0.0) {
                duration = perSceneDuration;
                if (i == renderCount - 1) {
                    const double consumed = perSceneDuration * static_cast<double>(renderCount - 1);
                    duration = std::max(0.1, voiceoverDurationSeconds - consumed);
                }
            } else if (i < scenes.size() && scenes[i].duration_seconds > 0.0) {
                duration = scenes[i].duration_seconds;
            }

            if (duration <= 0.0) {
                std::cerr << "error: no duration available for scene " << i
                          << " (voiceover_duration=" << voiceoverDurationSeconds
                          << ", scene_duration=" << (i < scenes.size() ? scenes[i].duration_seconds : 0.0) << ")\n";
                return 1;
            }

            std::cerr << "scene " << i << " duration=" << duration << "s\n";
            if (!media::buildSceneSegment(imagePath, segmentPath, duration)) {
                std::cerr << "failed to build segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
        }
    }

    fs::path videoOnlyPath = workDir / "video_only.mp4";
    if (!media::concatSegments(segments, videoOnlyPath, workDir)) {
        std::cerr << "failed to concat segments\n";
        return 1;
    }

    fs::path finalOutput = outputPath;
    if (!voiceoverPaths.empty()) {
        fs::path audioPath = downloadedVoiceoverPath.empty() ? workDir / "voiceover_audio" : downloadedVoiceoverPath;
        fs::path muxedOutput = workDir / "final_with_audio.mp4";
        if (!media::muxAudio(videoOnlyPath, audioPath, muxedOutput)) {
            std::cerr << "failed to mux audio into final video\n";
            return 1;
        }
        std::error_code ec;
        fs::copy_file(muxedOutput, finalOutput, fs::copy_options::overwrite_existing, ec);
    } else {
        std::error_code ec;
        fs::copy_file(videoOnlyPath, finalOutput, fs::copy_options::overwrite_existing, ec);
    }

    std::cout << "{\"success\":true,\"job_id\":\"" << jobId << "\",\"output_path\":\"" << finalOutput.string()
              << "\",\"video_name\":\"" << videoName << "\",\"audio_language_for_srt\":\"" << audioLanguage
              << "\",\"video_mode\":\"" << (clipMode ? "clip_stock" : "scene_image")
              << "\",\"audio_duration_seconds\":" << voiceoverDurationSeconds << "}" << std::endl;
    if (!driveOutputFolder.empty()) {
        std::cerr << "drive_output_folder_hint=" << driveOutputFolder << "\n";
    }
    return 0;
}
