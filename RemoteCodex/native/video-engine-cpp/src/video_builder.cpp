#include "video_builder.hpp"
#include "velox/services/file_utils.hpp"
#include "json_utils.hpp"

namespace fs = std::filesystem;
namespace json = velox::json;
namespace file = velox::file;

namespace velox {

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
}fs::path firstAvailableImage(const SceneRuntime& scene, const fs::path& workDir, size_t index, const std::string& cacheDir) {
	const auto imagePath = workDir / ("scene_" + std::to_string(index) + ".jpg");
	std::vector<std::string> candidates = scene.image_links;
	if (candidates.empty() && !scene.image_link.empty()) {
		candidates.push_back(scene.image_link);
	}
	for (const auto& candidate : candidates) {
		if (file::downloadAsset(candidate, imagePath, cacheDir)) {
			return imagePath;
		}
	}
	return {};
}

fs::path firstAvailableClip(const std::vector<std::string>& candidates, const fs::path& workDir, size_t index, const std::string& cacheDir) {
	const auto clipPath = workDir / ("clip_" + std::to_string(index) + ".mp4");
	for (const auto& candidate : candidates) {
		if (json::trim(candidate).empty()) {
			continue;
		}
		if (file::isDriveFolderUrl(candidate)) {
			continue;
		}
		if (file::downloadAsset(candidate, clipPath, cacheDir)) {
			return clipPath;
		}
	}
	return {};
}

} // namespace velox
