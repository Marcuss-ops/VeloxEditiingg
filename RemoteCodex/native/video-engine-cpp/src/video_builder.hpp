#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "video_contract.hpp"

namespace velox {

// Le strutture SceneAsset/ClipAsset sono definite in video_contract.hpp (namespace velox::video)
// e corrispondono a contract.SceneRequest/ClipRequest in Go (shared/contract/contract.go).
// Qui usiamo alias per comodità nel namespace velox.
using SceneRuntime = velox::video::SceneAsset;
using ClipRuntime = velox::video::ClipAsset;

double extractDurationValue(const std::string& json, const std::string& key, double fallback);
ClipRuntime parseClipObject(const std::string& obj);
std::vector<SceneRuntime> parseScenes(const std::string& requestJson);
std::vector<ClipRuntime> parseClipSegments(const std::string& requestJson);
std::vector<std::string> parseStringListField(const std::string& requestJson, const std::string& key);
std::filesystem::path firstAvailableImage(const SceneRuntime& scene, const std::filesystem::path& workDir, size_t index);
std::filesystem::path firstAvailableClip(const std::vector<std::string>& candidates, const std::filesystem::path& workDir, size_t index);

} // namespace velox
