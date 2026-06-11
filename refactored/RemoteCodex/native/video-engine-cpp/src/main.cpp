#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

#include "video_builder.hpp"
#include "file_utils.hpp"
#include "json_utils.hpp"
#include "media_utils.hpp"
#include "video_contract.hpp"

namespace fs = std::filesystem;
namespace json = velox::json;
namespace file = velox::file;
namespace media = velox::media;
using velox::parseStringListField;
using velox::parseScenes;
using velox::parseClipSegments;
using velox::firstAvailableClip;
using velox::firstAvailableImage;

namespace {

struct SceneWorkResult {
    size_t index{0};
    fs::path segmentPath;
    bool success{false};
    std::string error;
};

size_t determineSceneWorkerCount(size_t renderCount) {
    if (renderCount <= 1) {
        return renderCount;
    }

    size_t configured = 0;
    if (const char* env = std::getenv("VELOX_SCENE_BUILD_WORKERS")) {
        try {
            const int parsed = std::stoi(env);
            if (parsed > 0) {
                configured = static_cast<size_t>(parsed);
            }
        } catch (...) {
            configured = 0;
        }
    }

    if (configured > 0) {
        return std::max<size_t>(1, std::min(renderCount, configured));
    }

    unsigned int hw = std::thread::hardware_concurrency();
    if (hw == 0) {
        hw = 4;
    }
    const size_t defaultWorkers = std::max<size_t>(2, static_cast<size_t>(hw / 2));
    return std::max<size_t>(1, std::min(renderCount, defaultWorkers));
}

SceneWorkResult buildSceneWorkItem(
    size_t index,
    size_t renderCount,
    const std::vector<std::string>& sceneImagePaths,
    const std::vector<velox::video::SceneAsset>& scenes,
    const fs::path& workDir,
    double perSceneDuration,
    double voiceoverDurationSeconds
) {
    SceneWorkResult result;
    result.index = index;
    result.segmentPath = workDir / ("segment_" + std::to_string(index) + ".mp4");

    fs::path imagePath;
    if (index < sceneImagePaths.size()) {
        const auto imagePathStr = sceneImagePaths[index];
        if (!json::trim(imagePathStr).empty()) {
            const auto candidatePath = workDir / ("scene_" + std::to_string(index) + ".jpg");
            if (file::downloadAsset(imagePathStr, candidatePath)) {
                imagePath = candidatePath;
            }
        }
    } else if (index < scenes.size()) {
        imagePath = firstAvailableImage(scenes[index], workDir, index);
    }

    double duration = 0.0;
    if (perSceneDuration > 0.0) {
        duration = perSceneDuration;
        if (index == renderCount - 1) {
            const double consumed = perSceneDuration * static_cast<double>(renderCount - 1);
            duration = std::max(0.1, voiceoverDurationSeconds - consumed);
        }
    } else if (index < scenes.size() && scenes[index].duration_seconds > 0.0) {
        duration = scenes[index].duration_seconds;
    }

    if (duration <= 0.0) {
        result.error = "no duration available for scene " + std::to_string(index);
        return result;
    }

    if (!media::buildSceneSegment(imagePath, result.segmentPath, duration)) {
        result.error = "failed to build segment " + std::to_string(index);
        return result;
    }

    result.success = true;
    return result;
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
        const size_t workerCount = determineSceneWorkerCount(renderCount);
        std::vector<SceneWorkResult> results(renderCount);
        std::atomic<size_t> nextIndex{0};
        std::vector<std::thread> workers;
        workers.reserve(workerCount);

        for (size_t t = 0; t < workerCount; ++t) {
            workers.emplace_back([&]() {
                while (true) {
                    const size_t index = nextIndex.fetch_add(1);
                    if (index >= renderCount) {
                        break;
                    }
                    results[index] = buildSceneWorkItem(
                        index,
                        renderCount,
                        sceneImagePaths,
                        scenes,
                        workDir,
                        perSceneDuration,
                        voiceoverDurationSeconds
                    );
                }
            });
        }

        for (auto& worker : workers) {
            worker.join();
        }

        for (size_t i = 0; i < renderCount; ++i) {
            if (!results[i].success) {
                std::cerr << "error: " << results[i].error
                          << " (voiceover_duration=" << voiceoverDurationSeconds
                          << ", scene_duration=" << (i < scenes.size() ? scenes[i].duration_seconds : 0.0) << ")\n";
                return 1;
            }
            std::cerr << "scene " << i << " segment built at " << results[i].segmentPath << "\n";
            segments.push_back(results[i].segmentPath);
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
