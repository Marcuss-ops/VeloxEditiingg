#include <algorithm>
#include <array>
#include <cmath>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "video_contract.hpp"

namespace fs = std::filesystem;

namespace {

struct SceneRuntime {
    std::string text;
    std::string image_link;
    std::vector<std::string> image_links;
    double duration_seconds{5.0};
};

struct ClipRuntime {
    std::string text;
    std::string clip_link;
    std::vector<std::string> clip_links;
    double duration_seconds{4.0};
    std::string kind;
};

std::string readFile(const fs::path& path) {
    std::ifstream in(path);
    if (!in) {
        return {};
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

bool writeFile(const fs::path& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        return false;
    }
    out << content;
    return static_cast<bool>(out);
}

std::string trim(std::string s) {
    auto notSpace = [](unsigned char c) { return !std::isspace(c); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
    s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
    return s;
}

std::string shellQuote(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') {
            out += "'\"'\"'";
        } else {
            out.push_back(c);
        }
    }
    out.push_back('\'');
    return out;
}

bool runCommand(const std::string& cmd) {
    int rc = std::system(cmd.c_str());
    return rc == 0;
}

std::string captureCommandOutput(const std::string& cmd) {
    std::array<char, 4096> buffer{};
    std::string output;
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        return {};
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        output.append(buffer.data());
    }
    pclose(pipe);
    return output;
}

std::string extractJsonString(const std::string& json, const std::string& key) {
    const std::regex re("\"" + key + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
    std::smatch match;
    if (std::regex_search(json, match, re) && match.size() > 1) {
        return match[1].str();
    }
    return {};
}

std::string unescapeJsonString(std::string s) {
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

std::string extractJsonStringValue(const std::string& json, const std::string& key) {
    return unescapeJsonString(extractJsonString(json, key));
}

double extractJsonNumberValue(const std::string& json, const std::string& key, double fallback = 0.0) {
    const std::regex re("\"" + key + "\"\\s*:\\s*([-+]?[0-9]*\\.?[0-9]+)");
    std::smatch match;
    if (std::regex_search(json, match, re) && match.size() > 1) {
        try {
            return std::stod(match[1].str());
        } catch (...) {
            return fallback;
        }
    }
    return fallback;
}

std::string extractArrayBlock(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) {
        return {};
    }
    pos = json.find('[', pos);
    if (pos == std::string::npos) {
        return {};
    }
    int depth = 0;
    for (size_t i = pos; i < json.size(); ++i) {
        char c = json[i];
        if (c == '"') {
            ++i;
            bool escape = false;
            for (; i < json.size(); ++i) {
                char cc = json[i];
                if (escape) {
                    escape = false;
                    continue;
                }
                if (cc == '\\') {
                    escape = true;
                    continue;
                }
                if (cc == '"') {
                    break;
                }
            }
            continue;
        }
        if (c == '[') {
            ++depth;
        } else if (c == ']') {
            --depth;
            if (depth == 0) {
                return json.substr(pos, i - pos + 1);
            }
        }
    }
    return {};
}

std::vector<std::string> extractArrayStrings(const std::string& json, const std::string& key) {
    std::vector<std::string> values;
    auto block = extractArrayBlock(json, key);
    if (block.empty()) {
        return values;
    }
    const std::regex re("\"((?:\\\\.|[^\"])*)\"");
    for (std::sregex_iterator it(block.begin(), block.end(), re), end; it != end; ++it) {
        if (it->size() > 1) {
            values.push_back(unescapeJsonString((*it)[1].str()));
        }
    }
    return values;
}

std::vector<std::string> splitTopLevelObjects(const std::string& arrayBlock) {
    std::vector<std::string> objects;
    if (arrayBlock.size() < 2 || arrayBlock.front() != '[') {
        return objects;
    }

    bool inString = false;
    bool escape = false;
    int depth = 0;
    size_t objStart = std::string::npos;

    for (size_t i = 1; i < arrayBlock.size() - 1; ++i) {
        char c = arrayBlock[i];
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
            if (depth == 0) {
                objStart = i;
            }
            ++depth;
        } else if (c == '}') {
            --depth;
            if (depth == 0 && objStart != std::string::npos) {
                objects.push_back(arrayBlock.substr(objStart, i - objStart + 1));
                objStart = std::string::npos;
            }
        }
    }
    return objects;
}

double extractDurationValue(const std::string& json, const std::string& key, double fallback) {
    return extractJsonNumberValue(json, key, fallback);
}

ClipRuntime parseClipObject(const std::string& obj) {
    ClipRuntime clip;
    clip.text = extractJsonStringValue(obj, "text");
    clip.clip_link = extractJsonStringValue(obj, "clip_link");
    clip.clip_links = extractArrayStrings(obj, "clip_links");
    clip.duration_seconds = extractDurationValue(obj, "duration_seconds", 4.0);
    if (clip.duration_seconds <= 0.0) {
        clip.duration_seconds = 4.0;
    }
    clip.kind = extractJsonStringValue(obj, "kind");
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
    auto arrayBlock = extractArrayBlock(requestJson, "scenes");
    if (arrayBlock.empty()) {
        return scenes;
    }
    for (const auto& obj : splitTopLevelObjects(arrayBlock)) {
        SceneRuntime scene;
        scene.text = extractJsonStringValue(obj, "text");
        scene.image_link = extractJsonStringValue(obj, "image_link");
        scene.image_links = extractArrayStrings(obj, "image_links");
        scene.duration_seconds = extractJsonNumberValue(obj, "duration_seconds", 5.0);
        if (scene.duration_seconds <= 0.0) {
            scene.duration_seconds = 5.0;
        }
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
    auto arrayBlock = extractArrayBlock(requestJson, "clip_segments");
    if (arrayBlock.empty()) {
        arrayBlock = extractArrayBlock(requestJson, "segments");
    }
    if (arrayBlock.empty()) {
        return clips;
    }
    for (const auto& obj : splitTopLevelObjects(arrayBlock)) {
        clips.push_back(parseClipObject(obj));
    }
    return clips;
}

std::vector<std::string> parseStringListField(const std::string& requestJson, const std::string& key) {
    auto values = extractArrayStrings(requestJson, key);
    if (!values.empty()) {
        return values;
    }
    auto raw = extractJsonStringValue(requestJson, key);
    if (!raw.empty()) {
        return {raw};
    }
    return {};
}

std::string normalizeDriveUrl(const std::string& url) {
    std::smatch match;
    if (std::regex_search(url, match, std::regex(R"(/file/d/([^/]+))"))) {
        return "https://drive.google.com/uc?export=download&id=" + match[1].str();
    }
    return url;
}

bool isDriveFolderUrl(const std::string& url) {
    return url.find("/drive/folders/") != std::string::npos;
}

std::string resolveDriveFolderToFileUrl(const std::string& folderUrl) {
    if (!isDriveFolderUrl(folderUrl)) {
        return folderUrl;
    }

    const std::string html = captureCommandOutput("curl -L --silent --show-error " + shellQuote(folderUrl));
    if (html.empty()) {
        return {};
    }

    const std::regex fileViewRe(R"(https://drive\.google\.com/file/d/([^"/?]+))");
    std::smatch match;
    if (std::regex_search(html, match, fileViewRe) && match.size() > 1) {
        return normalizeDriveUrl(match[0].str());
    }

    const std::regex fileIdRe(R"(/file/d/([^"/?]+))");
    if (std::regex_search(html, match, fileIdRe) && match.size() > 1) {
        return "https://drive.google.com/uc?export=download&id=" + match[1].str();
    }

    const std::regex openIdRe(R"(open\?id=([^"&]+))");
    if (std::regex_search(html, match, openIdRe) && match.size() > 1) {
        return "https://drive.google.com/uc?export=download&id=" + match[1].str();
    }

    return {};
}

bool copyFile(const fs::path& src, const fs::path& dst) {
    std::error_code ec;
    fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
    return !ec;
}

fs::path makeTempDir(const fs::path& base, const std::string& prefix) {
    fs::create_directories(base);
    for (int i = 0; i < 100; ++i) {
        auto candidate = base / (prefix + std::to_string(std::rand()));
        std::error_code ec;
        if (fs::create_directory(candidate, ec) && !ec) {
            return candidate;
        }
    }
    return {};
}

bool downloadAsset(const std::string& source, const fs::path& dest) {
    if (source.empty()) {
        return false;
    }
    if (fs::exists(source)) {
        return copyFile(source, dest);
    }
    std::string resolvedSource = source;
    if (isDriveFolderUrl(source)) {
        resolvedSource = resolveDriveFolderToFileUrl(source);
        if (resolvedSource.empty()) {
            return false;
        }
    }
    const auto url = normalizeDriveUrl(resolvedSource);
    std::string cmd = "curl -L --fail --silent --show-error -o " + shellQuote(dest.string()) + " " + shellQuote(url);
    return runCommand(cmd);
}

double probeMediaDurationSeconds(const fs::path& mediaPath) {
    if (mediaPath.empty() || !fs::exists(mediaPath)) {
        return 0.0;
    }
    std::ostringstream cmd;
    cmd << "ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "
        << shellQuote(mediaPath.string());
    const std::string output = trim(captureCommandOutput(cmd.str()));
    if (output.empty() || output == "N/A") {
        return 0.0;
    }
    try {
        const double duration = std::stod(output);
        return duration > 0.0 ? duration : 0.0;
    } catch (...) {
        return 0.0;
    }
}

fs::path firstAvailableImage(const SceneRuntime& scene, const fs::path& workDir, size_t index) {
    const auto imagePath = workDir / ("scene_" + std::to_string(index) + ".jpg");
    std::vector<std::string> candidates = scene.image_links;
    if (candidates.empty() && !scene.image_link.empty()) {
        candidates.push_back(scene.image_link);
    }
    for (const auto& candidate : candidates) {
        if (downloadAsset(candidate, imagePath)) {
            return imagePath;
        }
    }
    return {};
}

fs::path firstAvailableClip(const std::vector<std::string>& candidates, const fs::path& workDir, size_t index) {
    const auto clipPath = workDir / ("clip_" + std::to_string(index) + ".mp4");
    for (const auto& candidate : candidates) {
        if (trim(candidate).empty()) {
            continue;
        }
        if (isDriveFolderUrl(candidate)) {
            continue;
        }
        if (downloadAsset(candidate, clipPath)) {
            return clipPath;
        }
    }
    return {};
}

bool buildSceneSegment(const fs::path& imagePath, const fs::path& segmentPath, double duration) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y ";
    if (!imagePath.empty() && fs::exists(imagePath)) {
        const int fps = 30;
        const int frames = std::max(1, static_cast<int>(std::round(duration * fps)));
        const std::string filter =
            "scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,"
            "zoompan=z='min(zoom+0.0008,1.10)':d=" + std::to_string(frames) + ":s=1920x1080:fps=30,"
            "format=yuv420p";
        cmd << "-loop 1 -i " << shellQuote(imagePath.string())
            << " -vf " << shellQuote(filter)
            << " -frames:v " << frames
            << " -c:v libx264 -pix_fmt yuv420p -r 30 " << shellQuote(segmentPath.string());
    } else {
        cmd << "-f lavfi -t " << duration
            << " -i " << shellQuote("color=c=black:s=1920x1080")
            << " -c:v libx264 -pix_fmt yuv420p -r 30 " << shellQuote(segmentPath.string());
    }
    return runCommand(cmd.str());
}

bool buildVideoSegment(const fs::path& clipPath, const fs::path& segmentPath, double duration) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y ";
    if (!clipPath.empty() && fs::exists(clipPath)) {
        cmd << "-i " << shellQuote(clipPath.string())
            << " -t " << duration
            << " -vf " << shellQuote("scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p")
            << " -c:v libx264 -pix_fmt yuv420p -r 30 -an " << shellQuote(segmentPath.string());
    } else {
        return false;
    }
    return runCommand(cmd.str());
}

bool concatSegments(const std::vector<fs::path>& segments, const fs::path& outputPath, const fs::path& workDir) {
    auto listPath = workDir / "segments.txt";
    std::ostringstream list;
    for (const auto& segment : segments) {
        list << "file " << shellQuote(segment.string()) << "\n";
    }
    if (!writeFile(listPath, list.str())) {
        return false;
    }
    std::ostringstream cmd;
    cmd << "ffmpeg -y -f concat -safe 0 -i " << shellQuote(listPath.string())
        << " -c copy " << shellQuote(outputPath.string());
    return runCommand(cmd.str());
}

bool muxAudio(const fs::path& videoPath, const fs::path& audioPath, const fs::path& outputPath) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y -i " << shellQuote(videoPath.string())
        << " -i " << shellQuote(audioPath.string())
        << " -c:v copy -c:a aac -shortest " << shellQuote(outputPath.string());
    return runCommand(cmd.str());
}

}  // namespace

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

    const auto requestJson = readFile(requestPath);
    if (requestJson.empty()) {
        std::cerr << "failed to read request file\n";
        return 1;
    }

    const auto outputPathStr = trim(extractJsonStringValue(requestJson, "output_path"));
    const auto scriptText = extractJsonStringValue(requestJson, "script_text");
    const auto videoName = extractJsonStringValue(requestJson, "video_name");
    const auto audioLanguage = extractJsonStringValue(requestJson, "audio_language_for_srt");
    const auto jobId = extractJsonStringValue(requestJson, "job_id");
    const auto videoMode = trim(extractJsonStringValue(requestJson, "video_mode"));
    const auto driveOutputFolder = trim(extractJsonStringValue(requestJson, "drive_output_folder"));
    const auto voiceoverPaths = extractArrayStrings(requestJson, "voiceover_paths");
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
    fs::path workDir = makeTempDir(workBase, "job_");
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
            if (downloadAsset(candidate, audioPath)) {
                downloaded = true;
                downloadedVoiceoverPath = audioPath;
                break;
            }
        }
        if (!downloaded) {
            std::cerr << "failed to download voiceover audio\n";
            return 1;
        }
        voiceoverDurationSeconds = probeMediaDurationSeconds(downloadedVoiceoverPath);
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
            if (!buildVideoSegment(clipPath, segmentPath, 4.0)) {
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
            if (!buildVideoSegment(clipPath, segmentPath, clip.duration_seconds > 0.0 ? clip.duration_seconds : 4.0)) {
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
            if (!buildVideoSegment(clipPath, segmentPath, 5.0)) {
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
        const double sceneDurationOverride =
            (voiceoverDurationSeconds > 0.0 && renderCount > 0)
                ? (voiceoverDurationSeconds / static_cast<double>(renderCount))
                : 0.0;
        for (size_t i = 0; i < renderCount; ++i) {
            fs::path imagePath;
            if (i < sceneImagePaths.size()) {
                const auto imagePathStr = sceneImagePaths[i];
                if (!trim(imagePathStr).empty()) {
                    const auto candidatePath = workDir / ("scene_" + std::to_string(i) + ".jpg");
                    if (downloadAsset(imagePathStr, candidatePath)) {
                        imagePath = candidatePath;
                    }
                }
            } else if (i < scenes.size()) {
                imagePath = firstAvailableImage(scenes[i], workDir, i);
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(i) + ".mp4");
            double duration = i < scenes.size() ? scenes[i].duration_seconds : 5.0;
            if (sceneDurationOverride > 0.0) {
                duration = sceneDurationOverride;
                if (i == renderCount - 1) {
                    const double consumed = sceneDurationOverride * static_cast<double>(renderCount - 1);
                    duration = std::max(0.1, voiceoverDurationSeconds - consumed);
                }
            }
            if (!buildSceneSegment(imagePath, segmentPath, duration)) {
                std::cerr << "failed to build segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
        }
    }

    fs::path videoOnlyPath = workDir / "video_only.mp4";
    if (!concatSegments(segments, videoOnlyPath, workDir)) {
        std::cerr << "failed to concat segments\n";
        return 1;
    }

    fs::path finalOutput = outputPath;
    if (!voiceoverPaths.empty()) {
        fs::path audioPath = downloadedVoiceoverPath.empty() ? workDir / "voiceover_audio" : downloadedVoiceoverPath;
        fs::path muxedOutput = workDir / "final_with_audio.mp4";
        if (!muxAudio(videoOnlyPath, audioPath, muxedOutput)) {
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
