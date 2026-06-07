#include <algorithm>
#include <cctype>
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

std::string normalizeDriveUrl(const std::string& url) {
    std::smatch match;
    if (std::regex_search(url, match, std::regex(R"(/file/d/([^/]+))"))) {
        return "https://drive.google.com/uc?export=download&id=" + match[1].str();
    }
    return url;
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
    const auto url = normalizeDriveUrl(source);
    std::string cmd = "curl -L --fail --silent --show-error -o " + shellQuote(dest.string()) + " " + shellQuote(url);
    return runCommand(cmd);
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

bool buildSceneSegment(const fs::path& imagePath, const fs::path& segmentPath, double duration) {
    std::ostringstream cmd;
    cmd << "ffmpeg -y ";
    if (!imagePath.empty() && fs::exists(imagePath)) {
        cmd << "-loop 1 -t " << duration << " -i " << shellQuote(imagePath.string())
            << " -vf " << shellQuote("scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p")
            << " -c:v libx264 -pix_fmt yuv420p -r 30 " << shellQuote(segmentPath.string());
    } else {
        cmd << "-f lavfi -t " << duration
            << " -i " << shellQuote("color=c=black:s=1280x720")
            << " -c:v libx264 -pix_fmt yuv420p -r 30 " << shellQuote(segmentPath.string());
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
    const auto voiceoverPaths = extractArrayStrings(requestJson, "voiceover_paths");
    const auto scenes = parseScenes(requestJson);

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

    std::vector<fs::path> segments;
    segments.reserve(std::max<size_t>(1, scenes.size()));
    for (size_t i = 0; i < std::max<size_t>(1, scenes.size()); ++i) {
        fs::path imagePath;
        if (i < scenes.size()) {
            imagePath = firstAvailableImage(scenes[i], workDir, i);
        }
        fs::path segmentPath = workDir / ("segment_" + std::to_string(i) + ".mp4");
        const double duration = i < scenes.size() ? scenes[i].duration_seconds : 5.0;
        if (!buildSceneSegment(imagePath, segmentPath, duration)) {
            std::cerr << "failed to build segment " << i << "\n";
            return 1;
        }
        segments.push_back(segmentPath);
    }

    fs::path videoOnlyPath = workDir / "video_only.mp4";
    if (!concatSegments(segments, videoOnlyPath, workDir)) {
        std::cerr << "failed to concat segments\n";
        return 1;
    }

    fs::path finalOutput = outputPath;
    if (!voiceoverPaths.empty()) {
        fs::path audioPath = workDir / "voiceover_audio";
        bool downloaded = false;
        for (const auto& candidate : voiceoverPaths) {
            if (downloadAsset(candidate, audioPath)) {
                downloaded = true;
                break;
            }
        }
        if (downloaded) {
            fs::path muxedOutput = workDir / "final_with_audio.mp4";
            if (!muxAudio(videoOnlyPath, audioPath, muxedOutput)) {
                std::error_code ec;
                fs::copy_file(videoOnlyPath, finalOutput, fs::copy_options::overwrite_existing, ec);
            } else {
                std::error_code ec;
                fs::copy_file(muxedOutput, finalOutput, fs::copy_options::overwrite_existing, ec);
            }
        } else {
            std::error_code ec;
            fs::copy_file(videoOnlyPath, finalOutput, fs::copy_options::overwrite_existing, ec);
        }
    } else {
        std::error_code ec;
        fs::copy_file(videoOnlyPath, finalOutput, fs::copy_options::overwrite_existing, ec);
    }

    std::cout << "{\"success\":true,\"job_id\":\"" << jobId << "\",\"output_path\":\"" << finalOutput.string()
              << "\",\"video_name\":\"" << videoName << "\",\"audio_language_for_srt\":\"" << audioLanguage << "\"}" << std::endl;
    return 0;
}
