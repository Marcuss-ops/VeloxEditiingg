#pragma once

#include <string>
#include <vector>

namespace velox::video {

struct SceneAsset {
    std::string text;
    std::string image_link;
    std::vector<std::string> image_links;
};

struct SceneVideoRequest {
    std::string job_id;
    std::string video_name;
    std::string script_text;
    std::vector<std::string> voiceover_paths;
    std::vector<SceneAsset> scenes;
    std::string scenes_json;
    std::string output_path;
    std::string audio_language_for_srt;
};

struct SceneVideoResult {
    std::string job_id;
    std::string output_path;
    bool success{false};
    std::string error;
};

}  // namespace velox::video

