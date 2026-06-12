#pragma once

#include <string>
#include <vector>

namespace velox::video {

// SceneAsset: una singola scena nel request JSON.
// Corrisponde a contract.SceneRequest in Go (shared/contract/contract.go).
struct SceneAsset {
    std::string text;
    std::string image_link;
    std::vector<std::string> image_links;
    double duration_seconds{0.0};
};

// ClipAsset: un segmento clip nel request JSON.
// Corrisponde a contract.ClipRequest in Go (shared/contract/contract.go).
struct ClipAsset {
    std::string text;
    std::string clip_link;
    std::vector<std::string> clip_links;
    double duration_seconds{4.0};
    std::string kind;
};

// SceneVideoRequest: l'intero request JSON per il video engine.
// Corrisponde a contract.VideoEngineRequest in Go (shared/contract/contract.go).
struct SceneVideoRequest {
    std::string job_id;
    std::string video_name;
    std::string script_text;
    std::vector<std::string> voiceover_paths;
    std::vector<SceneAsset> scenes;
    std::string scenes_json;
    std::string output_path;
    std::string video_mode;
    std::vector<std::string> intro_clip_paths;
    std::vector<std::string> stock_clip_paths;
    std::string clip_segments_json;
    std::string drive_output_folder;
    std::string audio_language_for_srt;
};

// SceneVideoResult: risultato dell'elaborazione video.
// Non ha un corrispettivo Go diretto (usato internamente dal C++ engine).
struct SceneVideoResult {
    std::string job_id;
    std::string output_path;
    bool success{false};
    std::string error;
};

}  // namespace velox::video
