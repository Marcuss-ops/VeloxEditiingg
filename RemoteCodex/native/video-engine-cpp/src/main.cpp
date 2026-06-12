// Velox Video Engine — CLI tool per elaborazione video.
//
// Sotto-comandi disponibili (eseguibili singolarmente):
//
//   --full-video --request <path>
//       Pipeline completa: scarica asset, costruisce segmenti, concatena, muxa audio.
//       (Comportamento originale — usa tutti i blocchi internamente.)
//
//   --download-asset --url <url> --dest <path>
//       Scarica un asset (audio/immagine/video) da URL (supporta Google Drive).
//
//   --probe-media <path>
//       Stampa la durata in secondi di un file multimediale (stdout JSON).
//
//   --build-scene-segment --image <path> --duration <sec> --out <path>
//       Genera un segmento video da un'immagine con effetto zoompan.
//
//   --build-clip-segment --clip <path> --duration <sec> --out <path>
//       Genera un segmento video da un clip video (scalato/croppato).
//
//   --concat-segments --list <file> --out <path>
//       Concatena segmenti video usando un file lista (formato concat demuxer).
//
//   --mux-audio --video <path> --audio <path> --out <path>
//       Muxa una traccia audio su un video.
//
//   --help
//       Mostra questa guida.
//
// Ogni sotto-comando stampa JSON su stdout e log su stderr.
// Errori portano a exit code 1.

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
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

// ──────────────────────────────────────────────
// Help
// ──────────────────────────────────────────────

static void printUsage(const char* prog) {
    std::cerr << "Velox Video Engine — CLI tool per elaborazione video\n"
              << "\n"
              << "Utilizzo: " << prog << " <sotto-comando> [opzioni]\n"
              << "\n"
              << "Sotto-comandi:\n"
              << "\n"
              << "  --full-video --request <path>\n"
              << "      Pipeline completa: scarica asset, costruisce segmenti,\n"
              << "      concatena, muxa audio.\n"
              << "\n"
              << "  --download-asset --url <url> --dest <path>\n"
              << "      Scarica un asset da URL (supporta Google Drive).\n"
              << "\n"
              << "  --probe-media <path>\n"
              << "      Stampa la durata in secondi (stdout JSON).\n"
              << "\n"
              << "  --build-scene-segment --image <path> --duration <s> --out <path>\n"
              << "      Genera segmento video da immagine (zoompan).\n"
              << "\n"
              << "  --build-clip-segment --clip <path> --duration <s> --out <path>\n"
              << "      Genera segmento video da clip (scalato/croppato).\n"
              << "\n"
              << "  --concat-segments --list <file> --out <path>\n"
              << "      Concatena segmenti video usando file lista.\n"
              << "\n"
              << "  --mux-audio --video <path> --audio <path> --out <path>\n"
              << "      Muxa audio su video.\n"
              << "\n"
              << "  --help\n"
              << "      Mostra questa guida.\n"
              << std::endl;
}

// ──────────────────────────────────────────────
// Sotto-comando: --help
// ──────────────────────────────────────────────

static int cmdHelp(const char* prog) {
    printUsage(prog);
    return 0;
}

// ──────────────────────────────────────────────
// Sotto-comando: --download-asset
// ──────────────────────────────────────────────

static std::string escapeJsonString(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 4);
    for (char c : s) {
        if (c == '"')       out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c == '\n')  out += "\\n";
        else if (c == '\t')  out += "\\t";
        else if (c == '\r')  out += "\\r";
        else                 out += c;
    }
    return out;
}

static int cmdDownloadAsset(int argc, char** argv) {
    std::string url, dest;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--url" && i + 1 < argc) url = argv[++i];
        else if (arg == "--dest" && i + 1 < argc) dest = argv[++i];
    }
    if (url.empty()) {
        std::cerr << "errore: --url richiesto\n";
        return 1;
    }
    if (dest.empty()) {
        std::cerr << "errore: --dest richiesto\n";
        return 1;
    }
    bool ok = file::downloadAsset(url, fs::path(dest));
    std::cout << "{\"success\":"
              << (ok ? "true" : "false")
              << ",\"url\":\"" << escapeJsonString(url)
              << "\",\"dest\":\"" << escapeJsonString(dest) << "\"}" << std::endl;
    return ok ? 0 : 1;
}

// ──────────────────────────────────────────────
// Sotto-comando: --probe-media
// ──────────────────────────────────────────────

static int cmdProbeMedia(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "errore: --probe-media richiede un path\n";
        return 1;
    }
    fs::path mediaPath(argv[2]);
    double duration = media::probeMediaDurationSeconds(mediaPath);
    if (duration <= 0.0) {
        std::cerr << "errore: impossibile rilevare durata per " << mediaPath << "\n";
        std::cout << "{\"success\":false,\"path\":\"" << mediaPath.string()
                  << "\",\"duration_seconds\":0.0}" << std::endl;
        return 1;
    }
    std::cout << "{\"success\":true,\"path\":\"" << mediaPath.string()
              << "\",\"duration_seconds\":" << duration << "}" << std::endl;
    return 0;
}

// ──────────────────────────────────────────────
// Sotto-comando: --build-scene-segment
// ──────────────────────────────────────────────

static int cmdBuildSceneSegment(int argc, char** argv) {
    std::string image, out;
    double duration = 0.0;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--image" && i + 1 < argc) image = argv[++i];
        else if (arg == "--duration" && i + 1 < argc) duration = std::stod(argv[++i]);
        else if (arg == "--out" && i + 1 < argc) out = argv[++i];
    }
    if (image.empty()) { std::cerr << "errore: --image richiesto\n"; return 1; }
    if (out.empty())   { std::cerr << "errore: --out richiesto\n";   return 1; }
    if (duration <= 0) { std::cerr << "errore: --duration > 0 richiesto\n"; return 1; }

    bool ok = media::buildSceneSegment(fs::path(image), fs::path(out), duration);
    std::cout << "{\"success\":"
              << (ok ? "true" : "false")
              << ",\"out\":\"" << out << "\"}" << std::endl;
    return ok ? 0 : 1;
}

// ──────────────────────────────────────────────
// Sotto-comando: --build-clip-segment
// ──────────────────────────────────────────────

static int cmdBuildClipSegment(int argc, char** argv) {
    std::string clip, out;
    double duration = 0.0;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--clip" && i + 1 < argc) clip = argv[++i];
        else if (arg == "--duration" && i + 1 < argc) duration = std::stod(argv[++i]);
        else if (arg == "--out" && i + 1 < argc) out = argv[++i];
    }
    if (clip.empty()) { std::cerr << "errore: --clip richiesto\n"; return 1; }
    if (out.empty())  { std::cerr << "errore: --out richiesto\n";  return 1; }
    if (duration <= 0) { std::cerr << "errore: --duration > 0 richiesto\n"; return 1; }

    bool ok = media::buildVideoSegment(fs::path(clip), fs::path(out), duration);
    std::cout << "{\"success\":"
              << (ok ? "true" : "false")
              << ",\"out\":\"" << out << "\"}" << std::endl;
    return ok ? 0 : 1;
}

// ──────────────────────────────────────────────
// Sotto-comando: --concat-segments
// ──────────────────────────────────────────────

static int cmdConcatSegments(int argc, char** argv) {
    std::string list, out;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--list" && i + 1 < argc) list = argv[++i];
        else if (arg == "--out" && i + 1 < argc) out = argv[++i];
    }
    if (list.empty()) { std::cerr << "errore: --list richiesto\n"; return 1; }
    if (out.empty())  { std::cerr << "errore: --out richiesto\n";  return 1; }

    // Legge i segmenti dal file lista (una riga per path)
    std::vector<fs::path> segments;
    std::string content = file::readFile(fs::path(list));
    if (!content.empty()) {
        std::istringstream ss(content);
        std::string line;
        while (std::getline(ss, line)) {
            line = json::trim(line);
            if (line.empty()) continue;
            // Tolgo il prefisso "file " se presente (formato concat demuxer)
            const std::string filePrefix = "file ";
            if (line.compare(0, filePrefix.size(), filePrefix) == 0) {
                line = json::trim(line.substr(filePrefix.size()));
            }
            // Tolle le shell quote se presenti
            if (line.size() >= 2 && line.front() == '\'' && line.back() == '\'') {
                line = line.substr(1, line.size() - 2);
            }
            if (!line.empty()) {
                segments.push_back(fs::path(line));
            }
        }
    }

    if (segments.empty()) {
        std::cerr << "errore: nessun segmento valido in " << list << "\n";
        return 1;
    }

    fs::path workDir = fs::path(out).parent_path();
    if (workDir.empty()) workDir = fs::current_path();
    bool ok = media::concatSegments(segments, fs::path(out), workDir);
    std::cout << "{\"success\":"
              << (ok ? "true" : "false")
              << ",\"out\":\"" << out << "\",\"segments\":" << segments.size() << "}" << std::endl;
    return ok ? 0 : 1;
}

// ──────────────────────────────────────────────
// Sotto-comando: --mux-audio
// ──────────────────────────────────────────────

static int cmdMuxAudio(int argc, char** argv) {
    std::string video, audio, out;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--video" && i + 1 < argc) video = argv[++i];
        else if (arg == "--audio" && i + 1 < argc) audio = argv[++i];
        else if (arg == "--out" && i + 1 < argc) out = argv[++i];
    }
    if (video.empty()) { std::cerr << "errore: --video richiesto\n"; return 1; }
    if (audio.empty()) { std::cerr << "errore: --audio richiesto\n"; return 1; }
    if (out.empty())   { std::cerr << "errore: --out richiesto\n";   return 1; }

    bool ok = media::muxAudio(fs::path(video), fs::path(audio), fs::path(out));
    std::cout << "{\"success\":"
              << (ok ? "true" : "false")
              << ",\"out\":\"" << out << "\"}" << std::endl;
    return ok ? 0 : 1;
}

// ──────────────────────────────────────────────
// Sotto-comando: --full-video (comportamento originale)
// ──────────────────────────────────────────────

namespace {

struct SceneWorkResult {
    size_t index{0};
    fs::path segmentPath;
    bool success{false};
    std::string error;
};

size_t determineSceneWorkerCount(size_t renderCount) {
    if (renderCount <= 1) return renderCount;
    size_t configured = 0;
    if (const char* env = std::getenv("VELOX_SCENE_BUILD_WORKERS")) {
        try {
            const int parsed = std::stoi(env);
            if (parsed > 0) configured = static_cast<size_t>(parsed);
        } catch (...) { configured = 0; }
    }
    if (configured > 0)
        return std::max<size_t>(1, std::min(renderCount, configured));
    unsigned int hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;
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
            if (file::downloadAsset(imagePathStr, candidatePath))
                imagePath = candidatePath;
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

static int cmdFullVideo(int argc, char** argv) {
    std::string requestPath;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--request" && i + 1 < argc) requestPath = argv[++i];
    }
    if (requestPath.empty()) {
        std::cerr << "errore: --full-video richiede --request <path>\n";
        return 1;
    }

    const auto requestJson = file::readFile(requestPath);
    if (requestJson.empty()) {
        std::cerr << "errore: failed to read request file\n";
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
    if (stockClipPaths.empty())
        stockClipPaths = parseStringListField(requestJson, "stock_clip_sources");
    const auto scenes = parseScenes(requestJson);
    const auto clipSegments = parseClipSegments(requestJson);

    double voiceoverDurationSeconds = 0.0;
    fs::path downloadedVoiceoverPath;

    if (outputPathStr.empty()) {
        std::cerr << "errore: missing output_path in request\n";
        return 1;
    }

    fs::path outputPath(outputPathStr);
    fs::create_directories(outputPath.parent_path());

    fs::path workBase = fs::temp_directory_path() / "velox_video_engine";
    fs::path workDir = file::makeTempDir(workBase, "job_");
    if (workDir.empty()) {
        std::cerr << "errore: failed to create temp work dir\n";
        return 1;
    }

    const bool clipMode = videoMode == "clip_stock"
        || !clipSegments.empty()
        || !introClipPaths.empty()
        || !stockClipPaths.empty();

    // Download voiceover
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
            std::cerr << "errore: failed to download voiceover audio\n";
            return 1;
        }
        voiceoverDurationSeconds = media::probeMediaDurationSeconds(downloadedVoiceoverPath);
    }

    std::vector<fs::path> segments;

    if (clipMode) {
        size_t segmentIndex = 0;
        // Intro clip segments
        for (size_t i = 0; i < introClipPaths.size(); ++i) {
            std::vector<std::string> candidates = {introClipPaths[i]};
            fs::path clipPath = firstAvailableClip(candidates, workDir, segmentIndex);
            if (clipPath.empty()) {
                std::cerr << "errore: failed to resolve intro clip segment " << i << "\n";
                return 1;
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(segmentIndex) + ".mp4");
            if (!media::buildVideoSegment(clipPath, segmentPath, 4.0)) {
                std::cerr << "errore: failed to build intro clip segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
            ++segmentIndex;
        }
        // Clip segments
        for (size_t i = 0; i < clipSegments.size(); ++i) {
            const auto& clip = clipSegments[i];
            std::vector<std::string> candidates = clip.clip_links;
            if (candidates.empty() && !clip.clip_link.empty())
                candidates.push_back(clip.clip_link);
            fs::path clipPath = firstAvailableClip(candidates, workDir, segmentIndex);
            if (clipPath.empty()) {
                std::cerr << "errore: failed to resolve clip segment " << i << "\n";
                return 1;
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(segmentIndex) + ".mp4");
            double segDuration = clip.duration_seconds > 0.0 ? clip.duration_seconds : 4.0;
            if (!media::buildVideoSegment(clipPath, segmentPath, segDuration)) {
                std::cerr << "errore: failed to build clip segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
            ++segmentIndex;
        }
        // Stock clip segments
        for (size_t i = 0; i < stockClipPaths.size(); ++i) {
            std::vector<std::string> candidates = {stockClipPaths[i]};
            fs::path clipPath = firstAvailableClip(candidates, workDir, segmentIndex);
            if (clipPath.empty()) {
                std::cerr << "errore: failed to resolve stock clip segment " << i << "\n";
                return 1;
            }
            fs::path segmentPath = workDir / ("segment_" + std::to_string(segmentIndex) + ".mp4");
            if (!media::buildVideoSegment(clipPath, segmentPath, 5.0)) {
                std::cerr << "errore: failed to build stock clip segment " << i << "\n";
                return 1;
            }
            segments.push_back(segmentPath);
            ++segmentIndex;
        }
    } else {
        // Scene image mode
        const size_t renderCount = !sceneImagePaths.empty()
            ? sceneImagePaths.size()
            : std::max<size_t>(1, scenes.size());
        segments.reserve(renderCount);

        double perSceneDuration = 0.0;
        if (voiceoverDurationSeconds > 0.0 && renderCount > 0) {
            perSceneDuration = voiceoverDurationSeconds / static_cast<double>(renderCount);
            std::cerr << "voiceover_duration=" << voiceoverDurationSeconds
                      << "s, scenes=" << renderCount
                      << ", per_scene=" << perSceneDuration << "s\n";
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
                    if (index >= renderCount) break;
                    results[index] = buildSceneWorkItem(
                        index, renderCount, sceneImagePaths, scenes,
                        workDir, perSceneDuration, voiceoverDurationSeconds);
                }
            });
        }
        for (auto& worker : workers) worker.join();

        for (size_t i = 0; i < renderCount; ++i) {
            if (!results[i].success) {
                std::cerr << "errore: " << results[i].error
                          << " (voiceover_duration=" << voiceoverDurationSeconds
                          << ", scene_duration=" << (i < scenes.size() ? scenes[i].duration_seconds : 0.0) << ")\n";
                return 1;
            }
            std::cerr << "scene " << i << " segment built at " << results[i].segmentPath << "\n";
            segments.push_back(results[i].segmentPath);
        }
    }

    // Concat segments
    fs::path videoOnlyPath = workDir / "video_only.mp4";
    if (!media::concatSegments(segments, videoOnlyPath, workDir)) {
        std::cerr << "errore: failed to concat segments\n";
        return 1;
    }

    // Mux audio
    fs::path finalOutput = outputPath;
    if (!voiceoverPaths.empty()) {
        fs::path audioPath = downloadedVoiceoverPath.empty() ? workDir / "voiceover_audio" : downloadedVoiceoverPath;
        fs::path muxedOutput = workDir / "final_with_audio.mp4";
        if (!media::muxAudio(videoOnlyPath, audioPath, muxedOutput)) {
            std::cerr << "errore: failed to mux audio into final video\n";
            return 1;
        }
        std::error_code ec;
        fs::copy_file(muxedOutput, finalOutput, fs::copy_options::overwrite_existing, ec);
    } else {
        std::error_code ec;
        fs::copy_file(videoOnlyPath, finalOutput, fs::copy_options::overwrite_existing, ec);
    }

    std::cout << "{\"success\":true,\"job_id\":\"" << jobId
              << "\",\"output_path\":\"" << finalOutput.string()
              << "\",\"video_name\":\"" << videoName
              << "\",\"audio_language_for_srt\":\"" << audioLanguage
              << "\",\"video_mode\":\"" << (clipMode ? "clip_stock" : "scene_image")
              << "\",\"audio_duration_seconds\":" << voiceoverDurationSeconds << "}" << std::endl;
    if (!driveOutputFolder.empty())
        std::cerr << "drive_output_folder_hint=" << driveOutputFolder << "\n";
    return 0;
}

// ──────────────────────────────────────────────
// Main dispatcher
// ──────────────────────────────────────────────

int main(int argc, char** argv) {
    if (argc < 2) {
        printUsage(argv[0]);
        return 1;
    }

    const std::string cmd = argv[1];

    if (cmd == "--help" || cmd == "-h") {
        return cmdHelp(argv[0]);
    }
    if (cmd == "--full-video") {
        return cmdFullVideo(argc, argv);
    }
    if (cmd == "--download-asset") {
        return cmdDownloadAsset(argc, argv);
    }
    if (cmd == "--probe-media") {
        return cmdProbeMedia(argc, argv);
    }
    if (cmd == "--build-scene-segment") {
        return cmdBuildSceneSegment(argc, argv);
    }
    if (cmd == "--build-clip-segment") {
        return cmdBuildClipSegment(argc, argv);
    }
    if (cmd == "--concat-segments") {
        return cmdConcatSegments(argc, argv);
    }
    if (cmd == "--mux-audio") {
        return cmdMuxAudio(argc, argv);
    }

    std::cerr << "errore: sotto-comando sconosciuto \"" << cmd << "\"\n";
    printUsage(argv[0]);
    return 1;
}
