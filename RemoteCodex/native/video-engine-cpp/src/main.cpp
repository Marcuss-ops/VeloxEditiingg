// Velox Video Engine — CLI tool per elaborazione video.
//
// Sotto-comandi disponibili (eseguibili singolarmente):
//
//   --full-video --request <path>
//       Pipeline completa: scarica asset, costruisce segmenti, concatena, muxa audio.
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

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "video_builder.hpp"
#include "file_utils.hpp"
#include "json_utils.hpp"
#include "media_utils.hpp"

namespace fs = std::filesystem;
namespace json = velox::json;
namespace file = velox::file;
namespace media = velox::media;

// Forward declaration (implemented in cmd_full_video.cpp)
int cmdFullVideo(int argc, char** argv);
int cmdRenderPlan(int argc, char** argv);

static void printUsage(const char* prog) {
    std::cerr << "Velox Video Engine — CLI tool per elaborazione video\n"
              << "\nUtilizzo: " << prog << " <sotto-comando> [opzioni]\n"
              << "\nSotto-comandi:\n"
              << "\n  --render --plan <path>"
              << "\n  --full-video --request <path>"
              << "\n  --download-asset --url <url> --dest <path>"
              << "\n  --probe-media <path>"
              << "\n  --build-scene-segment --image <path> --duration <sec> --out <path>"
              << "\n  --build-clip-segment --clip <path> --duration <sec> --out <path>"
              << "\n  --concat-segments --list <file> --out <path>"
              << "\n  --mux-audio --video <path> --audio <path> --out <path>"
              << "\n  --help\n" << std::endl;
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
    if (cmd == "--render") {
        return cmdRenderPlan(argc, argv);
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
