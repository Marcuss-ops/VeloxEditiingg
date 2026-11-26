// Velox Video Engine — CLI tool per elaborazione video.
//
// Sotto-comandi disponibili (eseguibili singolarmente):
//
//   --render --plan <path>
//       Renderizza un piano di montaggio (copy-only packet pipeline).
//
//   --render-frames --input <path> --output <path> [--width W --height H --fps N --codec c --preset p --pool N]
//       Renderizza frame con filtri.
//
//   --help
//       Mostra questa guida.
//
// Ogni sotto-comando stampa JSON su stdout e log su stderr.
// Errori portano a exit code 1.

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include "velox/services/file_utils.hpp"
#include "json_utils.hpp"
#include "velox/services/media_utils.hpp"
#include "velox/services/media_probe.hpp"

namespace fs = std::filesystem;
namespace json = velox::json;
namespace file = velox::file;
namespace media = velox::media;

int cmdRenderPlan(int argc, char** argv);
int cmdRenderFrames(int argc, char** argv);

static int cmdProbeSource(int argc, char** argv) {
    std::string inputPath;
    for (int i = 2; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "--input" && i + 1 < argc) inputPath = argv[++i];
    }
    if (inputPath.empty()) {
        std::cerr << "errore: --probe-source richiede --input <path>\n";
        return 1;
    }
    const auto probe = velox::media::probeSourceForCertificationInProcess(inputPath);
    if (!probe) {
        std::cerr << "errore: LibAV could not establish complete source evidence\n";
        return 1;
    }
    std::cout << std::setprecision(17) << "{\"codec_name\":\"" << probe->codec_name
              << "\",\"profile\":\"" << probe->profile
              << "\",\"level\":" << probe->level
              << ",\"width\":" << probe->width
              << ",\"height\":" << probe->height
              << ",\"pixel_format\":\"" << probe->pixel_format
              << "\",\"frame_rate\":\"" << probe->frame_rate_num << "/" << probe->frame_rate_den
              << "\",\"average_frame_rate\":\"" << probe->average_frame_rate_num << "/" << probe->average_frame_rate_den
              << "\",\"time_base\":\"" << probe->time_base_num << "/" << probe->time_base_den
              << "\",\"start_time_ticks\":" << probe->start_time
              << ",\"start_time_verified\":true"
              << ",\"start_time_seconds\":" << probe->start_time_seconds
              << ",\"b_frames\":" << probe->b_frames
              << ",\"bit_rate\":" << probe->bit_rate
              << ",\"exact_frame_count\":" << probe->exact_frame_count
              << ",\"extradata_sha256\":\"" << probe->extradata_sha256
              << "\",\"packet_keyframe_times\":[";
    for (std::size_t i = 0; i < probe->packet_keyframe_times.size(); ++i) {
        if (i != 0) std::cout << ",";
        std::cout << probe->packet_keyframe_times[i];
    }
    std::cout << "]}" << std::endl;
    return 0;
}

static void printUsage(const char* prog) {
    std::cerr << "Velox Video Engine — CLI tool per elaborazione video\n"
              << "\nUtilizzo: " << prog << " <sotto-comando> [opzioni]\n"
              << "\nSotto-comandi:\n"
              << "\n  --render --plan <path>"
              << "\n  --probe-source --input <path>"
              << "\n  --render-frames --input <path> --output <path> [--width W --height H --fps N --codec c --preset p --pool N]"
              << "\n  --help\n" << std::endl;
}

static int cmdHelp(const char* prog) {
    printUsage(prog);
    return 0;
}

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
    if (cmd == "--probe-source") {
        return cmdProbeSource(argc, argv);
    }
    if (cmd == "--render-frames") {
        return cmdRenderFrames(argc, argv);
    }
    std::cerr << "errore: sotto-comando sconosciuto \"" << cmd << "\"\n";
    printUsage(argv[0]);
    return 1;
}
