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
#include <sstream>
#include <string>
#include <vector>

#include "video_builder.hpp"
#include "velox/services/file_utils.hpp"
#include "json_utils.hpp"
#include "velox/services/media_utils.hpp"

namespace fs = std::filesystem;
namespace json = velox::json;
namespace file = velox::file;
namespace media = velox::media;

int cmdRenderPlan(int argc, char** argv);
int cmdRenderFrames(int argc, char** argv);

static void printUsage(const char* prog) {
    std::cerr << "Velox Video Engine — CLI tool per elaborazione video\n"
              << "\nUtilizzo: " << prog << " <sotto-comando> [opzioni]\n"
              << "\nSotto-comandi:\n"
              << "\n  --render --plan <path>"
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
    if (cmd == "--render-frames") {
        return cmdRenderFrames(argc, argv);
    }
    std::cerr << "errore: sotto-comando sconosciuto \"" << cmd << "\"\n";
    printUsage(argv[0]);
    return 1;
}
