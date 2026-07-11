#pragma once
#include <string>
#include <vector>
#include <filesystem>

namespace velox::file {

// CommandResult captures the outcome AND wall-clock duration of a
// child process launched via runCommandTimed(). The zero value is safe
// (ok=false, exit_code=0, wall_ms=0).
struct CommandResult {
    bool ok{false};
    int exit_code{0};
    double wall_ms{0};
};

std::string readFile(const std::filesystem::path& path);
bool writeFile(const std::filesystem::path& path, const std::string& content);
std::string cacheFilename(const std::string& source);
std::filesystem::path cacheAssetPath(const std::filesystem::path& cacheDir, const std::string& source);
std::string shellQuote(const std::string& s);
bool runCommand(const std::string& cmd);
// runCommandTimed behaves like runCommand but also measures wall-clock
// time in milliseconds. Useful for per-ffmpeg-invocation telemetry.
CommandResult runCommandTimed(const std::string& cmd);
std::string captureCommandOutput(const std::string& cmd);
std::string normalizeDriveUrl(const std::string& url);
bool isDriveFolderUrl(const std::string& url);
std::string resolveDriveFolderToFileUrl(const std::string& folderUrl);
bool copyFile(const std::filesystem::path& src, const std::filesystem::path& dst);
std::filesystem::path makeTempDir(const std::filesystem::path& base, const std::string& prefix);
bool downloadAsset(const std::string& source, const std::filesystem::path& dest, const std::string& cacheDir = "");

} // namespace velox::file
