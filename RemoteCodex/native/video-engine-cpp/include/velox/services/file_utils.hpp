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
    double child_user_ms{0};
    double child_system_ms{0};
    long child_max_rss_kb{0};
    long child_input_blocks{0};
    long child_output_blocks{0};
};

std::string readFile(const std::filesystem::path& path);
bool writeFile(const std::filesystem::path& path, const std::string& content);
std::string cacheFilename(const std::string& source);
std::filesystem::path cacheAssetPath(const std::filesystem::path& cacheDir, const std::string& source);
// Returns an existing immutable local asset path without materializing a
// workdir copy. `cacheReference` may be either the verified cache file itself
// (the worker's preferred contract) or a legacy cache directory used with
// cacheAssetPath(). An empty path means the caller must use the remote
// download/staging fallback.
std::filesystem::path resolveLocalAssetPath(
    const std::string& source,
    const std::filesystem::path& cacheReference);
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
// Returns a unique partial path in the target's parent directory. Keeping the
// partial beside the target guarantees that publishAtomic can rename without
// crossing filesystems.
std::filesystem::path makePartialPath(const std::filesystem::path& target);
// Publishes an already-complete partial: fsync(file), atomic rename, then
// fsync(parent directory). The partial and target must share a directory.
// Returns false only before the rename commit; after a successful rename the
// target is published even if the post-commit directory fsync is unavailable.
// `durable` is true only when the parent-directory fsync completed; false
// explicitly represents the published-but-not-durable state.
bool publishAtomic(const std::filesystem::path& partial,
                   const std::filesystem::path& target,
                   std::string* error = nullptr,
                   bool* durable = nullptr);
bool downloadAsset(const std::string& source, const std::filesystem::path& dest, const std::string& cacheDir = "");

} // namespace velox::file
