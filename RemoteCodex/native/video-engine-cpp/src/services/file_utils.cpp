#include "velox/services/file_utils.hpp"
#include "velox/services/io_counters.hpp"
#include "json_utils.hpp"
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <regex>
#include <iostream>
#include <sys/resource.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace velox::file {

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

std::string cacheFilename(const std::string& source) {
    std::string name;
    name.reserve(source.size());
    for (char c : source) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            name.push_back(c);
        } else {
            name.push_back('_');
        }
    }
    if (name.size() > 200) {
        name.resize(200);
    }
    return name + ".cache";
}

fs::path cacheAssetPath(const fs::path& cacheDir, const std::string& source) {
    return cacheDir / cacheFilename(source);
}

fs::path resolveLocalAssetPath(const std::string& source, const fs::path& cacheReference) {
    // The worker resolver replaces verified velox-asset:// references with
    // the immutable local path before the RenderPlan reaches this process.
    // Check that path first; libavformat can open it directly and no staging
    // copy is needed.
    if (!source.empty() && fs::is_regular_file(source)) {
        return fs::path(source);
    }

    if (cacheReference.empty()) {
        return {};
    }

    // `cache_key` is also used by older callers as the actual verified local
    // file path. Do not interpret that path as a directory and accidentally
    // fall through to downloadAsset(), which would copy it into workdir.
    if (fs::is_regular_file(cacheReference)) {
        return cacheReference;
    }

    // Preserve compatibility with the legacy C++ cache-directory contract.
    if (fs::is_directory(cacheReference)) {
        const fs::path cached = cacheAssetPath(cacheReference, source);
        if (fs::is_regular_file(cached)) {
            return cached;
        }
    }
    return {};
}

std::string shellQuote(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') {
            out += "'\\''";
        } else {
            out.push_back(c);
        }
    }
    out.push_back('\'');
    return out;
}

bool runCommand(const std::string& cmd) {
    services::recordExternalSpawn(cmd);
    int rc = std::system(cmd.c_str());
    return rc == 0;
}

CommandResult runCommandTimed(const std::string& cmd) {
    // Count this spawn before launching: the sidecar process_counters
    // block is the engine-declared external-spawn ledger (the Phase-1
    // copy-only invariant is external_spawn_count == 0).
    services::recordExternalSpawn(cmd);
    CommandResult res;
    struct rusage before{};
    struct rusage after{};
    getrusage(RUSAGE_CHILDREN, &before);
    auto start = std::chrono::steady_clock::now();
    int rc = std::system(cmd.c_str());
    auto end = std::chrono::steady_clock::now();
    getrusage(RUSAGE_CHILDREN, &after);
    res.wall_ms = std::chrono::duration<double, std::milli>(end - start).count();
    res.exit_code = rc;
    res.ok = (rc == 0);
    res.child_user_ms = static_cast<double>(after.ru_utime.tv_sec - before.ru_utime.tv_sec) * 1000.0 +
                        static_cast<double>(after.ru_utime.tv_usec - before.ru_utime.tv_usec) / 1000.0;
    res.child_system_ms = static_cast<double>(after.ru_stime.tv_sec - before.ru_stime.tv_sec) * 1000.0 +
                          static_cast<double>(after.ru_stime.tv_usec - before.ru_stime.tv_usec) / 1000.0;
    // ru_maxrss is a high-water mark rather than a delta. It is therefore
    // reported as the post-command child high-water mark, not as per-command
    // allocated memory.
    res.child_max_rss_kb = after.ru_maxrss;
    res.child_input_blocks = after.ru_inblock - before.ru_inblock;
    res.child_output_blocks = after.ru_oublock - before.ru_oublock;
    return res;
}

std::string captureCommandOutput(const std::string& cmd) {
    services::recordExternalSpawn(cmd);
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

std::string normalizeDriveUrl(const std::string& url) {
    std::smatch match;
    if (std::regex_search(url, match, std::regex(R"(/file/d/([^/]+))"))) {
        return "https://drive.usercontent.google.com/download?id=" + match[1].str() + "&export=download&authuser=0";
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
    std::error_code sizeEc;
    const auto bytes = fs::file_size(src, sizeEc);
    services::recordFileCopy((!ec && !sizeEc) ? static_cast<int64_t>(bytes) : 0);
    const char* diskMetrics = std::getenv("VELOX_BENCH_DISK_COPY_METRICS");
    if (diskMetrics != nullptr && std::string(diskMetrics) != "0" &&
        std::string(diskMetrics) != "false") {
        std::error_code sizeEc;
        const auto bytes = fs::file_size(src, sizeEc);
        std::cerr << "{\"metric\":\"disk.copy\",\"bytes\":"
                  << ((!ec && !sizeEc) ? bytes : 0)
                  << ",\"ok\":" << (!ec ? "true" : "false") << "}\n";
    }
    return !ec;
}

fs::path makeTempDir(const fs::path& requestedBase, const std::string& prefix) {
    // A previous container run can leave a fixed /tmp subdirectory owned by
    // another UID. Try the requested namespace first, then fall back to a
    // unique directory directly under the system temp root instead of making
    // every render fail closed on that stale path.
    std::vector<fs::path> bases;
    bases.push_back(requestedBase);
    std::error_code tempError;
    const fs::path systemTemp = fs::temp_directory_path(tempError);
    if (!tempError && systemTemp != requestedBase) {
        bases.push_back(systemTemp);
    }

    const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto process = static_cast<long long>(::getpid());
    for (const auto& base : bases) {
        std::error_code createError;
        fs::create_directories(base, createError);
        if (createError || !fs::is_directory(base)) {
            continue;
        }
        for (int attempt = 0; attempt < 100; ++attempt) {
            auto candidate = base / (prefix + std::to_string(process) + "_" +
                                     std::to_string(nonce) + "_" + std::to_string(attempt));
            std::error_code ec;
            if (fs::create_directory(candidate, ec) && !ec) {
                return candidate;
            }
        }
    }
    return {};
}

fs::path makePartialPath(const fs::path& target) {
    if (target.empty()) {
        return {};
    }
    fs::path parent = target.parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::string extension = target.extension().string();
    const std::string filename = target.filename().string();
    const std::string stem = extension.empty()
        ? filename
        : filename.substr(0, filename.size() - extension.size());
    return parent / (stem + ".partial." +
                     std::to_string(static_cast<long long>(::getpid())) + "." +
                     std::to_string(nonce) + extension);
}

bool publishAtomic(const fs::path& partial, const fs::path& target, std::string* error, bool* durable) {
    if (durable != nullptr) {
        *durable = false;
    }
    auto fail = [&](const std::string& message) {
        if (error != nullptr) {
            *error = message;
        }
        return false;
    };
    if (partial.empty() || target.empty()) {
        return fail("partial and target paths are required");
    }
    auto failBeforeRename = [&](const std::string& message) {
        std::error_code cleanup_error;
        fs::remove(partial, cleanup_error);
        return fail(message);
    };
    std::error_code absolute_error;
    const fs::path partial_parent = fs::absolute(partial, absolute_error).parent_path().lexically_normal();
    if (absolute_error) {
        return failBeforeRename("cannot resolve partial parent directory: " + absolute_error.message());
    }
    absolute_error.clear();
    const fs::path target_parent = fs::absolute(target, absolute_error).parent_path().lexically_normal();
    if (absolute_error || partial_parent != target_parent) {
        return failBeforeRename("partial and target must be in the same directory");
    }
    if (!fs::is_regular_file(partial)) {
        return failBeforeRename("partial output is not a regular file: " + partial.string());
    }

    const int fd = ::open(partial.c_str(), O_RDONLY);
    if (fd < 0) {
        std::error_code ec;
        fs::remove(partial, ec);
        return fail("open partial for fsync failed: " + partial.string());
    }
    const bool synced = ::fsync(fd) == 0;
    const int sync_errno = errno;
    const bool closed = ::close(fd) == 0;
    if (!synced || !closed) {
        std::error_code ec;
        fs::remove(partial, ec);
        if (!synced) {
            return fail("fsync partial failed: " + std::string(std::strerror(sync_errno)));
        }
        return fail("close partial failed: " + partial.string());
    }

    std::error_code rename_error;
    fs::rename(partial, target, rename_error);
    if (rename_error) {
        std::error_code ec;
        fs::remove(partial, ec);
        return fail("atomic rename failed: " + rename_error.message());
    }

    fs::path parent = target.parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    const int dir_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) {
        std::cerr << "warning: atomic output committed but output directory could not be opened for fsync: "
                  << parent << "\n";
        return true;
    }
    const bool dir_synced = ::fsync(dir_fd) == 0;
    const int dir_errno = errno;
    const bool dir_closed = ::close(dir_fd) == 0;
    if (!dir_synced) {
        std::cerr << "warning: atomic output committed but output directory fsync failed: "
                  << std::strerror(dir_errno) << "\n";
    } else if (!dir_closed) {
        std::cerr << "warning: atomic output committed but output directory close failed: "
                  << parent << "\n";
    } else if (durable != nullptr) {
        *durable = true;
    }
    return true;
}

// Forward declaration: defined after downloadAsset, which uses it.
static int64_t assetCopyBytes(const fs::path& source);

bool downloadAsset(const std::string& source, const fs::path& dest, const std::string& cacheDir) {
    if (source.empty()) {
        return false;
    }

    if (fs::exists(source)) {
        const bool ok = copyFile(source, dest);
        services::recordAssetCopy(assetCopyBytes(source));
        return ok;
    }

    if (!cacheDir.empty()) {
        fs::create_directories(cacheDir);
        auto cachedPath = cacheAssetPath(cacheDir, source);
        if (fs::exists(cachedPath)) {
            const bool ok = copyFile(cachedPath, dest);
            services::recordAssetCopy(assetCopyBytes(cachedPath));
            return ok;
        }
    }

    std::string resolvedSource = source;
    if (isDriveFolderUrl(source)) {
        resolvedSource = resolveDriveFolderToFileUrl(source);
        if (resolvedSource.empty()) {
            return false;
        }
    }
    // A missing local path must not be handed to curl: curl interprets an
    // arbitrary cache/path string as a URL and reports the misleading
    // "bad/illegal format" error. The worker resolver is responsible for
    // turning verified velox asset references into local files before this
    // boundary; only explicit HTTP(S) sources may reach the network path.
    const bool isHTTP = resolvedSource.rfind("http://", 0) == 0 ||
                        resolvedSource.rfind("https://", 0) == 0;
    if (!isHTTP) {
        return false;
    }
    const auto url = normalizeDriveUrl(resolvedSource);

    auto tempDest = fs::path(dest.string() + ".download_tmp");
    std::string cmd = "curl -L --fail --silent --show-error -o " + shellQuote(tempDest.string()) + " " + shellQuote(url);
    if (!runCommand(cmd)) {
        std::error_code ec;
        fs::remove(tempDest, ec);
        return false;
    }

    bool ok = copyFile(tempDest, dest);
    services::recordAssetCopy(assetCopyBytes(tempDest));

    if (!cacheDir.empty() && ok) {
        auto cachedPath = cacheAssetPath(cacheDir, source);
        copyFile(tempDest, cachedPath);
        services::recordAssetCopy(assetCopyBytes(tempDest));
    }

    std::error_code ec;
    fs::remove(tempDest, ec);

    return ok;
}

// assetCopyBytes returns the byte count of a materialization copy source
// (0 when unreadable) so downloadAsset can project its staging copies
// into the asset_bytes_copied counter without double counting the
// generic file_copy accounting done inside copyFile.
static int64_t assetCopyBytes(const fs::path& source) {
    std::error_code ec;
    const auto bytes = fs::file_size(source, ec);
    return ec ? 0 : static_cast<int64_t>(bytes);
}

} // namespace velox::file
