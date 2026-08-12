#include "velox/services/file_utils.hpp"
#include "json_utils.hpp"
#include <array>
#include <chrono>
#include <cstdlib>
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
    int rc = std::system(cmd.c_str());
    return rc == 0;
}

CommandResult runCommandTimed(const std::string& cmd) {
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

bool downloadAsset(const std::string& source, const fs::path& dest, const std::string& cacheDir) {
    if (source.empty()) {
        return false;
    }

    if (fs::exists(source)) {
        return copyFile(source, dest);
    }

    if (!cacheDir.empty()) {
        fs::create_directories(cacheDir);
        auto cachedPath = cacheAssetPath(cacheDir, source);
        if (fs::exists(cachedPath)) {
            return copyFile(cachedPath, dest);
        }
    }

    std::string resolvedSource = source;
    if (isDriveFolderUrl(source)) {
        resolvedSource = resolveDriveFolderToFileUrl(source);
        if (resolvedSource.empty()) {
            return false;
        }
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

    if (!cacheDir.empty() && ok) {
        auto cachedPath = cacheAssetPath(cacheDir, source);
        copyFile(tempDest, cachedPath);
    }

    std::error_code ec;
    fs::remove(tempDest, ec);

    return ok;
}

} // namespace velox::file
