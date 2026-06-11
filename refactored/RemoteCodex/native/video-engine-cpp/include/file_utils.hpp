#ifndef VELOX_FILE_UTILS_HPP
#define VELOX_FILE_UTILS_HPP

// Utility per I/O su filesystem, download di asset e risoluzione URL Google Drive.
// Funzioni:
//   - Lettura/scrittura file
//   - Download via curl (con supporto Google Drive)
//   - Esecuzione comandi shell
//   - Copia file e creazione directory temporanee

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>

#include "json_utils.hpp"

namespace fs = std::filesystem;

namespace velox {
namespace file {

inline std::string readFile(const fs::path& path) {
    std::ifstream in(path);
    if (!in) {
        return {};
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

inline bool writeFile(const fs::path& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        return false;
    }
    out << content;
    return static_cast<bool>(out);
}

inline std::string shellQuote(const std::string& s) {
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

inline bool runCommand(const std::string& cmd) {
    int rc = std::system(cmd.c_str());
    return rc == 0;
}

inline std::string captureCommandOutput(const std::string& cmd) {
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

inline std::string normalizeDriveUrl(const std::string& url) {
    std::smatch match;
    if (std::regex_search(url, match, std::regex(R"(/file/d/([^/]+))"))) {
        return "https://drive.google.com/uc?export=download&id=" + match[1].str();
    }
    return url;
}

inline bool isDriveFolderUrl(const std::string& url) {
    return url.find("/drive/folders/") != std::string::npos;
}

inline std::string resolveDriveFolderToFileUrl(const std::string& folderUrl) {
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

inline bool copyFile(const fs::path& src, const fs::path& dst) {
    std::error_code ec;
    fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
    return !ec;
}

inline fs::path makeTempDir(const fs::path& base, const std::string& prefix) {
    fs::create_directories(base);
    for (int i = 0; i < 100; ++i) {
        auto candidate = base / (prefix + std::to_string(std::rand()));
        std::error_code ec;
        if (fs::create_directory(candidate, ec) && !ec) {
            return candidate;
        }
    }
    return {};
}

inline bool downloadAsset(const std::string& source, const fs::path& dest) {
    if (source.empty()) {
        return false;
    }
    if (fs::exists(source)) {
        return copyFile(source, dest);
    }
    std::string resolvedSource = source;
    if (isDriveFolderUrl(source)) {
        resolvedSource = resolveDriveFolderToFileUrl(source);
        if (resolvedSource.empty()) {
            return false;
        }
    }
    const auto url = normalizeDriveUrl(resolvedSource);
    std::string cmd = "curl -L --fail --silent --show-error -o " + shellQuote(dest.string()) + " " + shellQuote(url);
    return runCommand(cmd);
}

} // namespace file
} // namespace velox

#endif // VELOX_FILE_UTILS_HPP
