#include "velox/services/file_utils.hpp"
#include "json_utils.hpp"
#include <array>
#include <fstream>
#include <sstream>
#include <regex>
#include <iostream>

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
    return !ec;
}

fs::path makeTempDir(const fs::path& base, const std::string& prefix) {
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
