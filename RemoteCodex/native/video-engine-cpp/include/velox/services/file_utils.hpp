#pragma once
#include <string>
#include <vector>
#include <filesystem>

namespace velox::file {

std::string readFile(const std::filesystem::path& path);
bool writeFile(const std::filesystem::path& path, const std::string& content);
std::string cacheFilename(const std::string& source);
std::filesystem::path cacheAssetPath(const std::filesystem::path& cacheDir, const std::string& source);
std::string shellQuote(const std::string& s);
bool runCommand(const std::string& cmd);
std::string captureCommandOutput(const std::string& cmd);
std::string normalizeDriveUrl(const std::string& url);
bool isDriveFolderUrl(const std::string& url);
std::string resolveDriveFolderToFileUrl(const std::string& folderUrl);
bool copyFile(const std::filesystem::path& src, const std::filesystem::path& dst);
std::filesystem::path makeTempDir(const std::filesystem::path& base, const std::string& prefix);
bool downloadAsset(const std::string& source, const std::filesystem::path& dest, const std::string& cacheDir = "");

} // namespace velox::file
