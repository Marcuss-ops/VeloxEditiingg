#include "render_plan_parser_internal.hpp"
#include "json_utils.hpp"

#include <regex>

namespace velox::plan::detail {
namespace {

std::string unescapeJsonString(std::string value) {
    std::string out;
    out.reserve(value.size());
    bool escape = false;
    for (char c : value) {
        if (escape) {
            switch (c) {
                case 'n': out.push_back('\n'); break;
                case 't': out.push_back('\t'); break;
                case 'r': out.push_back('\r'); break;
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                default: out.push_back(c); break;
            }
            escape = false;
            continue;
        }
        if (c == '\\') {
            escape = true;
            continue;
        }
        out.push_back(c);
    }
    return out;
}

std::string regexEscape(const std::string& value) {
    static const std::string specials = "\\^$.|?*+()[]{}";
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
        if (specials.find(c) != std::string::npos) out.push_back('\\');
        out.push_back(c);
    }
    return out;
}

} // namespace

std::string extractObjectBlock(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return {};
    pos = json.find('{', pos);
    if (pos == std::string::npos) return {};

    int depth = 0;
    bool inString = false;
    bool escape = false;
    for (size_t i = pos; i < json.size(); ++i) {
        const char c = json[i];
        if (inString) {
            if (escape) {
                escape = false;
            } else if (c == '\\') {
                escape = true;
            } else if (c == '"') {
                inString = false;
            }
            continue;
        }
        if (c == '"') {
            inString = true;
        } else if (c == '{') {
            ++depth;
        } else if (c == '}' && --depth == 0) {
            return json.substr(pos, i - pos + 1);
        }
    }
    return {};
}

std::string bindingPathFor(const std::string& bindingsBlock, const std::string& assetId) {
    if (bindingsBlock.empty() || assetId.empty()) return {};
    const std::regex re(
        "\"" + regexEscape(assetId) + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
    std::smatch match;
    if (std::regex_search(bindingsBlock, match, re) && match.size() > 1) {
        return unescapeJsonString(match[1].str());
    }
    return {};
}

} // namespace velox::plan::detail
