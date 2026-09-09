#include "render_plan_parser_internal.hpp"
#include "json_utils.hpp"

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

// bindingPathFor looks up "<assetId>" : "<path>" in a flat bindings object.
// Implemented as a manual scan instead of a per-call std::regex: the regex
// was constructed per lookup (audit P2: O(plan × regex-construction) at
// plan scale) and regex construction dominates the actual scan cost.
std::string bindingPathFor(const std::string& bindingsBlock, const std::string& assetId) {
    if (bindingsBlock.empty() || assetId.empty()) return {};
    const std::string needle = "\"" + assetId + "\"";
    size_t pos = 0;
    while ((pos = bindingsBlock.find(needle, pos)) != std::string::npos) {
        size_t cursor = pos + needle.size();
        while (cursor < bindingsBlock.size() &&
               std::isspace(static_cast<unsigned char>(bindingsBlock[cursor]))) {
            ++cursor;
        }
        if (cursor >= bindingsBlock.size() || bindingsBlock[cursor] != ':') {
            pos += needle.size();
            continue;
        }
        ++cursor;
        while (cursor < bindingsBlock.size() &&
               std::isspace(static_cast<unsigned char>(bindingsBlock[cursor]))) {
            ++cursor;
        }
        if (cursor >= bindingsBlock.size() || bindingsBlock[cursor] != '"') {
            pos += needle.size();
            continue;
        }
        ++cursor;
        std::string raw;
        raw.reserve(64);
        bool escape = false;
        for (; cursor < bindingsBlock.size(); ++cursor) {
            const char c = bindingsBlock[cursor];
            if (escape) {
                raw.push_back('\\');
                raw.push_back(c);
                escape = false;
                continue;
            }
            if (c == '\\') {
                escape = true;
                continue;
            }
            if (c == '"') {
                return unescapeJsonString(raw);
            }
            raw.push_back(c);
        }
        return {}; // unterminated string: treat as absent
    }
    return {};
}

} // namespace velox::plan::detail
