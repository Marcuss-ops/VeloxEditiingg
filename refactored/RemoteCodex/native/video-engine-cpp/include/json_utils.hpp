#ifndef VELOX_JSON_UTILS_HPP
#define VELOX_JSON_UTILS_HPP

#include <cctype>
#include <regex>
#include <string>
#include <vector>

namespace velox {
namespace json {

inline std::string trim(std::string s) {
    auto notSpace = [](unsigned char c) { return !std::isspace(c); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
    s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
    return s;
}

inline std::string extractJsonString(const std::string& json, const std::string& key) {
    const std::regex re("\"" + key + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
    std::smatch match;
    if (std::regex_search(json, match, re) && match.size() > 1) {
        return match[1].str();
    }
    return {};
}

inline std::string unescapeJsonString(std::string s) {
    std::string out;
    out.reserve(s.size());
    bool escape = false;
    for (char c : s) {
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

inline std::string extractJsonStringValue(const std::string& json, const std::string& key) {
    return unescapeJsonString(extractJsonString(json, key));
}

inline double extractJsonNumberValue(const std::string& json, const std::string& key, double fallback = 0.0) {
    const std::regex re("\"" + key + "\"\\s*:\\s*([-+]?[0-9]*\\.?[0-9]+)");
    std::smatch match;
    if (std::regex_search(json, match, re) && match.size() > 1) {
        try {
            return std::stod(match[1].str());
        } catch (...) {
            return fallback;
        }
    }
    return fallback;
}

inline std::string extractArrayBlock(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) {
        return {};
    }
    pos = json.find('[', pos);
    if (pos == std::string::npos) {
        return {};
    }
    int depth = 0;
    for (size_t i = pos; i < json.size(); ++i) {
        char c = json[i];
        if (c == '"') {
            ++i;
            bool escape = false;
            for (; i < json.size(); ++i) {
                char cc = json[i];
                if (escape) {
                    escape = false;
                    continue;
                }
                if (cc == '\\') {
                    escape = true;
                    continue;
                }
                if (cc == '"') {
                    break;
                }
            }
            continue;
        }
        if (c == '[') {
            ++depth;
        } else if (c == ']') {
            --depth;
            if (depth == 0) {
                return json.substr(pos, i - pos + 1);
            }
        }
    }
    return {};
}

inline std::vector<std::string> extractArrayStrings(const std::string& json, const std::string& key) {
    std::vector<std::string> values;
    auto block = extractArrayBlock(json, key);
    if (block.empty()) {
        return values;
    }
    const std::regex re("\"((?:\\\\.|[^\"])*)\"");
    for (std::sregex_iterator it(block.begin(), block.end(), re), end; it != end; ++it) {
        if (it->size() > 1) {
            values.push_back(unescapeJsonString((*it)[1].str()));
        }
    }
    return values;
}

inline std::vector<std::string> splitTopLevelObjects(const std::string& arrayBlock) {
    std::vector<std::string> objects;
    if (arrayBlock.size() < 2 || arrayBlock.front() != '[') {
        return objects;
    }

    bool inString = false;
    bool escape = false;
    int depth = 0;
    size_t objStart = std::string::npos;

    for (size_t i = 1; i < arrayBlock.size() - 1; ++i) {
        char c = arrayBlock[i];
        if (inString) {
            if (escape) {
                escape = false;
                continue;
            }
            if (c == '\\') {
                escape = true;
                continue;
            }
            if (c == '"') {
                inString = false;
            }
            continue;
        }

        if (c == '"') {
            inString = true;
            continue;
        }
        if (c == '{') {
            if (depth == 0) {
                objStart = i;
            }
            ++depth;
        } else if (c == '}') {
            --depth;
            if (depth == 0 && objStart != std::string::npos) {
                objects.push_back(arrayBlock.substr(objStart, i - objStart + 1));
                objStart = std::string::npos;
            }
        }
    }
    return objects;
}

} // namespace json
} // namespace velox

#endif // VELOX_JSON_UTILS_HPP
