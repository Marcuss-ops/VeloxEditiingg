#ifndef VELOX_JSON_UTILS_HPP
#define VELOX_JSON_UTILS_HPP

// Utility per parsing JSON via regex, usate dal C++ video engine.
// Poiché il C++ engine non usa una libreria JSON completa (es. nlohmann),
// queste funzioni estraggono valori da JSON serializzato usando regex e
// scanning manuale di array/oggetti annidati.
//
// Limitazioni note:
//   - Non gestisce JSON annidato oltre un livello di array/oggetti
//   - Le regex non sono conformi allo standard JSON (non gestiscono escape
//     complessi, Unicode, etc.)
//   - Adeguato per il subset JSON prodotto dal Go serialization del progetto

#include <cctype>
#include <regex>
#include <string>
#include <vector>

namespace velox {
namespace json {

// Canonical JSON string escaper for the C++ engine. Every emission site
// (telemetry emitter, render engine metadata, mux metrics, media utils)
// must route through this single definition (audit P2: four divergent
// copies were a semantic-drift risk). Escapes quotes, backslashes, and
// every control character below 0x20 (short-form for the named escapes,
// \u00XX for the rest).
inline std::string escapeJsonString(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 4);
    static const char hex[] = "0123456789abcdef";
    for (unsigned char c : value) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b':  out += "\\b"; break;
            case '\f':  out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    out += "\\u00";
                    out += hex[(c >> 4) & 0x0f];
                    out += hex[c & 0x0f];
                } else {
                    out += static_cast<char>(c);
                }
                break;
        }
    }
    return out;
}

inline std::string trim(std::string s) {
    auto notSpace = [](unsigned char c) { return !std::isspace(c); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
    s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
    return s;
}

inline std::string extractJsonString(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return {};
    pos += needle.size();
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) ++pos;
    if (pos >= json.size() || json[pos] != ':') {
        // allow spaces before colon: find colon after needle
        pos = json.find(':', pos - needle.size());
        if (pos == std::string::npos) return {};
    }
    ++pos;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) ++pos;
    if (pos >= json.size() || json[pos] != '"') return {};
    ++pos;
    std::string raw;
    raw.reserve(64);
    bool escape = false;
    for (; pos < json.size(); ++pos) {
        char c = json[pos];
        if (escape) {
            raw.push_back('\\');
            raw.push_back(c);
            escape = false;
            continue;
        }
        if (c == '\\') { escape = true; continue; }
        if (c == '"') return raw;
        raw.push_back(c);
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
    const std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return fallback;
    pos = json.find(':', pos + needle.size());
    if (pos == std::string::npos) return fallback;
    ++pos;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) ++pos;
    size_t start = pos;
    if (pos < json.size() && (json[pos] == '-' || json[pos] == '+')) ++pos;
    bool has_digit = false;
    while (pos < json.size() && (std::isdigit(static_cast<unsigned char>(json[pos])) || json[pos] == '.')) {
        if (std::isdigit(static_cast<unsigned char>(json[pos]))) has_digit = true;
        ++pos;
    }
    if (!has_digit) return fallback;
    try {
        return std::stod(json.substr(start, pos - start));
    } catch (...) {
        return fallback;
    }
}

inline bool extractJsonBoolValue(const std::string& json, const std::string& key, bool fallback = false) {
    const std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return fallback;
    pos = json.find(':', pos + needle.size());
    if (pos == std::string::npos) return fallback;
    ++pos;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) ++pos;
    if (json.compare(pos, 4, "true") == 0) return true;
    if (json.compare(pos, 5, "false") == 0) return false;
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
    // Manual scan: extract quoted strings without per-element regex.
    bool in_string = false;
    bool escape = false;
    size_t start = 0;
    for (size_t i = 0; i < block.size(); ++i) {
        char c = block[i];
        if (in_string) {
            if (escape) { escape = false; continue; }
            if (c == '\\') { escape = true; continue; }
            if (c == '"') {
                values.push_back(unescapeJsonString(block.substr(start, i - start)));
                in_string = false;
            }
            continue;
        }
        if (c == '"') { in_string = true; start = i + 1; }
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
