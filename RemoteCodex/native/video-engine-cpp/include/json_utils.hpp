#ifndef VELOX_JSON_UTILS_HPP
#define VELOX_JSON_UTILS_HPP

// Small dependency-free JSON structural scanner used by the C++ video engine.
// It is intentionally not a general-purpose DOM: callers only need the byte
// span of a named value and can then decode the scalar or copy the object /
// array block. Unlike the old string::find helpers, keys are recognized only
// as object members, never when they occur inside a URL or another string.

#include <cctype>
#include <algorithm>
#include <regex>
#include <string>
#include <vector>

namespace velox {
namespace json {

namespace detail {

inline void skipWhitespace(const std::string& json, size_t& pos, size_t end) {
    while (pos < end && std::isspace(static_cast<unsigned char>(json[pos]))) ++pos;
}

inline bool skipString(const std::string& json, size_t& pos, size_t end) {
    if (pos >= end || json[pos] != '"') return false;
    ++pos;
    bool escaped = false;
    for (; pos < end; ++pos) {
        const char c = json[pos];
        if (escaped) {
            escaped = false;
            continue;
        }
        if (c == '\\') {
            escaped = true;
        } else if (c == '"') {
            ++pos;
            return true;
        }
    }
    return false;
}

inline bool skipValue(const std::string& json, size_t& pos, size_t end) {
    skipWhitespace(json, pos, end);
    if (pos >= end) return false;
    if (json[pos] == '"') return skipString(json, pos, end);

    if (json[pos] == '{' || json[pos] == '[') {
        const char open = json[pos];
        const char close = open == '{' ? '}' : ']';
        int depth = 0;
        bool inString = false;
        bool escaped = false;
        for (; pos < end; ++pos) {
            const char c = json[pos];
            if (inString) {
                if (escaped) escaped = false;
                else if (c == '\\') escaped = true;
                else if (c == '"') inString = false;
                continue;
            }
            if (c == '"') {
                inString = true;
            } else if (c == open) {
                ++depth;
            } else if (c == close && --depth == 0) {
                ++pos;
                return true;
            } else if ((open == '{' && c == '[') || (open == '[' && c == '{')) {
                // The outer scan has to account for both container kinds.
                size_t nested = pos;
                if (!skipValue(json, nested, end)) return false;
                pos = nested - 1;
            }
        }
        return false;
    }

    const size_t start = pos;
    while (pos < end && json[pos] != ',' && json[pos] != '}' && json[pos] != ']') ++pos;
    return pos > start;
}

inline bool decodeRawString(const std::string& json, size_t start, size_t end,
                            std::string& raw) {
    if (start >= end || json[start] != '"') return false;
    size_t pos = start;
    if (!skipString(json, pos, end) || pos == 0 || pos > end) return false;
    raw.assign(json, start + 1, pos - start - 2);
    return true;
}

inline bool findMemberValueIn(const std::string& json, size_t start, size_t end,
                              const std::string& key,
                              size_t& valueStart, size_t& valueEnd) {
    size_t pos = start;
    skipWhitespace(json, pos, end);
    if (pos >= end) return false;

    if (json[pos] == '{') {
        ++pos;
        while (true) {
            skipWhitespace(json, pos, end);
            if (pos >= end) return false;
            if (json[pos] == '}') return false;
            const size_t keyStart = pos;
            std::string memberKey;
            if (!decodeRawString(json, keyStart, end, memberKey)) return false;
            if (!skipString(json, pos, end)) return false;
            skipWhitespace(json, pos, end);
            if (pos >= end || json[pos++] != ':') return false;
            skipWhitespace(json, pos, end);
            const size_t candidateStart = pos;
            if (!skipValue(json, pos, end)) return false;
            if (memberKey == key) {
                valueStart = candidateStart;
                valueEnd = pos;
                return true;
            }
            size_t nestedStart = candidateStart;
            size_t nestedEnd = pos;
            if (findMemberValueIn(json, nestedStart, nestedEnd, key,
                                  valueStart, valueEnd)) return true;
            skipWhitespace(json, pos, end);
            if (pos >= end) return false;
            if (json[pos] == ',') {
                ++pos;
                continue;
            }
            return false;
        }
    }

    if (json[pos] == '[') {
        ++pos;
        while (true) {
            skipWhitespace(json, pos, end);
            if (pos >= end) return false;
            if (json[pos] == ']') return false;
            const size_t candidateStart = pos;
            if (!skipValue(json, pos, end)) return false;
            if (findMemberValueIn(json, candidateStart, pos, key,
                                  valueStart, valueEnd)) return true;
            skipWhitespace(json, pos, end);
            if (pos >= end) return false;
            if (json[pos] == ',') {
                ++pos;
                continue;
            }
            return false;
        }
    }
    return false;
}

inline bool findMemberValue(const std::string& json, const std::string& key,
                            size_t& valueStart, size_t& valueEnd) {
    return findMemberValueIn(json, 0, json.size(), key, valueStart, valueEnd);
}

} // namespace detail

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
    size_t start = 0;
    size_t end = 0;
    if (!detail::findMemberValue(json, key, start, end) ||
        start >= end || json[start] != '"') return {};
    std::string raw;
    if (!detail::decodeRawString(json, start, end, raw)) return {};
    return raw;
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
    size_t start = 0;
    size_t end = 0;
    if (!detail::findMemberValue(json, key, start, end)) return fallback;
    try {
        const std::string raw = trim(json.substr(start, end - start));
        size_t consumed = 0;
        const double value = std::stod(raw, &consumed);
        return consumed == raw.size() ? value : fallback;
    } catch (...) {
        return fallback;
    }
}

inline bool extractJsonBoolValue(const std::string& json, const std::string& key, bool fallback = false) {
    size_t start = 0;
    size_t end = 0;
    if (!detail::findMemberValue(json, key, start, end)) return fallback;
    const std::string raw = trim(json.substr(start, end - start));
    if (raw == "true") return true;
    if (raw == "false") return false;
    return fallback;
}

inline bool hasJsonKey(const std::string& json, const std::string& key) {
    size_t start = 0;
    size_t end = 0;
    return detail::findMemberValue(json, key, start, end);
}

inline std::string extractArrayBlock(const std::string& json, const std::string& key) {
    size_t start = 0;
    size_t end = 0;
    if (!detail::findMemberValue(json, key, start, end) ||
        start >= end || json[start] != '[') return {};
    return json.substr(start, end - start);
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
