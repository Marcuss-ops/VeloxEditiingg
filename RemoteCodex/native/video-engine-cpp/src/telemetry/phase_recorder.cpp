// phase_recorder.cpp — C++ engine-side event recorder implementation.
#include "velox/telemetry/phase_recorder.hpp"

#include <cctype>
#include <chrono>
#include <iomanip>
#include <ostream>
#include <sstream>

namespace {
class JsonValidator {
public:
    explicit JsonValidator(const std::string& input) : input_(input) {}

    bool validObject() {
        skipWhitespace();
        if (!parseObject()) return false;
        skipWhitespace();
        return pos_ == input_.size();
    }

private:
    void skipWhitespace() {
        while (pos_ < input_.size() && std::isspace(static_cast<unsigned char>(input_[pos_]))) ++pos_;
    }

    bool parseObject() {
        if (!consume('{')) return false;
        skipWhitespace();
        if (consume('}')) return true;
        while (true) {
            skipWhitespace();
            if (!parseString()) return false;
            skipWhitespace();
            if (!consume(':')) return false;
            skipWhitespace();
            if (!parseValue()) return false;
            skipWhitespace();
            if (consume('}')) return true;
            if (!consume(',')) return false;
        }
    }

    bool parseArray() {
        if (!consume('[')) return false;
        skipWhitespace();
        if (consume(']')) return true;
        while (true) {
            skipWhitespace();
            if (!parseValue()) return false;
            skipWhitespace();
            if (consume(']')) return true;
            if (!consume(',')) return false;
        }
    }

    bool parseString() {
        if (!consume('"')) return false;
        while (pos_ < input_.size()) {
            const unsigned char c = static_cast<unsigned char>(input_[pos_++]);
            if (c == '"') return true;
            if (c < 0x20) return false;
            if (c == '\\') {
                if (pos_ >= input_.size()) return false;
                const char escaped = input_[pos_++];
                if (escaped == 'u') {
                    for (int i = 0; i < 4; ++i) {
                        if (pos_ >= input_.size() ||
                            !std::isxdigit(static_cast<unsigned char>(input_[pos_++]))) return false;
                    }
                } else if (std::string("\\\"/bfnrt").find(escaped) == std::string::npos) {
                    return false;
                }
            }
        }
        return false;
    }

    bool parseNumber() {
        const size_t start = pos_;
        if (pos_ < input_.size() && input_[pos_] == '-') ++pos_;
        if (pos_ >= input_.size()) return false;
        if (input_[pos_] == '0') {
            ++pos_;
        } else {
            if (!std::isdigit(static_cast<unsigned char>(input_[pos_]))) return false;
            while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_]))) ++pos_;
        }
        if (pos_ < input_.size() && input_[pos_] == '.') {
            ++pos_;
            const size_t fraction = pos_;
            while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_]))) ++pos_;
            if (fraction == pos_) return false;
        }
        if (pos_ < input_.size() && (input_[pos_] == 'e' || input_[pos_] == 'E')) {
            ++pos_;
            if (pos_ < input_.size() && (input_[pos_] == '+' || input_[pos_] == '-')) ++pos_;
            const size_t exponent = pos_;
            while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_]))) ++pos_;
            if (exponent == pos_) return false;
        }
        return start != pos_;
    }

    bool parseLiteral(const char* literal) {
        const size_t length = std::char_traits<char>::length(literal);
        if (input_.compare(pos_, length, literal) != 0) return false;
        pos_ += length;
        return true;
    }

    bool parseValue() {
        skipWhitespace();
        if (pos_ >= input_.size()) return false;
        switch (input_[pos_]) {
            case '{': return parseObject();
            case '[': return parseArray();
            case '"': return parseString();
            case 't': return parseLiteral("true");
            case 'f': return parseLiteral("false");
            case 'n': return parseLiteral("null");
            default: return parseNumber();
        }
    }

    bool consume(char expected) {
        if (pos_ >= input_.size() || input_[pos_] != expected) return false;
        ++pos_;
        return true;
    }

    const std::string& input_;
    size_t pos_{0};
};

bool isValidJsonObjectShape(const std::string& value) {
    return !value.empty() && JsonValidator(value).validObject();
}

std::string escapeJsonString(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 4);
    const char hex[] = "0123456789abcdef";
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
} // namespace

namespace velox::telemetry {

namespace {
// ISO-8601 UTC wall stamp, millisecond precision: 2026-07-31T12:00:00.123Z
std::string formatUtc(const std::chrono::system_clock::time_point& tp) {
    std::time_t t = std::chrono::system_clock::to_time_t(tp);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  tp.time_since_epoch()).count() % 1000;
    std::tm utc{};
#if defined(_WIN32)
    gmtime_s(&utc, &t);
#else
    gmtime_r(&t, &utc);
#endif
    std::ostringstream os;
    os << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S") << "."
       << std::setfill('0') << std::setw(3) << (ms < 0 ? 0 : ms) << "Z";
    return os.str();
}
} // namespace

bool IsCanonicalOrigin(const std::string& s) {
    return s == kOriginMaster || s == kOriginWorker || s == kOriginEngine ||
           s == kOriginFFmpeg || s == kOriginUpload || s == kOriginValidation;
}

bool IsCanonicalScope(const std::string& s) {
    return s == kScopeJob || s == kScopeTask || s == kScopeAttempt ||
           s == kScopeSegment || s == kScopeAudioTrack ||
           s == kScopeSubtitleTrack || s == kScopeArtifact;
}

bool IsValidJsonObject(const std::string& s) {
    return isValidJsonObjectShape(s);
}

void PhaseEvent::AppendJson(std::ostringstream& out) const {
    out << "{";
    out << "\"origin\":\"" << escapeJsonString(origin) << "\"";
    out << ",\"scope\":\"" << escapeJsonString(scope) << "\"";
    out << ",\"component\":\"" << escapeJsonString(component) << "\"";
    out << ",\"action\":\"" << escapeJsonString(action) << "\"";
    out << ",\"phase\":\"" << escapeJsonString(phase) << "\"";
    out << ",\"event_type\":\"" << escapeJsonString(event_type) << "\"";
    out << ",\"event_name\":\"" << escapeJsonString(event_name) << "\"";
    out << ",\"event_index\":" << event_index;
    out << ",\"started_at\":\"" << escapeJsonString(started_at) << "\"";
    out << ",\"completed_at\":\"" << escapeJsonString(completed_at) << "\"";
    out << ",\"duration_ms\":" << duration_ms;
    out << ",\"status\":\"" << escapeJsonString(status) << "\"";
    out << ",\"error_code\":\"" << escapeJsonString(error_code) << "\"";
    out << ",\"error_message\":\"" << escapeJsonString(error_message) << "\"";
    out << ",\"bytes_in\":" << bytes_in;
    out << ",\"bytes_out\":" << bytes_out;
    out << ",\"frames\":" << frames;
    out << ",\"segment_index\":" << segment_index;
    out << ",\"track_kind\":\"" << escapeJsonString(track_kind) << "\"";
    out << ",\"track_index\":" << track_index;
    out << ",\"started_offset_ms\":" << started_offset_ms;
    out << ",\"finished_offset_ms\":" << finished_offset_ms;
    out << ",\"cpu_ms\":" << cpu_ms;
    out << ",\"queue_wait_ms\":" << queue_wait_ms;
    out << ",\"frames_in\":" << frames_in;
    out << ",\"frames_out\":" << frames_out;
    if (isValidJsonObjectShape(metadata_json)) {
        out << ",\"metadata\":" << metadata_json;
    }
    out << "}";
}

void PhaseRecorder::SetMetadataJSON(int64_t token, std::string metadata_json) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = inflight_.find(token);
    if (it == inflight_.end() || !it->second.active) return;
    if (!isValidJsonObjectShape(metadata_json)) return;
    it->second.partial.metadata_json = std::move(metadata_json);
}

void PhaseRecorder::SetDetailedMetrics(int64_t token, int32_t segment_index,
                                       std::string track_kind, int32_t track_index,
                                       double started_offset_ms, double finished_offset_ms,
                                       double cpu_ms, double queue_wait_ms,
                                       int64_t frames_in, int64_t frames_out) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = inflight_.find(token);
    if (it == inflight_.end() || !it->second.active) return;
    auto& event = it->second.partial;
    event.segment_index = segment_index;
    event.track_kind = std::move(track_kind);
    event.track_index = track_index;
    event.started_offset_ms = started_offset_ms;
    event.finished_offset_ms = finished_offset_ms;
    event.cpu_ms = cpu_ms;
    event.queue_wait_ms = queue_wait_ms;
    event.frames_in = frames_in;
    event.frames_out = frames_out;
}

void PhaseRecorder::Reset() {
    std::lock_guard<std::mutex> lock(mu_);
    events_.clear();
    inflight_.clear();
    indexes_.clear();
    next_token_ = 0;
}

int64_t PhaseRecorder::Begin(std::string origin, std::string scope,
                             std::string component, std::string action,
                             std::string phase, std::string event_type,
                             std::string event_name) {
    if (!IsCanonicalOrigin(origin)) origin = kOriginEngine;
    if (!IsCanonicalScope(scope)) scope = kScopeAttempt;

    std::lock_guard<std::mutex> lock(mu_);
    PhaseEvent ev;
    ev.origin = std::move(origin);
    ev.scope = std::move(scope);
    ev.component = std::move(component);
    ev.action = std::move(action);
    ev.phase = std::move(phase);
    ev.event_type = std::move(event_type);
    ev.event_name = std::move(event_name);
    ev.started_at = UtcNowIso8601();
    ev.event_index = indexes_[ev.origin]++;
    ev.status = kStatusOk;

    int64_t token = next_token_++;
    Inflight& in = inflight_[token];
    in.start_mono = std::chrono::steady_clock::now();
    in.started_at = ev.started_at;
    in.partial = std::move(ev);
    in.active = true;
    return token;
}

void PhaseRecorder::Complete(int64_t token, int64_t bytes_in, int64_t bytes_out,
                             int64_t frames, const std::string& status,
                             const std::string& error_code,
                             const std::string& error_message) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = inflight_.find(token);
    if (it == inflight_.end() || !it->second.active) return;

    auto end_mono = std::chrono::steady_clock::now();
    PhaseEvent ev = std::move(it->second.partial);
    ev.bytes_in = bytes_in;
    ev.bytes_out = bytes_out;
    ev.frames = frames;
    ev.status = status;
    ev.error_code = error_code;
    ev.error_message = error_message;
    ev.completed_at = UtcNowIso8601();
    ev.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         end_mono - it->second.start_mono)
                         .count();
    if (ev.event_type.empty()) {
        ev.event_type = (status == kStatusFailed) ? "failed" : "completed";
    }
    it->second.active = false;
    events_.push_back(std::move(ev));
    inflight_.erase(it);
}

void PhaseRecorder::Abort(int64_t token, const std::string& error_code,
                          const std::string& error_message) {
    Complete(token, 0, 0, 0, kStatusFailed, error_code, error_message);
}

void PhaseRecorder::Emit(std::string origin, std::string scope, std::string component,
                         std::string action, std::string phase,
                         const std::string& status, const std::string& event_type,
                         const std::string& error_code,
                         const std::string& error_message) {
    if (!IsCanonicalOrigin(origin)) origin = kOriginEngine;
    if (!IsCanonicalScope(scope)) scope = kScopeAttempt;

    std::lock_guard<std::mutex> lock(mu_);
    PhaseEvent ev;
    ev.origin = std::move(origin);
    ev.scope = std::move(scope);
    ev.component = std::move(component);
    ev.action = std::move(action);
    ev.phase = std::move(phase);
    ev.event_type = event_type.empty()
                        ? ((status == kStatusFailed) ? "failed" : "completed")
                        : event_type;
    ev.event_index = indexes_[ev.origin]++;
    ev.started_at = UtcNowIso8601();
    ev.completed_at = ev.started_at;
    ev.status = status;
    ev.error_code = error_code;
    ev.error_message = error_message;
    events_.push_back(std::move(ev));
}

std::vector<PhaseEvent> PhaseRecorder::Snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return events_;
}

size_t PhaseRecorder::Count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return events_.size();
}

std::string PhaseRecorder::UtcNowIso8601() {
    return formatUtc(std::chrono::system_clock::now());
}

// ── ScopedPhase ────────────────────────────────────────────────────────────

ScopedPhase::ScopedPhase(PhaseRecorder& recorder, std::string origin,
                         std::string scope, std::string component,
                         std::string action, std::string phase,
                         std::string event_type, std::string event_name)
    : recorder_(&recorder),
      token_(recorder.Begin(std::move(origin), std::move(scope),
                            std::move(component), std::move(action),
                            std::move(phase), std::move(event_type),
                            std::move(event_name))) {}

ScopedPhase::~ScopedPhase() { finish(); }

void ScopedPhase::SetMetadataJSON(std::string metadata_json) {
    if (!done_ && recorder_ != nullptr) {
        recorder_->SetMetadataJSON(token_, std::move(metadata_json));
    }
}

void ScopedPhase::SetDetailedMetrics(int32_t segment_index, std::string track_kind,
                                     int32_t track_index, double started_offset_ms,
                                     double finished_offset_ms, double cpu_ms,
                                     double queue_wait_ms, int64_t frames_in,
                                     int64_t frames_out) {
    if (!done_ && recorder_ != nullptr) {
        recorder_->SetDetailedMetrics(token_, segment_index, std::move(track_kind),
                                      track_index, started_offset_ms, finished_offset_ms,
                                      cpu_ms, queue_wait_ms, frames_in, frames_out);
    }
}

void ScopedPhase::Complete(int64_t bytes_in, int64_t bytes_out, int64_t frames,
                           const std::string& status, const std::string& error_code,
                           const std::string& error_message) {
    if (done_) return;
    done_ = true;
    recorder_->Complete(token_, bytes_in, bytes_out, frames, status, error_code,
                        error_message);
}

void ScopedPhase::Abort(const std::string& error_code,
                        const std::string& error_message) {
    Complete(0, 0, 0, kStatusFailed, error_code, error_message);
}

ScopedPhase::ScopedPhase(ScopedPhase&& other) noexcept
    : recorder_(other.recorder_), token_(other.token_), done_(other.done_) {
    other.recorder_ = nullptr;
    other.done_ = true;
}

ScopedPhase& ScopedPhase::operator=(ScopedPhase&& other) noexcept {
    if (this != &other) {
        finish();
        recorder_ = other.recorder_;
        token_ = other.token_;
        done_ = other.done_;
        other.recorder_ = nullptr;
        other.done_ = true;
    }
    return *this;
}

void ScopedPhase::finish() {
    if (!done_ && recorder_ != nullptr) {
        done_ = true;
        recorder_->Complete(token_, 0, 0, 0, kStatusOk);
    }
}

} // namespace velox::telemetry
