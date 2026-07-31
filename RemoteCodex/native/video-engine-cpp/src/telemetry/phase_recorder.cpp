// phase_recorder.cpp — C++ engine-side event recorder implementation.
#include "velox/telemetry/phase_recorder.hpp"

#include <chrono>
#include <iomanip>
#include <ostream>
#include <sstream>

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

void PhaseEvent::AppendJson(std::ostringstream& out) const {
    out << "{";
    out << "\"origin\":\"" << origin << "\"";
    out << ",\"scope\":\"" << scope << "\"";
    out << ",\"component\":\"" << component << "\"";
    out << ",\"action\":\"" << action << "\"";
    out << ",\"phase\":\"" << phase << "\"";
    out << ",\"event_type\":\"" << event_type << "\"";
    out << ",\"event_name\":\"" << event_name << "\"";
    out << ",\"event_index\":" << event_index;
    out << ",\"started_at\":\"" << started_at << "\"";
    out << ",\"completed_at\":\"" << completed_at << "\"";
    out << ",\"duration_ms\":" << duration_ms;
    out << ",\"status\":\"" << status << "\"";
    out << ",\"error_code\":\"" << error_code << "\"";
    out << ",\"error_message\":\"" << error_message << "\"";
    out << ",\"bytes_in\":" << bytes_in;
    out << ",\"bytes_out\":" << bytes_out;
    out << ",\"frames\":" << frames;
    if (!metadata_json.empty()) {
        out << ",\"metadata\":" << metadata_json;
    }
    out << "}";
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
