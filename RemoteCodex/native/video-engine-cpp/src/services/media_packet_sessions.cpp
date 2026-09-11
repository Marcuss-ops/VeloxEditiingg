#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/channel_layout.h>
#include <libavutil/version.h>
}

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace velox::media::packet {

namespace {

std::string channelLayoutName(const AVChannelLayout& layout) {
    char buffer[256]{};
    if (av_channel_layout_describe(&layout, buffer, sizeof(buffer)) >= 0) {
        return buffer;
    }
    return {};
}

bool rawAacContainer(const std::string& format_name) {
    std::string normalized;
    normalized.reserve(format_name.size());
    for (const char value : format_name) {
        normalized.push_back(static_cast<char>(
            std::tolower(static_cast<unsigned char>(value))));
    }
    return normalized == "aac" || normalized.find("adts") != std::string::npos ||
           normalized.find("latm") != std::string::npos ||
           normalized.find("loas") != std::string::npos;
}

} // namespace

InputSession* InputSessionRegistry::resolve(const fs::path& path, std::string& error) {
    const std::string key = path.lexically_normal().string();
    if (pending_opens_.find(key) != pending_opens_.end()) {
        return waitForPending(key, error);
    }
    auto existing = sessions_.find(key);
    if (existing != sessions_.end()) {
        return existing->second.get();
    }
    auto session = std::make_unique<InputSession>();
    if (!session->open(path, error)) {
        return nullptr;
    }
    InputSession* result = session.get();
    sessions_.emplace(key, std::move(session));
    return result;
}

InputSession* InputSessionRegistry::waitForPending(const std::string& key,
                                                    std::string& error) {
    const auto pending = pending_opens_.find(key);
    if (pending == pending_opens_.end()) {
        const auto existing = sessions_.find(key);
        return existing == sessions_.end() ? nullptr : existing->second.get();
    }
    const auto& state = pending->second;
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->ready.wait(lock, [&]() { return state->done; });
        if (!state->success) {
            error = state->error;
            return nullptr;
        }
    }
    const auto existing = sessions_.find(key);
    if (existing == sessions_.end()) {
        error = "input session disappeared after background open: " + key;
        return nullptr;
    }
    return existing->second.get();
}

void InputSessionRegistry::joinOpenWorkers() {
    for (auto& worker : open_workers_) {
        if (worker.joinable()) worker.join();
    }
    open_workers_.clear();
}

InputSessionRegistry::~InputSessionRegistry() {
    joinOpenWorkers();
}

bool InputSession::open(const fs::path& path, std::string& error,
                        bool metadata_certified) {
    if (demuxer_.isOpen()) {
        if (path_ == path) {
            return true;
        }
        demuxer_.close();
        keyframe_decisions_.clear();
    }
    if (!demuxer_.open(path, error, metadata_certified)) {
        return false;
    }
    path_ = path;
    return true;
}

FinalAudioMetadata InputSession::finalAudioMetadata(int stream_index) const {
    FinalAudioMetadata metadata;
    if (!demuxer_.isOpen()) return metadata;
    const AVStream* stream = demuxer_.stream(stream_index);
    if (stream == nullptr || stream->codecpar == nullptr ||
        stream->codecpar->codec_type != AVMEDIA_TYPE_AUDIO) {
        return metadata;
    }

    const AVCodecParameters* parameters = stream->codecpar;
    metadata.codec = avcodec_get_name(parameters->codec_id);
    metadata.sample_rate = parameters->sample_rate;
#if LIBAVUTIL_VERSION_MAJOR >= 57
    metadata.channels = parameters->ch_layout.nb_channels;
    if (parameters->ch_layout.nb_channels > 0 &&
        parameters->ch_layout.order != AV_CHANNEL_ORDER_UNSPEC) {
        metadata.channel_layout = channelLayoutName(parameters->ch_layout);
    }
#else
    metadata.channels = parameters->channels;
    if (parameters->channel_layout != 0) {
        char buffer[64]{};
        av_get_channel_layout_string(
            buffer, sizeof(buffer), parameters->channels, parameters->channel_layout);
        metadata.channel_layout = buffer;
    }
#endif
    if (const AVFormatContext* context = demuxer_.raw();
        context != nullptr && context->iformat != nullptr &&
        context->iformat->name != nullptr) {
        metadata.format_name = context->iformat->name;
    }
    if (validTimestamp(stream->duration)) {
        const double duration = static_cast<double>(stream->duration) *
            av_q2d(stream->time_base);
        if (std::isfinite(duration) && duration > 0.0) {
            metadata.duration_seconds = duration;
            metadata.duration_verified = true;
        }
    }
    if (validTimestamp(stream->start_time)) {
        const double start_time = static_cast<double>(stream->start_time) *
            av_q2d(stream->time_base);
        if (std::isfinite(start_time)) {
            metadata.start_time_seconds = start_time;
            metadata.start_time_verified = true;
        }
    }
    metadata.extradata_verified = parameters->extradata_size > 0 &&
        parameters->extradata != nullptr;
    metadata.container_verified = !rawAacContainer(metadata.format_name);
    metadata.metadata_verified =
        !metadata.codec.empty() && metadata.sample_rate > 0 && metadata.channels > 0 &&
        !metadata.channel_layout.empty() && metadata.duration_verified &&
        std::isfinite(metadata.duration_seconds) && metadata.duration_seconds > 0.0 &&
        metadata.start_time_verified && std::isfinite(metadata.start_time_seconds);
    return metadata;
}

bool InputSession::seekToTimestampUs(int stream_index, int64_t timestamp_us,
                                     std::string& error) {
    return demuxer_.seekToTimestampUs(stream_index, timestamp_us, error);
}

bool InputSession::sourceWindowStartsOnKeyframe(int input_stream_index,
                                                int64_t source_in_us,
                                                std::string& error) {
    if (source_in_us < 0) {
        error = "copy-only source_in_us must be non-negative";
        return false;
    }
    const auto cache_key = std::make_pair(input_stream_index, source_in_us);
    const auto cached = keyframe_decisions_.find(cache_key);
    if (cached != keyframe_decisions_.end()) {
        if (!cached->second) {
            error = "copy-only source window must start on an exact video keyframe: " +
                path_.string() + " source_in_us=" + std::to_string(source_in_us);
        }
        return cached->second;
    }
    if (!demuxer_.isOpen()) {
        error = "input session is not open";
        return false;
    }
    if (input_stream_index < 0 ||
        static_cast<unsigned int>(input_stream_index) >= demuxer_.raw()->nb_streams) {
        error = "stream index is invalid for " + path_.string();
        return false;
    }
    const AVStream* input_stream = demuxer_.stream(input_stream_index);
    if (input_stream == nullptr || input_stream->codecpar == nullptr ||
        input_stream->codecpar->codec_type != AVMEDIA_TYPE_VIDEO) {
        error = "requested keyframe stream is missing from " + path_.string();
        return false;
    }
    if (!demuxer_.seekToTimestampUs(input_stream_index, source_in_us, error)) {
        return false;
    }

    const int64_t source_start = validTimestamp(input_stream->start_time)
        ? input_stream->start_time : 0;
    AVPacket* packet = av_packet_alloc();
    if (packet == nullptr) {
        error = "av_packet_alloc failed while checking keyframe alignment";
        return false;
    }
    bool found = false;
    bool eof = false;
    std::string read_error;
    while (!eof) {
        if (!demuxer_.readFrame(*packet, eof, read_error)) {
            error = "av_read_frame(" + path_.string() +
                ") while checking keyframe alignment: " + read_error;
            av_packet_free(&packet);
            return false;
        }
        if (eof) {
            break;
        }
        if (packet->stream_index == input_stream_index &&
            (packet->flags & AV_PKT_FLAG_KEY) != 0) {
            const int64_t packet_us = relativeTimestamp(
                packet->pts != AV_NOPTS_VALUE ? packet->pts : packet->dts,
                source_start, input_stream->time_base);
            if (packet_us == source_in_us) {
                found = true;
                av_packet_unref(packet);
                break;
            }
        }
        av_packet_unref(packet);
    }
    av_packet_free(&packet);
    keyframe_decisions_[cache_key] = found;
    if (!found) {
        error = "copy-only source window must start on an exact video keyframe: " +
            path_.string() + " source_in_us=" + std::to_string(source_in_us);
    }
    return found;
}

bool InputSessionRegistry::preopenAsync(
    const std::vector<InputSessionRegistry::OpenRequest>& requests,
    std::string& error) {
    constexpr std::size_t k_max_concurrent_opens = 8;
    struct OpenTask {
        OpenRequest request;
        InputSession* session{nullptr};
        std::shared_ptr<OpenState> state;
    };
    std::vector<OpenRequest> unique;
    std::map<std::string, std::size_t> indices;
    unique.reserve(requests.size());
    for (const auto& request : requests) {
        const std::string key = request.path.lexically_normal().string();
        if (pending_opens_.find(key) != pending_opens_.end() ||
            sessions_.find(key) != sessions_.end()) continue;
        const auto [it, inserted] = indices.emplace(key, unique.size());
        if (!inserted) {
            // One path may be reused by a certified video and an
            // uncertified audio use. The conservative result is a full probe.
            unique[it->second].metadata_certified =
                unique[it->second].metadata_certified && request.metadata_certified;
            continue;
        }
        unique.push_back(request);
    }
    if (unique.empty()) return true;
    // Cursor look-ahead normally adds one short-lived worker at a time. Keep
    // completed joinable workers bounded too; this permits video and audio
    // look-ahead to overlap without ever growing an unbounded thread pool.
    if (open_workers_.size() + unique.size() > k_max_concurrent_opens) {
        joinOpenWorkers();
    }

    auto tasks = std::make_shared<std::vector<OpenTask>>();
    tasks->reserve(unique.size());
    for (const auto& request : unique) {
        const std::string key = request.path.lexically_normal().string();
        auto session = std::make_unique<InputSession>();
        InputSession* session_ptr = session.get();
        sessions_.emplace(key, std::move(session));
        auto state = std::make_shared<OpenState>();
        pending_opens_.emplace(key, state);
        tasks->push_back(OpenTask{request, session_ptr, std::move(state)});
    }
    const std::size_t worker_count = std::min(k_max_concurrent_opens, tasks->size());
    auto next = std::make_shared<std::atomic<std::size_t>>(0);
    for (std::size_t worker_index = 0; worker_index < worker_count; ++worker_index) {
        open_workers_.emplace_back([tasks, next]() {
            while (true) {
                const std::size_t index = next->fetch_add(1, std::memory_order_relaxed);
                if (index >= tasks->size()) return;
                auto& task = (*tasks)[index];
                std::string task_error;
                const bool success = task.session->open(
                    task.request.path, task_error,
                    task.request.metadata_certified);
                {
                    std::lock_guard<std::mutex> lock(task.state->mutex);
                    task.state->success = success;
                    task.state->error = std::move(task_error);
                    task.state->done = true;
                }
                task.state->ready.notify_all();
            }
        });
    }
    return true;
}

bool InputSessionRegistry::preopen(const std::vector<OpenRequest>& requests,
                                   std::string& error) {
    if (!preopenAsync(requests, error)) return false;
    bool success = true;
    for (const auto& request : requests) {
        if (resolve(request.path, error) == nullptr) success = false;
    }
    joinOpenWorkers();
    return success;
}

bool InputSessionRegistry::preopen(const std::vector<fs::path>& paths,
                                   std::string& error) {
    std::vector<OpenRequest> requests;
    requests.reserve(paths.size());
    for (const auto& path : paths) requests.push_back(OpenRequest{path, false});
    return preopen(requests, error);
}

} // namespace velox::media::packet

#endif // VELOX_ENABLE_LIBAV
