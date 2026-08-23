#ifdef VELOX_ENABLE_LIBAV

#include "media_packet_pipeline_internal.hpp"

extern "C" {
#include <libavformat/avformat.h>
}

#include <algorithm>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace velox::media::packet {

InputSession* InputSessionRegistry::resolve(const fs::path& path, std::string& error) {
    const std::string key = path.lexically_normal().string();
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

bool InputSession::open(const fs::path& path, std::string& error) {
    if (demuxer_.isOpen()) {
        if (path_ == path) {
            return true;
        }
        demuxer_.close();
        keyframe_decisions_.clear();
    }
    if (!demuxer_.open(path, error)) {
        return false;
    }
    path_ = path;
    return true;
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

bool InputSessionRegistry::preopen(const std::vector<fs::path>& paths,
                                   std::string& error) {
    std::vector<fs::path> unique;
    std::vector<InputSession*> sessions;
    std::set<std::string> seen;
    unique.reserve(paths.size());
    sessions.reserve(paths.size());
    for (const auto& path : paths) {
        const std::string key = path.lexically_normal().string();
        if (!seen.insert(key).second || sessions_.find(key) != sessions_.end()) {
            continue;
        }
        unique.push_back(path);
        auto session = std::make_unique<InputSession>();
        sessions.push_back(session.get());
        sessions_.emplace(key, std::move(session));
    }
    if (unique.empty()) {
        return true;
    }

    constexpr std::size_t k_max_concurrent_opens = 8;
    std::vector<std::string> errors(unique.size());
    std::vector<bool> ok(unique.size(), false);
    for (std::size_t begin = 0; begin < unique.size(); begin += k_max_concurrent_opens) {
        const std::size_t end = std::min(unique.size(), begin + k_max_concurrent_opens);
        std::vector<std::thread> workers;
        workers.reserve(end - begin);
        for (std::size_t index = begin; index < end; ++index) {
            workers.emplace_back([&, index]() {
                ok[index] = sessions[index]->open(unique[index], errors[index]);
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
    }
    for (std::size_t index = 0; index < unique.size(); ++index) {
        if (!ok[index]) {
            error = errors[index];
            return false;
        }
    }
    return true;
}

} // namespace velox::media::packet

#endif // VELOX_ENABLE_LIBAV
