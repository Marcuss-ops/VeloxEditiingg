#pragma once

#include "velox/services/media_packet_pipeline.hpp"
#include "velox/services/media_packet_components.hpp"

namespace velox::media {

// Copy-pipeline implementation consumed by the public MediaPacketPipeline
// facade. The request validation, session preparation, packet collection and
// final mux remain internal to the services layer.
bool runCopyOnlyMux(const CopyOnlyMuxRequest& request,
                    CopyOnlyMuxResult* result);

} // namespace velox::media

namespace velox::media::packet {

std::string ffmpegError(int error);
int64_t rescale(int64_t value, AVRational source, AVRational destination);
int64_t relativeTimestamp(int64_t timestamp,
                          int64_t source_start,
                          AVRational input_time_base);

} // namespace velox::media::packet
