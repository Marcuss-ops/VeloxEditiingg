# LibAV/FFmpeg dependency and source selection.

set(VELOX_PACKET_PIPELINE_FACADE_SOURCE
    src/services/media_packet_pipeline.cpp)
set(VELOX_PACKET_PIPELINE_COMPONENT_SOURCES
    src/services/media_packet_demuxer.cpp
    src/services/media_packet_sessions.cpp
    src/services/media_packet_rewriter.cpp
    src/services/media_packet_copy.cpp
    src/services/media_packet_muxer.cpp)
set(VELOX_MEDIA_UTILS_SOURCES
    src/services/media_utils_codec.cpp
    src/services/media_utils_stream.cpp
    src/services/media_utils_timestamp.cpp
    src/services/media_utils_pixel_format.cpp
    src/services/media_utils_audio.cpp
    src/services/media_utils_builders.cpp
    src/services/media_utils_execution.cpp
    src/services/media_utils_probe.cpp)
set(VELOX_SEGMENT_EXECUTION_SOURCES
    src/services/segment_execution.cpp
    src/services/segment_scheduler.cpp)
set(VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES)
if(VELOX_ENABLE_LIBAV)
    set(VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES
        src/services/segment_execution_libav.cpp)
endif()

if(VELOX_ENABLE_LIBAV)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(FFMPEG REQUIRED IMPORTED_TARGET
        libavformat
        libavcodec
        libavutil
        libswscale
    )
endif()

function(velox_link_ffmpeg target)
    if(VELOX_ENABLE_LIBAV)
        target_link_libraries(${target} PRIVATE PkgConfig::FFMPEG)
    endif()
endfunction()
