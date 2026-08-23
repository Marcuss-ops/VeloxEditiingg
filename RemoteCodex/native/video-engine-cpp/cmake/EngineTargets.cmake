# Main production engine target.
add_executable(velox_video_engine
    src/main.cpp
    src/cmd_full_video.cpp
    src/video_builder.cpp
    src/plan/render_plan_parser.cpp
    src/plan/render_plan_parser_helpers.cpp
    src/plan/render_plan_parser_v1.cpp
    src/plan/render_plan_parser_v2.cpp
    src/app/commands.cpp
    src/services/file_utils.cpp
    src/services/io_counters.cpp
    src/services/media_probe.cpp
    ${VELOX_PACKET_PIPELINE_FACADE_SOURCE}
    ${VELOX_PACKET_PIPELINE_COMPONENT_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_LIBAV_SOURCES}
    ${VELOX_SEGMENT_EXECUTION_SOURCES}
    ${VELOX_MEDIA_UTILS_SOURCES}
    ${VELOX_FRAME_PIPELINE_SOURCES}
    src/audio/audio_plan.cpp
    src/audio/audio_benchmark.cpp
    src/services/ffmpeg_progress_parser.cpp
    ${VELOX_RENDER_ENGINE_SOURCES}
    src/core/execution_plan.cpp
    src/telemetry/emitter.cpp
)

target_include_directories(velox_video_engine PRIVATE include src include/velox)
velox_link_ffmpeg(velox_video_engine)
add_dependencies(velox_video_engine velox_telemetry_catalog_codegen)
