# Shared source groups used by engine and test targets.
set(VELOX_FRAME_PIPELINE_SOURCES
    src/services/frame_pipeline.cpp
    src/services/frame_pipeline_queue.cpp
    src/services/frame_pipeline_support.cpp
    src/services/frame_pipeline_decoder.cpp
    src/services/frame_pipeline_filter.cpp
    src/services/frame_pipeline_compositor.cpp
    src/services/frame_pipeline_encoder.cpp
    src/services/frame_pipeline_stages.cpp
    src/render/frame_graph.cpp
    src/render/kernel_registry.cpp)
set(VELOX_FRAME_GRAPH_TEST_SOURCES
    src/render/frame_backend.cpp
    src/render/frame_graph.cpp
    src/render/kernel_registry.cpp)
set(VELOX_FRAME_OVERLAY_TEST_SOURCES
    src/render/frame_overlay.cpp
    src/render/frame_graph.cpp)
set(VELOX_RENDER_ENGINE_SOURCES
    src/core/render_engine.cpp
    src/core/render_engine_lifecycle.cpp
    src/core/render_engine_metrics.cpp
    src/core/render_engine_orchestrator.cpp
    src/core/render_engine_audio.cpp
    src/core/render_engine_timeline.cpp
    src/core/render_engine_sidecar.cpp
    src/core/render_engine_subtitles.cpp
    src/core/render_engine_packet.cpp
    src/core/canonical_video_profile.cpp
    src/core/render_engine_helpers.cpp)

# The language-neutral telemetry catalog is generated into the binary tree.
find_program(VELOX_GO_EXECUTABLE go)
set(VELOX_TELEMETRY_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(VELOX_TELEMETRY_GENERATED_HEADER
    "${VELOX_TELEMETRY_GENERATED_DIR}/velox/telemetry/catalog_generated.hpp")
if(VELOX_GO_EXECUTABLE)
    set(VELOX_SHARED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../shared")
    add_custom_command(
        OUTPUT "${VELOX_TELEMETRY_GENERATED_HEADER}"
        COMMAND "${VELOX_GO_EXECUTABLE}" run ./telemetry/cmd/cataloggen
                -input telemetry/schema/catalog.json
                -output "${VELOX_TELEMETRY_GENERATED_HEADER}"
        WORKING_DIRECTORY "${VELOX_SHARED_DIR}"
        DEPENDS
            "${VELOX_SHARED_DIR}/telemetry/schema/catalog.json"
            "${VELOX_SHARED_DIR}/telemetry/catalog.go"
            "${VELOX_SHARED_DIR}/telemetry/catalog_source.go"
            "${VELOX_SHARED_DIR}/telemetry/descriptor.go"
            "${VELOX_SHARED_DIR}/telemetry/fact_owner.go"
            "${VELOX_SHARED_DIR}/telemetry/cmd/cataloggen/main.go"
        COMMENT "Generating C++ telemetry catalog binding"
        VERBATIM)
else()
    set(VELOX_CHECKED_IN_TELEMETRY_HEADER
        "${CMAKE_CURRENT_SOURCE_DIR}/include/velox/telemetry/catalog_generated.hpp")
    if(NOT EXISTS "${VELOX_CHECKED_IN_TELEMETRY_HEADER}")
        message(FATAL_ERROR
            "Go is unavailable and the checked-in telemetry catalog binding is missing: "
            "${VELOX_CHECKED_IN_TELEMETRY_HEADER}")
    endif()
    add_custom_command(
        OUTPUT "${VELOX_TELEMETRY_GENERATED_HEADER}"
        COMMAND "${CMAKE_COMMAND}" -E make_directory
                "${VELOX_TELEMETRY_GENERATED_DIR}/velox/telemetry"
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different
                "${VELOX_CHECKED_IN_TELEMETRY_HEADER}"
                "${VELOX_TELEMETRY_GENERATED_HEADER}"
        DEPENDS "${VELOX_CHECKED_IN_TELEMETRY_HEADER}"
        COMMENT "Copying checked-in telemetry catalog binding"
        VERBATIM)
endif()
add_custom_target(velox_telemetry_catalog_codegen
    DEPENDS "${VELOX_TELEMETRY_GENERATED_HEADER}")
include_directories(BEFORE "${VELOX_TELEMETRY_GENERATED_DIR}")
