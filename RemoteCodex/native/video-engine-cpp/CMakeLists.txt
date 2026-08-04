cmake_minimum_required(VERSION 3.20)
project(velox_video_engine_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Deterministic, portable release flags. We deliberately avoid
# -march=native / -mtune=native: those pin the binary to the build host's
# CPU, which breaks the worker bundle's reproducibility contract (the
# Linux build host may differ from the worker host's CPU). -O3 + LTO is
# the highest-impact portable optimisation mix for the FFmpeg-heavy
# pipeline we link against.
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-O3)
    add_link_options(-flto)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
endif()

add_executable(velox_video_engine
    src/main.cpp
    src/cmd_full_video.cpp
    src/video_builder.cpp
    src/plan/render_plan_parser.cpp
    src/app/commands.cpp
    src/services/file_utils.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/core/render_engine.cpp
    src/telemetry/phase_recorder.cpp
)

target_include_directories(velox_video_engine PRIVATE include src include/velox)

# ─── F5 unit tests ──────────────────────────────────────────────────────
# Standalone test binary for the FFmpeg progress parser + sidecar
# writer. No GoogleTest dependency — runs the small in-repo expect()
# macros. CI gates on this target's exit code.
enable_testing()

add_executable(velox_ffmpeg_progress_parser_tests
    tests/test_ffmpeg_progress_parser.cpp
    src/services/ffmpeg_progress_parser.cpp
)

target_include_directories(velox_ffmpeg_progress_parser_tests
    PRIVATE include src include/velox)

# Tests must not require LTO/IPO from the main executable — keep the
# standalone binary small and stand-alone, exercised on every run.
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_ffmpeg_progress_parser_tests PRIVATE -O2)
endif()

add_test(NAME ffmpeg_progress_parser_tests
         COMMAND velox_ffmpeg_progress_parser_tests)

# ─── Block-1 phase recorder tests ──────────────────────────────────────
# Standalone test binary for the telemetry PhaseRecorder + ScopedPhase
# RAII helper. No GoogleTest dependency — the in-repo expect() macros.
add_executable(velox_phase_recorder_tests
    tests/test_phase_recorder.cpp
    src/telemetry/phase_recorder.cpp
    src/core/render_engine.cpp
    src/services/file_utils.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
)

target_include_directories(velox_phase_recorder_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_phase_recorder_tests PRIVATE -O2)
endif()

add_test(NAME phase_recorder_tests
         COMMAND velox_phase_recorder_tests)

# Integration smoke test for the mandatory looped-music contract. It uses
# the system FFmpeg binary to render a short source with loop=true and an
# omitted duration_seconds, then checks the 95-second output duration and
# process cleanup. The canonical Docker builder installs FFmpeg; local developers
# must provide FFmpeg and pgrep to run the full CTest suite.
add_executable(velox_looped_music_tests
    tests/test_looped_music.cpp
    src/core/render_engine.cpp
    src/services/file_utils.cpp
    src/services/media_utils.cpp
    src/services/ffmpeg_progress_parser.cpp
    src/telemetry/phase_recorder.cpp
)

target_include_directories(velox_looped_music_tests
    PRIVATE include src include/velox)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(velox_looped_music_tests PRIVATE -O2)
endif()

add_test(NAME looped_music_tests
         COMMAND velox_looped_music_tests)
set_tests_properties(looped_music_tests PROPERTIES
    ENVIRONMENT "VELOX_LOOPED_MUSIC_TEST=1")
