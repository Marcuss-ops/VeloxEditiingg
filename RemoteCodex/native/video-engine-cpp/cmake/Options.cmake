# Project-wide language and optimization policy.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optional in-process LibAV backend. OFF preserves the ffprobe/legacy path;
# ON enables the in-process MediaProbe and packet-copy pipeline.
option(VELOX_ENABLE_LIBAV
    "Build with in-process libavformat/libavcodec/libavutil (MediaProbe + packet mux)"
    OFF)

# Propagate the build flag to every engine/test target.
add_compile_definitions($<$<BOOL:${VELOX_ENABLE_LIBAV}>:VELOX_ENABLE_LIBAV>)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Deterministic portable release flags. Do not use host-specific CPU tuning:
# worker bundles must run on CPUs different from the build host.
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -O3
        -ffile-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=.
        -ffile-prefix-map=${CMAKE_CURRENT_BINARY_DIR}=.
    )
    add_link_options(-flto -Wl,--build-id=none)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
endif()
