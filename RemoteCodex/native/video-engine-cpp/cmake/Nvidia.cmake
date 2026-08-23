# NVIDIA integration boundary.
#
# The current engine exposes CUDA/NVENC selection at runtime through codec and
# frame-backend code, but this project does not enable CUDA language/toolkit
# discovery or add NVIDIA-specific targets in its existing CMake graph.
# Keeping this module explicit documents that boundary without changing target
# names, compiler flags, or dependency resolution.
set(VELOX_NVIDIA_ENABLED FALSE)
