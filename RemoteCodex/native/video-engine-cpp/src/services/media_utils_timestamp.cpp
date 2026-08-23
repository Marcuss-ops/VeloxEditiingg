#include "media_utils_internal.hpp"

#include <algorithm>
#include <cmath>

namespace velox::media::detail {

int frameCountForDuration(double duration, int fps) {
    return std::max(1, static_cast<int>(std::round(duration * fps)));
}

} // namespace velox::media::detail
