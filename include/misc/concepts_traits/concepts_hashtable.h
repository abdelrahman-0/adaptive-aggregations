#pragma once

#include "defaults.h"

namespace concepts {

template <typename T>
concept is_slot = (std::is_integral_v<T> or std::is_pointer_v<T>);

template <typename T, typename BufferPage>
concept is_partition_buffer = requires(T t, u32 part, BufferPage* page_to_evict) {
    { t.get_partition_page(part) } -> concepts::is_pointer;
    { t.evict(part, page_to_evict) } -> concepts::is_pointer;
};

} // namespace concepts
