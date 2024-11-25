#pragma once

#include "concepts_common.h"
#include "defaults.h"

namespace concepts {

template <typename T, typename BufferPage>
concept is_partition_buffer = requires(T t, u64 part, BufferPage* page_to_evict) {
    { t.get_partition_page(part) } -> concepts::is_pointer;
    { t.evict(part, page_to_evict) } -> concepts::is_pointer;
};

} // namespace concepts
