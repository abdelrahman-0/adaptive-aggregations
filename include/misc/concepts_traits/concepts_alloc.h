#pragma once

#include "concepts_common.h"
#include "defaults.h"

namespace concepts {

// mini allocator_traits
template <typename T>
concept is_mem_allocator = requires(u64 size, void* ptr) {
    { T::alloc(size) } -> concepts::is_pointer;
    { T::dealloc(ptr, size) };
};

// block allocator
template <typename T, typename BufferPage>
concept is_block_allocator = requires(T t, u64 size, BufferPage* ptr) {
    { t.get_page() } -> concepts::is_pointer;
    { t.return_page(ptr) } -> concepts::is_void;
};

} // namespace concepts
