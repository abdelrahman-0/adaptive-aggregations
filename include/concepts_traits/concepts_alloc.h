#pragma once

#include "defaults.h"

namespace concepts {

// mini allocator_traits
template <typename T>
concept is_allocator = requires(u64 size, void* ptr) {
    { T::alloc(size) } -> std::convertible_to<decltype(ptr)>;
    { T::dealloc(ptr, size) };
};

} // namespace concepts
