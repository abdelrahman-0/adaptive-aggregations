#pragma once

#include "defaults.h"

namespace concepts {

template <typename T>
concept is_slot = (std::is_integral_v<T> or std::is_pointer_v<T>);

template <typename T>
concept is_sketch = requires(T t, u64 hash) {
    { t.update(hash) } -> is_void;
    { t.get_estimate() } -> std::unsigned_integral;
};

} // namespace concepts
