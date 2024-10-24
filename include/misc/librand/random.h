#pragma once

#include <gflags/gflags.h>
#include <limits>
#include <random>

#include "defaults.h"
#include "misc/concepts_traits/concepts_common.h"

DECLARE_uint64(groups);

namespace librand {

thread_local auto rng = std::mt19937{0};

template <concepts::is_char T>
ALWAYS_INLINE T random(T min = 33 /* ! */, T max = 126 /* ~ */)
{
    thread_local std::uniform_int_distribution<T> dist(min, max);
    return dist(rng);
}

template <std::integral T>
requires(not concepts::is_char<T>)
ALWAYS_INLINE T random(T min = std::numeric_limits<T>::min(), T max = std::numeric_limits<T>::max())
{
    thread_local std::uniform_int_distribution<T> dist(min, max);
    return dist(rng);
}

template <std::floating_point T>
ALWAYS_INLINE T random(T min = std::numeric_limits<T>::min(), T max = std::numeric_limits<T>::max())
{
    thread_local std::uniform_real_distribution<T> dist(min, max);
    return dist(rng);
}

template <concepts::is_array T>
ALWAYS_INLINE T random()
{
    T arr{};
    for (auto& el : arr)
        el = random<typename T::value_type>();
    return arr;
}

template <concepts::is_tuple T>
ALWAYS_INLINE T random()
{
    T tup{};
    std::apply([](auto&& el) { el = random<decltype(el)>(); }, tup);
    return tup;
}

template <typename T, std::size_t length>
requires(not type_traits::is_tuple_v<T> and not type_traits::is_array_v<T>)
void random_column(std::array<T, length>& column)
{
    thread_local auto min = std::numeric_limits<T>::min();
    thread_local auto max = std::numeric_limits<T>::max();
    if constexpr (std::is_integral_v<T>) {
        // sample groups from all range
        thread_local bool generated_grps{false};
        thread_local std::uniform_int_distribution<T> dist(min, max);
        thread_local std::vector<T> groups(FLAGS_groups);
        if (not generated_grps) {
            std::generate(std::begin(groups), std::end(groups), [&] { return dist(rng); });
            generated_grps = true;
        }
        // sample from groups
        thread_local std::uniform_int_distribution<u64> dist_idxs(0, FLAGS_groups - 1);
        std::generate(std::begin(column), std::end(column), [&] { return groups[dist_idxs(rng)]; });
    }
    else if constexpr (std::is_floating_point_v<T>) {
        thread_local std::uniform_real_distribution<T> dist(min, max);
        std::generate(std::begin(column), std::end(column), [&] { return dist(rng); });
    }
    else {
        __builtin_unreachable();
    }
}

template <typename T, std::size_t length>
requires(type_traits::is_tuple_v<T> or type_traits::is_array_v<T>)
void random_column(std::array<T, length>& column)
{
    for (auto& el : column) {
        el = random<T>();
    }
}

} // namespace librand
