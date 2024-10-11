#pragma once

#include <limits>
#include <random>

#include "defaults.h"
#include "misc/concepts_traits/concepts_common.h"

namespace librand {

thread_local auto rng = std::mt19937{std::random_device{}()};

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
void random_column(std::array<T, length>& column, T min = std::numeric_limits<T>::min(),
                   T max = std::numeric_limits<T>::max())
{
    for (auto& el : column) {
        el = librand::random<std::remove_reference_t<decltype(el)>>(min, max);
    }
}

template <typename T, std::size_t length>
requires(type_traits::is_tuple_v<T> or type_traits::is_array_v<T>)
void random_column(std::array<T, length>& column)
{
    for (auto& el : column) {
        el = librand::random<std::remove_reference_t<decltype(el)>>();
    }
}

} // namespace librand
