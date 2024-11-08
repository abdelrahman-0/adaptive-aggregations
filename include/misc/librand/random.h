#pragma once

#include <algorithm>
#include <gflags/gflags.h>
#include <limits>
#include <random>
#include <ranges>

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

template <concepts::is_iterable iter_t>
using iterable_entry_t = std::remove_reference<iter_t>::type::value_type;

template <bool is_primary_key, typename iter_t>
requires concepts::is_iterable<iter_t>
void random_iterable(iter_t& iterable, iterable_entry_t<iter_t> _min = std::numeric_limits<iterable_entry_t<iter_t>>::min(),
                     iterable_entry_t<iter_t> _max = std::numeric_limits<iterable_entry_t<iter_t>>::max())
{
    using T = iterable_entry_t<iter_t>;
    thread_local auto min = _min;
    thread_local auto max = _max;
    if constexpr (std::is_same_v<T, u64> and is_primary_key) {
        // first, sample unique groups from entire range
        thread_local bool generated_grps{false};
        thread_local std::uniform_int_distribution<u64> dist(min, max);
        thread_local std::vector<T> groups(FLAGS_groups);
        if (not generated_grps) {
            // could have collisions, ignored for now
            std::generate(std::begin(groups), std::end(groups), [&] { return dist(rng); });
            generated_grps = true;
        }
        // guarantee all groups are contained at least once
        thread_local auto copy_begin = 0ul;
        thread_local auto copy_end = groups.size();
        auto amount_to_copy = std::min(copy_end - copy_begin, iterable.size());
        if (amount_to_copy) {
            std::copy(groups.begin() + copy_begin, groups.begin() + copy_begin + amount_to_copy, iterable.begin());
            copy_begin += amount_to_copy;
        }
        if (amount_to_copy < iterable.size()) {
            // then sample remaining entries from groups
            thread_local std::uniform_int_distribution<u64> dist_idxs(0, FLAGS_groups - 1);
            std::generate(std::begin(iterable) + amount_to_copy, std::end(iterable), [&] { return groups[dist_idxs(rng)]; });
        }
    }
    else {
        thread_local std::uniform_int_distribution<unsigned char> dist_char(0, 255);
        auto* char_arr = reinterpret_cast<unsigned char*>(iterable.data());
        std::for_each(char_arr, char_arr + sizeof(iterable), [&](auto& byte) { byte = dist_char(rng); });
    }
}

template <typename T, std::size_t length>
requires(type_traits::is_tuple_v<T> or type_traits::is_array_v<T>)
void random_iterable(std::array<T, length>& column)
{
    for (auto& el : column) {
        el = random<T>();
    }
}

} // namespace librand
