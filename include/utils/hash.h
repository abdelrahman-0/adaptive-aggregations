#pragma once

#include <array>
#include <cstdint>
#include <functional>

template <std::integral T>
inline uint64_t murmur_hash(T k) {
    const uint64_t m = 0xc6a4a7935bd1e995ull;
    const int r = 47;
    uint64_t h = 0x8445d61a4e774912ull ^ (8 * m);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

// Effectively implements Boost's hash_combine using WYHash as hash function:
// https://www.boost.org/doc/libs/1_36_0/boost/functional/hash/hash.hpp
template <std::integral Attribute, std::size_t N>
uint64_t hash_tuple(const std::array<Attribute, N>& tuple) noexcept {
    uint64_t seed = 0;
    for (auto a : tuple) {
        seed ^= murmur_hash(a) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}
