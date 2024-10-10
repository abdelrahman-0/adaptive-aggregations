#pragma once

#include <array>
#include <cstdint>
#include <functional>

inline uint64_t murmur_hash(std::integral auto k)
{
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

inline uint64_t char_hash(unsigned char k) { return k & 1; }

inline uint64_t uint32_hash(uint32_t k) { return k & 1; }

// Effectively implements Boost's hash_combine using WYHash as hash function:
// https://www.boost.org/doc/libs/1_36_0/boost/functional/hash/hash.hpp
template <std::integral Attribute, std::size_t N>
u64 hash_tuple(const std::array<Attribute, N>& tuple) noexcept
{
    uint64_t seed = 0;
    for (auto a : tuple) {
        seed ^= murmur_hash(a) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}

template <typename T>
requires requires(T t) { t.hash(); }
inline u64 hash_key(T x)
{
    return x.hash();
}

template <typename T>
inline u64 hash_key(T x)
{
    return x;
}

template <typename T, typename... Args>
inline u64 hash_key(T first, Args... args)
{
    u64 acc = hash_key(args...);
    acc ^= (hash_key(first) + 0x517cc1b727220a95ul + (acc << 6) + (acc >> 2));
    return acc;
}

template <u16 idx = 0, typename... Ts>
inline u64 hash_tuple(const std::tuple<Ts...>& tup)
{
    u64 acc;
    if constexpr (idx < sizeof...(Ts) - 1) {
        acc = hash_tuple<idx + 1>(tup);
        acc ^= (hash_key(std::get<idx>(tup)) + 0x517cc1b727220a95ul + (acc << 6) + (acc >> 2));
    }
    else {
        acc = hash_key(std::get<idx>(tup));
    }
    return acc;
}
