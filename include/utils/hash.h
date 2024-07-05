#pragma once

#include <cstdint>

inline uint64_t murmur_hash(uint64_t k)
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