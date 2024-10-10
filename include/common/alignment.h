#pragma once

#include "defaults.h"

template <typename T>
struct alignas(CACHELINE_SZ) CachelineAlignedAtomic {
    std::atomic<T> val;

    CachelineAlignedAtomic& operator=(T other)
    {
        val = other;
        return *this;
    }
};
