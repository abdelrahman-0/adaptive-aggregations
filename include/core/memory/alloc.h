// -----------------------------------------------------------------------------
// Maximilian Kuschewski (2023)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Modified by Abdelrahman Adel (2024)
// -----------------------------------------------------------------------------

#pragma once

#include <cstdint>
#include <sys/mman.h>

#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/exceptions/exceptions_alloc.h"

namespace memory {

template <bool huge = true>
struct MMapMemoryAllocator {
    constexpr static u64 min_physical_alloc() { return huge ? 2 * 1024 * 1024 : 4096; }

    template <typename T = void>
    static auto alloc(u64 size)
    {
        void* ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if constexpr (huge) {
            ::madvise(ptr, size, MADV_HUGEPAGE);
        }
        if (ptr == MAP_FAILED) {
            throw MMapAllocError(size);
        }
        return reinterpret_cast<T*>(ptr);
    }

    static void dealloc(void* ptr, u64 size) { ::munmap(ptr, size); }
};
} // namespace memory