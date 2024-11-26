// -----------------------------------------------------------------------------
// Maximilian Kuschewski (2023)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Modified by Abdelrahman Adel (2024)
// -----------------------------------------------------------------------------

#pragma once

#include <cstdint>
#include <cstdlib>
#include <jemalloc/jemalloc.h>
#include <sys/mman.h>

#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/exceptions/exceptions_alloc.h"

namespace mem {

template <bool huge>
struct MMapAllocator {

    template <typename T = void>
    static auto alloc(u64 size)
    {
        void* ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if constexpr (huge) {
            ::madvise(ptr, size, MADV_HUGEPAGE);
        }
        if (ptr == MAP_FAILED) {
            throw MMapAllocError{size};
        }
        return reinterpret_cast<T*>(ptr);
    }

    static void dealloc(void* ptr, u64 size = 0) noexcept
    {
        ::munmap(ptr, size);
    }

    static std::string get_type()
    {
        return "MMAP";
    }
};

template <bool huge>
struct JEMALLOCator {

    template <typename T = void>
    static auto alloc(u64 size)
    {
        void* ptr = mallocx(size, MALLOCX_ZERO);
        if (not ptr) {
            throw JEMALLOCError{size};
        }
        if constexpr (huge) {
            ::madvise(ptr, size, MADV_HUGEPAGE);
        }
        return reinterpret_cast<T*>(ptr);
    }

    static void dealloc(void* ptr, u64 = 0) noexcept
    {
        free(ptr);
    }

    static std::string get_type()
    {
        return "JEMALLOC";
    }
};

} // namespace mem
