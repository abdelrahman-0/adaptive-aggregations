#include <cstdint>

#include "rpmalloc.h"

template <typename T>
struct RPMallocAllocator {
    using value_type = T;
    RPMallocAllocator() = default;
    ~RPMallocAllocator() = default;

    T* allocate(std::size_t n) { return static_cast<T*>(rpmalloc(n * sizeof(value_type))); }
    void deallocate(T* p, std::size_t n) { rpfree(p); }
};