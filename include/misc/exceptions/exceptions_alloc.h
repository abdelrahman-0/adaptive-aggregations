#pragma once

#include <cstring>
#include <string>
#include <system_error>

#include "defaults.h"

class AllocError : public std::runtime_error {
  private:
    static auto bytes_to_GiBs(u64 sz) { return std::to_string(1.0 * sz / 1e6); }

  public:
    explicit AllocError(const std::string& type, u64 alloc_sz)
        : std::runtime_error("Could not allocate memory using " + type + " allocator\n" + "Tried allocating " +
                             bytes_to_GiBs(alloc_sz) + " GiBs and got: " + std::string(strerror(errno)))
    {
    }
};

class MMapAllocError : public AllocError {
  public:
    explicit MMapAllocError(u64 alloc_sz) : AllocError("mmap", alloc_sz) {}
};
