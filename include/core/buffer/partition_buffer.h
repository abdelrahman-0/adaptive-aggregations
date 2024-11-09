#pragma once

#include <atomic>
#include <tbb/concurrent_vector.h>
#include <vector>

#include "defaults.h"

namespace buf {

template <typename BufferPage, bool is_concurrent>
struct PartitionBuffer {
    std::conditional_t<is_concurrent, std::vector<tbb::concurrent_vector<BufferPage*>>, std::vector<std::vector<BufferPage*>>> partition_pages;

    explicit PartitionBuffer(u32 npartitions) : partition_pages(npartitions)
    {
    }

    void add_page(BufferPage* page, u32 part_no)
    {
        partition_pages[part_no].push_back(page);
    }
};

} // namespace buf
