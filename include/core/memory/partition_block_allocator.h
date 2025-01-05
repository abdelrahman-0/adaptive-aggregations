#pragma once

#include <vector>

#include "block_allocator.h"

namespace mem {

template <typename object_t, concepts::is_mem_allocator Alloc>
class PartitionBlockAllocatorNonConcurrent {
    using alloc_t = BlockAllocatorNonConcurrent<object_t, Alloc>;

    std::vector<alloc_t> part_allocs;

  public:
    PartitionBlockAllocatorNonConcurrent(u32 nparts, u32 block_sz, u64 max_allocations)
    {
        for (u32 part_no{0}; part_no < nparts; part_no++) {
            part_allocs.push_back(alloc_t(block_sz, max_allocations));
        }
    }

    [[nodiscard]]
    object_t* get_object(u32 part_no)
    {
        return part_allocs[part_no].get_object();
    }
};

template <typename object_t, concepts::is_mem_allocator Alloc>
class PartitionBlockAllocatorConcurrent {
    using alloc_t = BlockAllocatorConcurrent<object_t, Alloc>;

    std::vector<alloc_t> part_allocs;

  public:
    PartitionBlockAllocatorConcurrent(u32 nparts, u32 block_sz, u64 max_allocations)
    {
        for (u32 part_no{0}; part_no < nparts; part_no++) {
            part_allocs.push_back(alloc_t(block_sz, max_allocations));
        }
    }

    [[nodiscard]]
    object_t* get_object(u32 part_no)
    {
        return part_allocs[part_no].get_object();
    }
};

} // namespace mem
