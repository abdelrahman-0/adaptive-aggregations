#pragma once

#include <vector>

#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"

template <typename BufferPage>
class PartitionBuffer {
    using ConsumerFn = std::function<void(BufferPage*, bool)>;

  private:
    std::vector<BufferPage*> partitions;
    mem::BlockAllocator<BufferPage> block_alloc;

  public:
    PartitionBuffer(u16 npartitions, u32 block_sz, u64 max_allocations)
        : partitions(npartitions), block_alloc(block_sz, max_allocations)
    {
        for (auto& part : partitions) {
            part = block_alloc.get_page();
        }
    }
};
