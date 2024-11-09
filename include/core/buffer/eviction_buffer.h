#pragma once

#include <vector>

#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"

namespace buf {

template <typename BufferPage, concepts::is_block_allocator<BufferPage> BlockAlloc, typename Fn = std::function<void(BufferPage*, bool /* final eviction? */)>>
class EvictionBuffer {

  private:
    std::vector<BufferPage*> partitions;
    std::vector<Fn> consumer_fns;
    BlockAlloc& block_alloc;

  public:
    using ConsumerFn = Fn;

    EvictionBuffer(u32 npartitions, BlockAlloc& block_alloc, const std::vector<Fn>& consumer_fns)
        : partitions(npartitions), block_alloc(block_alloc), consumer_fns(std::move(consumer_fns))
    {
        // alloc partitions
        for (auto& part : partitions) {
            part = block_alloc.get_page();
            part->clear_tuples();
        }
    }

    ALWAYS_INLINE BufferPage* get_partition_page(u32 part) const
    {
        return partitions[part];
    }

    [[maybe_unused]]
    BufferPage* evict(u64 part_no, bool final_eviction = false)
    {
        auto*& part_page = partitions[part_no];
        consumer_fns[part_no](part_page, final_eviction);
        part_page = block_alloc.get_page();
        return part_page;
    }

    void finalize(bool final_eviction = true)
    {
        for (u32 part_no{0}; part_no < partitions.size(); ++part_no) {
            evict(part_no, final_eviction);
        }
    }
};

} // namespace buf
