#pragma once

#include <vector>

#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"

namespace buf {

template <typename page_t, concepts::is_block_allocator<page_t> BlockAlloc, typename Fn = std::function<void(page_t*, bool /* final eviction? */)>>
class EvictionBuffer {

  private:
    std::vector<page_t*> partitions;
    std::vector<Fn> eviction_fns;
    BlockAlloc& block_alloc;

  public:
    using EvictionFn = Fn;

    EvictionBuffer(u32 npartitions, BlockAlloc& block_alloc, const std::vector<Fn>& eviction_fns)
        : partitions(npartitions), block_alloc(block_alloc), eviction_fns(std::move(eviction_fns))
    {
        // alloc partitions
        for (auto& part : partitions) {
            part = block_alloc.get_page();
            part->clear_tuples();
        }
    }

    ALWAYS_INLINE page_t* get_partition_page(u32 part) const
    {
        return partitions[part];
    }

    [[maybe_unused]]
    page_t* evict(u64 part_no, bool final_eviction = false)
    {
        return evict(part_no, partitions[part_no], final_eviction);
    }

    [[maybe_unused]]
    page_t* evict(u64 part_no, page_t* part_page, bool final_eviction = false)
    {
        eviction_fns[part_no](part_page, final_eviction);
        if (final_eviction) {
            return nullptr;
        }
        part_page = block_alloc.get_page();
        partitions[part_no] = part_page;
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
