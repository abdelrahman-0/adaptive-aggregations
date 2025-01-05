#pragma once

#include <vector>

#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"

namespace buf {

template <typename page_t, typename BlockAlloc>
class EvictionBuffer {
  public:
    using EvictionFn = std::function<void(page_t*, bool /* final eviction? */)>;

  // private:
    std::vector<page_t*> partitions;
    std::vector<EvictionFn> eviction_fns;
    BlockAlloc& block_alloc;

  public:
    EvictionBuffer(u32 npartitions, BlockAlloc& block_alloc, const std::vector<EvictionFn>& _eviction_fns) : partitions(npartitions), eviction_fns(_eviction_fns), block_alloc(block_alloc)
    {
        // alloc partitions
        for (u32 part_no{0}; part_no < npartitions; ++part_no) {
            auto* part = block_alloc.get_object(part_no);
            part->clear_tuples();
            partitions[part_no] = part;
        }
    }

    ALWAYS_INLINE page_t* get_partition_page(u32 part_no) const
    {
        return partitions[part_no];
    }

    [[maybe_unused]]
    page_t* evict(u32 part_no, bool final_eviction = false)
    {
        return evict(part_no, partitions[part_no], final_eviction);
    }

    [[maybe_unused]]
    page_t* evict(u32 part_no, page_t* part_page, bool final_eviction = false)
    {
        eviction_fns[part_no](part_page, final_eviction);
        if (final_eviction) {
            return nullptr;
        }
        part_page           = block_alloc.get_object(part_no);
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
