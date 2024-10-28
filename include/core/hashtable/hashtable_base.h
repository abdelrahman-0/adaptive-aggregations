#pragma once

#include "hashtable_page.h"
#include "misc/concepts_traits/concepts_hashtable.h"

namespace ht {

template <IDX_MODE mode, typename Key, typename Value, concepts::is_mem_allocator Alloc, bool is_chained, bool use_ptr, bool is_concurrent = false>
struct BaseAggregationHashtable {
    using page_t = PageAggregation<mode, Key, Value, is_chained, use_ptr>;
    using idx_t = page_t::idx_t;
    using entry_t = page_t::entry_t;
    using ht_idx_t = std::conditional_t<is_concurrent, std::atomic<idx_t>, idx_t>;
    ht_idx_t* slots{nullptr};
    u64 ht_mask{0};

  protected:
    BaseAggregationHashtable() = default;

    explicit BaseAggregationHashtable(u64 size)
    {
        initialize(size);
    }

  public:
    void initialize(u64 size)
    {
        ASSERT(size == next_power_2(size));
        ht_mask = size - 1;

        // alloc ht
        slots = Alloc::template alloc<ht_idx_t>(sizeof(ht_idx_t) * size);
    }
};

} // namespace ht
