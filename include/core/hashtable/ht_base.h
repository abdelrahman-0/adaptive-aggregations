#pragma once

#include "ht_page.h"
#include "misc/concepts_traits/concepts_hashtable.h"

namespace ht {

template <typename Key, typename Value, IDX_MODE entry_mode, IDX_MODE slots_mode, concepts::is_mem_allocator Alloc, bool is_chained, bool use_ptr,
          bool is_concurrent = false, bool next_first = true>
struct BaseAggregationHashtable {
    using page_t = PageAggregation<Key, Value, entry_mode, is_chained, use_ptr>;
    using entry_t = page_t::entry_t;
    using idx_t = page_t::idx_t;
    // need to distinguish index type of slots from index type of entries on page
    using slot_idx_raw_t = agg_entry_idx_t<Key, Value, slots_mode, is_chained, next_first>;
    using slot_idx_t = std::conditional_t<is_concurrent, std::atomic<slot_idx_raw_t>, slot_idx_raw_t>;

    slot_idx_t* slots{nullptr};
    u64 ht_mask{0};

  protected:
    BaseAggregationHashtable() = default;

    explicit BaseAggregationHashtable(u64 size)
    {
        initialize(size);
    }

    ~BaseAggregationHashtable()
    {
        Alloc::dealloc(slots);
    }

  public:
    void initialize(u64 size)
    {
        ASSERT(size == next_power_2(size));
        ht_mask = size - 1;

        // alloc ht
        slots = Alloc::template alloc<slot_idx_t>(sizeof(slot_idx_t) * size);
    }
};

} // namespace ht
