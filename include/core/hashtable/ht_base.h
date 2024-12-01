#pragma once

#include "ht_page.h"
#include "misc/concepts_traits/concepts_hashtable.h"

namespace ht {

using namespace std::string_literals;

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, concepts::is_mem_allocator Alloc, bool is_concurrent, bool next_first>
struct BaseAggregationHashtable {
    using page_t         = PageAggregation<key_t, value_t, entry_mode, slots_mode == DIRECT, true>;
    using entry_t        = typename page_t::entry_t;
    using idx_t          = typename page_t::idx_t;
    // need to distinguish index mode of slots from index mode of entries on page
    using slot_idx_raw_t = agg_slot_idx_t<key_t, value_t, entry_mode, slots_mode, next_first>;
    using slot_idx_t     = std::conditional_t<is_concurrent, std::atomic<slot_idx_raw_t>, slot_idx_raw_t>;

    slot_idx_t* slots{nullptr};
    u8 mod_shift{0};

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
        mod_shift = 64 - __builtin_ctz(size);
        // alloc ht
        slots     = Alloc::template alloc<slot_idx_t>(sizeof(slot_idx_t) * size);
    }
};

} // namespace ht
