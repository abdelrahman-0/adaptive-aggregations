#pragma once

namespace ht {

template <typename key_t, typename value_t, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc>
struct ConcurrentChainedAggregationHashtable : public BaseAggregationHashtable<DIRECT, key_t, value_t, Alloc, true, true, true> {
    using base_t = BaseAggregationHashtable<DIRECT, key_t, value_t, Alloc, true, true, true>;
    using base_t::ht_mask;
    using base_t::slots;
    using typename base_t::entry_t;
    using typename base_t::idx_t;
    using typename base_t::page_t;

    ConcurrentChainedAggregationHashtable() = default;

    void aggregate(key_t& key, value_t& value, idx_t& next, u64 key_hash, entry_t* addr)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        auto head = slots[mod].load();
        while (true) {
            auto slot = head;
            // try update
            while (slot) {
                // walk chain of slots
                if (slot->get_group() == key) {
                    fn_agg(slot->get_aggregates(), value);
                    return;
                }
                slot = slot->get_next();
            }

            // try insert at head
            next = head;
            if (slots[mod].compare_exchange_strong(head, addr)) {
                return;
            }
            // chain was updated, try again
        }
    }

    void aggregate(entry_t& entry)
    {
        auto& key = entry.get_group();
        auto& agg = entry.get_aggregates();
        auto& next = entry.get_next();
        aggregate(key, agg, next, hash_tuple(key), &entry);
    }
};

} // namespace ht
