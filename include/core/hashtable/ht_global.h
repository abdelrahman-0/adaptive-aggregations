#pragma once

namespace ht {

template <typename key_t, typename value_t, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc>
struct ConcurrentChainedAggregationHashtable : public BaseAggregationHashtable<key_t, value_t, DIRECT, DIRECT, Alloc, true, true, true> {
    using base_t = BaseAggregationHashtable<key_t, value_t, DIRECT, DIRECT, Alloc, true, true, true>;
    using base_t::ht_mask;
    using base_t::slots;
    using typename base_t::entry_t;
    using typename base_t::idx_t;
    using typename base_t::page_t;
    using typename base_t::slot_idx_t;

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

    [[nodiscard]]
    static std::string get_type()
    {
        return "concurrent-chaining";
    }
};

template <typename key_t, typename value_t, IDX_MODE entry_mode, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc, bool is_salted = true>
struct ConcurrentOpenAggregationHashtable : public BaseAggregationHashtable<key_t, value_t, entry_mode, DIRECT, Alloc, true, true> {
    using base_t = BaseAggregationHashtable<key_t, value_t, entry_mode, DIRECT, Alloc, true, true>;
    using base_t::ht_mask;
    using base_t::slots;
    using typename base_t::entry_t;
    using typename base_t::page_t;
    using typename base_t::slot_idx_raw_t;
    using typename base_t::slot_idx_t;

    static constexpr u16 BITS_SALT = 16;
    static constexpr u16 BITS_SLOT = (sizeof(slot_idx_t) * 8) - BITS_SALT;

    ConcurrentOpenAggregationHashtable() = default;

    void aggregate(key_t& key, value_t& value, u64 key_hash, entry_t* addr)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        slot_idx_raw_t slot = slots[mod].load();

        // use top bits for salt
        u16 hash_prefix = key_hash >> BITS_SLOT;

        while (true) {
            // walk slots
            while (slot) {
                bool condition{true};
                if constexpr (is_salted) {
                    // check salt
                    condition = hash_prefix == static_cast<u16>(reinterpret_cast<uintptr_t>(slot));
                }
                if (condition) {
                    slot = reinterpret_cast<slot_idx_raw_t>(reinterpret_cast<uintptr_t>(slot) >> (is_salted * BITS_SALT));
                    if (slot->get_group() == key) {
                        fn_agg(slot->get_aggregates(), value);
                        return;
                    }
                }
                mod = (mod + 1) & ht_mask;
                slot = slots[mod].load();
            }
            if constexpr (is_salted) {
                if (slots[mod].compare_exchange_strong(slot, reinterpret_cast<slot_idx_raw_t>((reinterpret_cast<uintptr_t>(addr) << BITS_SALT) | hash_prefix))) {
                    return;
                }
            }
            else {
                if (slots[mod].compare_exchange_strong(slot, reinterpret_cast<slot_idx_raw_t>(addr))) {
                    return;
                }
            }
            // slot was updated, try again
        }
    }

    void aggregate(entry_t& entry)
    {
        auto& key = entry.get_group();
        auto& agg = entry.get_aggregates();
        aggregate(key, agg, hash_tuple(key), &entry);
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "concurrent-open"s + (is_salted ? "-salted" : "");
    }
};
} // namespace ht
