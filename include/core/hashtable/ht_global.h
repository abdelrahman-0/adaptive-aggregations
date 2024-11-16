#pragma once

#include "ht_base.h"

namespace ht {

template <typename key_t, typename value_t, IDX_MODE entry_mode, concepts::is_mem_allocator Alloc, bool is_multinode>
struct ConcurrentAggregationHashtable : public BaseAggregationHashtable<key_t, value_t, entry_mode, DIRECT, Alloc, true, true> {
    using base_t = BaseAggregationHashtable<key_t, value_t, entry_mode, DIRECT, Alloc, true, true>;
    using base_t::mod_shift;

    u64 size_mask{0};
    u8 group_shift;

    void initialize(u64 size, u8 npartgroups = 0)
    {
        ASSERT(npartgroups == next_power_2(npartgroups));
        group_shift = __builtin_ctz(npartgroups);
        base_t::initialize(size);
        size_mask = size - 1;
    }

    u64 get_pos(u64 key_hash)
    {
        // extract top bits from hash
        u64 mod = key_hash;
        if constexpr (is_multinode) {
            // remove fixed bits (to avoid clustering effect)
            mod <<= group_shift;
        }
        return mod >> mod_shift;
    }
};

template <typename key_t, typename value_t, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc, bool is_multinode>
struct ConcurrentChainedAggregationHashtable : public ConcurrentAggregationHashtable<key_t, value_t, DIRECT, Alloc, is_multinode> {
    using base_t = ConcurrentAggregationHashtable<key_t, value_t, DIRECT, Alloc, is_multinode>;
    using base_t::get_pos;
    using base_t::slots;
    using typename base_t::entry_t;
    using typename base_t::idx_t;
    using typename base_t::page_t;
    using typename base_t::slot_idx_raw_t;
    using typename base_t::slot_idx_t;

    ConcurrentChainedAggregationHashtable() = default;
    u64 size_mask{0};
    u8 group_shift;

    void aggregate(key_t& key, value_t& value, idx_t& next, u64 key_hash, entry_t* addr)
    {
        u64 mod = get_pos(key_hash);
        slot_idx_raw_t head = slots[mod].load();
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

    void insert(entry_t& entry)
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

template <typename key_t, typename value_t, IDX_MODE entry_mode, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc, bool is_multinode,
          bool is_salted = true>
struct ConcurrentOpenAggregationHashtable : public ConcurrentAggregationHashtable<key_t, value_t, entry_mode, Alloc, is_multinode> {
    using base_t = ConcurrentAggregationHashtable<key_t, value_t, entry_mode, Alloc, is_multinode>;
    using base_t::get_pos;
    using base_t::size_mask;
    using base_t::slots;
    using typename base_t::entry_t;
    using typename base_t::page_t;
    using typename base_t::slot_idx_raw_t;
    using typename base_t::slot_idx_t;

    static constexpr u16 BITS_SALT = 16;

    ConcurrentOpenAggregationHashtable() : base_t()
    {
    }

    void aggregate(key_t& key, value_t& value, u64 key_hash, entry_t* addr)
    {
        u64 mod = get_pos(key_hash);
        slot_idx_raw_t slot = slots[mod].load();

        // use bottom bits for salt
        u16 salt = static_cast<u16>(key_hash);

        while (true) {
            // walk slots
            while (slot) {
                bool condition{true};
                if constexpr (is_salted) {
                    // check salt
                    condition = salt == static_cast<u16>(reinterpret_cast<uintptr_t>(slot));
                }
                if (condition) {
                    slot = reinterpret_cast<slot_idx_raw_t>(reinterpret_cast<uintptr_t>(slot) >> (is_salted * BITS_SALT));
                    if (slot->get_group() == key) {
                        fn_agg(slot->get_aggregates(), value);
                        return;
                    }
                }
                mod = (mod + 1) & size_mask;
                slot = slots[mod].load();
            }
            if constexpr (is_salted) {
                if (slots[mod].compare_exchange_strong(slot, reinterpret_cast<slot_idx_raw_t>((reinterpret_cast<uintptr_t>(addr) << BITS_SALT) | salt))) {
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

    void insert(entry_t& entry)
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
