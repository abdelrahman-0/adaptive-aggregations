// Abdelrahman Adel (2024)
#pragma once

#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <optional>
#include <tbb/concurrent_queue.h>
#include <unordered_set>

#include "bench/bench.h"
#include "core/buffer/eviction_buffer.h"
#include "core/buffer/partition_inserter.h"
#include "core/memory/alloc.h"
#include "core/memory/block_allocator.h"
#include "core/sketch/hll_custom.h"
#include "defaults.h"
#include "ht_base.h"
#include "ht_page.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "misc/exceptions/exceptions_misc.h"

namespace ht {

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, concepts::is_mem_allocator Alloc, concepts::is_sketch sketch_t, bool is_grouped, bool is_heterogeneous>
struct PartitionedAggregationHashtable : protected BaseAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, false, true> {
    using base_t = BaseAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, false, true>;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::page_t;
    using typename base_t::slot_idx_t;
    // TODO partition allocator
    using block_alloc_t = std::conditional_t<is_heterogeneous, mem::BlockAllocatorConcurrent<page_t, Alloc>, mem::BlockAllocatorNonConcurrent<page_t, Alloc>>;
    using part_buf_t    = buf::EvictionBuffer<page_t, block_alloc_t>;
    using inserter_t    = buf::PartitionedAggregationInserter<page_t, entry_mode, part_buf_t, sketch_t, is_grouped>;

  protected:
    // global hashtables are non-inserters
    part_buf_t& part_buffer;
    inserter_t& inserter;
    double threshold_preagg{};
    u64 group_not_found{0};
    u64 group_found{0};
    u32 npartitions{};
    u8 partition_shift{0};

    static constexpr slot_idx_t EMPTY_DIRECT_SLOT   = 0;
    static constexpr slot_idx_t EMPTY_INDIRECT_SLOT = -1;

    PartitionedAggregationHashtable(u32 _npartitions, u32 _nslots, double _threshold_preagg, part_buf_t& _part_buffer, inserter_t& _inserter)
        : base_t(_npartitions * _nslots), threshold_preagg(_threshold_preagg), part_buffer(_part_buffer), inserter(_inserter), npartitions(_npartitions),
          partition_shift(__builtin_ctz(_nslots))
    {
        ENSURE(_npartitions == next_power_2(_npartitions));
        ENSURE(_nslots == next_power_2(_nslots));
        for (u32 part_no : range(npartitions)) {
            clear_slots(part_no);
        }
    }

    [[nodiscard]]
    auto* get_part_begin(u32 part_no) const
    {
        return slots + (part_no << partition_shift);
    }

    void clear_slots(u32 part_no)
    {
        // clear slots
        if constexpr (slots_mode == DIRECT) {
            std::fill(get_part_begin(part_no), get_part_begin(part_no + 1), EMPTY_DIRECT_SLOT);
        }
        else {
            std::fill(get_part_begin(part_no), get_part_begin(part_no + 1), EMPTY_INDIRECT_SLOT);
        }
    }

  public:
    [[nodiscard]]
    bool is_useless() const
    {
        return ((group_not_found + group_found) > 100'000) and (((1.0 * group_not_found) / (group_not_found + group_found)) > threshold_preagg);
    }
};

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc, concepts::is_sketch sketch_t,
          bool is_grouped, bool is_heterogeneous>
requires(entry_mode != NO_IDX and slots_mode != NO_IDX)
struct PartitionedChainedAggregationHashtable : PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, is_grouped, is_heterogeneous> {
    using base_t = PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, is_grouped, is_heterogeneous>;
    using base_t::clear_slots;
    using base_t::EMPTY_DIRECT_SLOT;
    using base_t::EMPTY_INDIRECT_SLOT;
    using base_t::group_found;
    using base_t::group_not_found;
    using base_t::inserter;
    using base_t::mod_shift;
    using base_t::part_buffer;
    using base_t::partition_shift;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::inserter_t;
    using typename base_t::page_t;
    using typename base_t::part_buf_t;
    using typename base_t::slot_idx_t;

    PartitionedChainedAggregationHashtable(u32 _npartitions, u32 _nslots, double _threshold_preagg, part_buf_t& _part_buffer, inserter_t& _inserter)
        : base_t(_npartitions, _nslots, _threshold_preagg, _part_buffer, _inserter)
    {
    }

    // TODO update last not head
    void aggregate(key_t& key, value_t& value, u64 key_hash)
    requires(slots_mode == DIRECT)
    {
        // extract lower bits from hash
        u64 mod          = key_hash >> mod_shift;
        u64 part_no      = mod >> partition_shift;
        auto* part_page  = part_buffer.get_partition_page(part_no);
        slot_idx_t& slot = slots[mod];
        auto next_offset = slot;
        auto offset      = next_offset;
        while (next_offset != EMPTY_DIRECT_SLOT) {
            // walk chain of slots
            if (next_offset->get_group() == key) {
                fn_agg(next_offset->get_aggregates(), value);
                ++group_found;
                return;
            }
            next_offset = reinterpret_cast<slot_idx_t>(part_page->get_next(next_offset));
        }
        auto [new_slot, evicted] = inserter.template insert<EMPTY_DIRECT_SLOT>(key, value, reinterpret_cast<idx_t>(offset), key_hash, part_no, part_page);
        slot                     = new_slot;
        if (evicted) {
            clear_slots(part_no);
        }
        ++group_not_found;
    }

    void aggregate(key_t& key, value_t& value, u64 key_hash)
    requires(slots_mode != DIRECT and slots_mode == entry_mode)
    {
        // extract lower bits from hash
        u64 mod          = key_hash >> mod_shift;
        u64 part_no      = mod >> partition_shift;
        auto* part_page  = part_buffer.get_partition_page(part_no);
        slot_idx_t& slot = slots[mod];
        auto next_offset = slot;
        auto offset      = next_offset;
        while (next_offset != EMPTY_INDIRECT_SLOT) {
            // walk chain of slots
            if (part_page->get_group(next_offset) == key) {
                fn_agg(part_page->get_aggregates(next_offset), value);
                ++group_found;
                return;
            }
            next_offset = part_page->get_next(next_offset);
        }
        auto [new_slot, evicted] = inserter.template insert<EMPTY_INDIRECT_SLOT>(key, value, offset, key_hash, part_no, part_page);
        slot                     = new_slot;
        if (evicted) {
            clear_slots(part_no);
        }
        ++group_not_found;
    }

    void aggregate(key_t& key, value_t& value, u64 key_hash)
    requires(slots_mode != DIRECT and entry_mode == DIRECT)
    {
        // extract lower bits from hash
        u64 mod          = key_hash >> mod_shift;
        u64 part_no      = mod >> partition_shift;
        auto* part_page  = part_buffer.get_partition_page(part_no);
        slot_idx_t& slot = slots[mod];
        auto next_offset = slot;
        auto offset      = next_offset;
        while (next_offset != EMPTY_INDIRECT_SLOT) {
            // walk chain of slots
            if (part_page->get_group(next_offset) == key) {
                fn_agg(part_page->get_aggregates(next_offset), value);
                ++group_found;
                return;
            }
            next_offset = reinterpret_cast<u64>(part_page->get_next(next_offset));
        }
        auto [new_slot, evicted] = inserter.template insert<EMPTY_INDIRECT_SLOT>(key, value, reinterpret_cast<idx_t>(static_cast<u64>(offset)), key_hash, part_no, part_page);
        slot                     = new_slot;
        if (evicted) {
            clear_slots(part_no);
        }
        ++group_not_found;
    }

    void insert(key_t& key, value_t& value)
    {
        aggregate(key, value, hash_tuple(key));
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "chaining-"s + ht::get_idx_mode_str(slots_mode);
    }
};

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc, concepts::is_sketch sketch_t,
          bool is_salted, bool is_grouped, bool is_heterogeneous>
requires(not is_salted or slots_mode != INDIRECT_16) // need at least 32 bits for 16-bit salt
struct PartitionedOpenAggregationHashtable : PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, is_grouped, is_heterogeneous> {
    using base_t = PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, is_grouped, is_heterogeneous>;
    using base_t::clear_slots;
    using base_t::EMPTY_DIRECT_SLOT;
    using base_t::EMPTY_INDIRECT_SLOT;
    using base_t::group_found;
    using base_t::group_not_found;
    using base_t::inserter;
    using base_t::mod_shift;
    using base_t::part_buffer;
    using base_t::partition_shift;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::inserter_t;
    using typename base_t::page_t;
    using typename base_t::part_buf_t;
    using typename base_t::slot_idx_t;

    static constexpr u16 BITS_SALT = [] {
        // evaluated at compile-time
        if (is_salted) {
            switch (slots_mode) {
            case INDIRECT_32:
                return 16;
            case INDIRECT_64:
                return 32;
            default:;
            }
        }
        return 0;
    }();

  private:
    u64 slots_mask;

  public:
    PartitionedOpenAggregationHashtable(u32 _npartitions, u32 _nslots, double _threshold_preagg, part_buf_t& _part_buffer, inserter_t& _inserter)
        : base_t(_npartitions, _nslots, _threshold_preagg, _part_buffer, _inserter), slots_mask(_nslots - 1)
    {
        ENSURE(_nslots >= page_t::max_tuples_per_page);
    }

    void aggregate(const key_t& key, const value_t& value, u64 key_hash)
    requires(slots_mode == DIRECT)
    {
        // extract top bits from hash
        u64 mod            = key_hash >> mod_shift;
        u64 part_no        = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page    = part_buffer.get_partition_page(part_no);
        slot_idx_t slot    = slots[mod];

        // use bottom bits for salt
        auto hash_prefix   = static_cast<u16>(key_hash);

        // walk sequence of slots
        while (slot != EMPTY_DIRECT_SLOT) {
            bool condition{true};
            if constexpr (is_salted) {
                // check salt
                condition = hash_prefix == static_cast<u16>(reinterpret_cast<uintptr_t>(slot));
            }
            if (condition) {
                slot = reinterpret_cast<slot_idx_t>(reinterpret_cast<uintptr_t>(slot) >> BITS_SALT);
                if (slot->get_group() == key) {
                    fn_agg(slot->get_aggregates(), value);
                    ++group_found;
                    return;
                }
            }

            mod  = (mod + 1) & slots_mask;
            slot = slots[partition_mask | mod];
        }
        auto [ht_entry_raw, evicted] = inserter.insert(key, value, key_hash, part_no, part_page);
        if (evicted) {
            // reset mod
            mod = key_hash >> mod_shift;
            clear_slots(part_no);
        }
        auto ht_entry = reinterpret_cast<uintptr_t>(ht_entry_raw);
        if constexpr (is_salted) {
            // make room for salt
            slots[partition_mask | mod] = reinterpret_cast<slot_idx_t>((ht_entry << BITS_SALT) | hash_prefix);
        }
        else {
            slots[partition_mask | mod] = reinterpret_cast<slot_idx_t>(ht_entry);
        }
        ++group_not_found;
    }

    // require that indirect addressing can index all slots on a page (minus the bits used for salting)
    void aggregate(const key_t& key, const value_t& value, u64 key_hash)
    requires(slots_mode != DIRECT and ((1ul << ((sizeof(slot_idx_t) * 8) - BITS_SALT)) > page_t::max_tuples_per_page))
    {
        // u32 and direct addressing use 16-bit salt, u64 uses 32-bit salt,  also uses 16-bit salt
        using salt_t       = std::conditional_t<slots_mode == INDIRECT_64, u32, u16>;
        // static constexpr slot_idx_t slot_idx_mask = static_cast<slot_idx_t>(~0) >> 1;
        // extract top bits from hash
        u64 mod            = key_hash >> mod_shift;
        u64 part_no        = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page    = part_buffer.get_partition_page(part_no);
        slot_idx_t slot    = slots[mod];

        // use bottom bits for salt
        auto hash_prefix   = static_cast<salt_t>(key_hash);

        // walk sequence of slots
        while (slot != EMPTY_INDIRECT_SLOT) {
            bool condition{true};
            if constexpr (is_salted) {
                // check salt
                condition = hash_prefix == static_cast<salt_t>(slot);
            }
            if (condition) {
                slot = slot >> BITS_SALT;
                if (part_page->get_group(slot) == key) {
                    fn_agg(part_page->get_aggregates(slot), value);
                    ++group_found;
                    return;
                }
            }

            mod  = (mod + 1) & slots_mask;
            slot = slots[partition_mask | mod];
        }
        auto [ht_entry, evicted] = inserter.insert(key, value, key_hash, part_no, part_page);
        if (evicted) {
            // eviction took place

            // reset mod
            mod = key_hash >> mod_shift;
            clear_slots(part_no);
        }
        if constexpr (is_salted) {
            slots[partition_mask | mod] = (ht_entry << BITS_SALT) | hash_prefix;
        }
        else {
            slots[partition_mask | mod] = ht_entry;
        }
        ++group_not_found;
    }

    void insert(const key_t& key, const value_t& value)
    {
        aggregate(key, value, hash_tuple(key));
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "unchained-"s + ht::get_idx_mode_str(slots_mode) + (is_salted ? "-salted" : "");
    }
};

} // namespace ht
