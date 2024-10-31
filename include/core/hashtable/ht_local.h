// Abdelrahman Adel (2024)

#pragma once
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <optional>
#include <tbb/concurrent_queue.h>

#include "bench/bench.h"
#include "core/buffer/partition_buffer.h"
#include "core/memory/alloc.h"
#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "ht_base.h"
#include "ht_page.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "misc/exceptions/exceptions_misc.h"

namespace ht {

// TODO is_chained can be inferred from mode
template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, concepts::is_mem_allocator Alloc, bool is_chained, bool use_ptr,
          bool is_heterogeneous>
struct PartitionedAggregationHashtable : protected BaseAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, is_chained, use_ptr> {

    using base_t = BaseAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, is_chained, use_ptr>;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::page_t;
    using typename base_t::slot_idx_t;
    using block_alloc_t = mem::BlockAllocator<page_t, Alloc, is_heterogeneous>;
    using part_buf_t = PartitionBuffer<page_t, block_alloc_t>;

  protected:
    // global hashtables are non-inserters
    part_buf_t& part_buffer;
    u32 partition_shift{0};

    PartitionedAggregationHashtable() : base_t() {};

    PartitionedAggregationHashtable(u32 _npartitions, u32 _nslots, part_buf_t& _part_buffer)
        : base_t(_npartitions * _nslots), part_buffer(_part_buffer), partition_shift(__builtin_ctz(_nslots))
    {
        ASSERT(_npartitions == next_power_2(_npartitions));
        ASSERT(_nslots == next_power_2(_nslots));
    }

    template <bool fill = true>
    page_t* evict(u64 part_no, bool final_eviction = false)
    {
        if constexpr (fill) {
            // clear partition
            auto* part_begin = slots + (part_no << partition_shift);
            if constexpr (slots_mode == DIRECT) {
                std::fill(part_begin, part_begin + (1 << partition_shift), reinterpret_cast<slot_idx_t>(page_t::EMPTY_SLOT));
            }
            else {
                std::fill(part_begin, part_begin + (1 << partition_shift), reinterpret_cast<u64>(page_t::EMPTY_SLOT));
            }
        }
        return part_buffer.evict(part_no);
    }
};

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, void fn_agg(value_t&, const value_t&),
          concepts::is_mem_allocator Alloc, bool is_heterogeneous = false>
struct PartitionedChainedAggregationHashtable
    : public PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, true, entry_mode == DIRECT, is_heterogeneous> {
    using base_t = PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, true, entry_mode == DIRECT, is_heterogeneous>;
    using base_t::evict;
    using base_t::ht_mask;
    using base_t::part_buffer;
    using base_t::partition_shift;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::page_t;
    using typename base_t::part_buf_t;
    using typename base_t::slot_idx_t;

  public:
    PartitionedChainedAggregationHashtable(u32 _npartitions, u32 _nslots, part_buf_t& _part_buffer) : base_t(_npartitions, _nslots, _part_buffer)
    {
    }

    void aggregate(key_t& key, value_t& value, u64 key_hash)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        idx_t& slot = slots[mod];
        idx_t next_offset = slot, offset = slot;
        while (next_offset != page_t::EMPTY_SLOT) {
            // walk chain of slots
            if (part_page->get_group(next_offset) == key) {
                fn_agg(part_page->get_aggregates(next_offset), value);
                return;
            }
            next_offset = part_page->get_next(next_offset);
        }
        if (part_page->full()) {
            // evict if full
            part_page = evict(part_no);
            part_page->clear_tuples();
            offset = page_t::EMPTY_SLOT;
        }
        slot = part_page->emplace_back_grp(key, value, offset);
    }

    void aggregate(key_t& key, value_t& value)
    {
        aggregate(key, value, hash_tuple(key));
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "chained-"s + (entry_mode == DIRECT ? "direct" : "indirect"); // TODO include mode and slot_mode
    }
};

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, void fn_agg(value_t&, const value_t&),
          concepts::is_mem_allocator Alloc, bool is_salted = true, bool is_chained = false, bool is_heterogeneous = false>
requires(not is_salted or entry_mode != INDIRECT_16) // need at least 32 bits for 16-bit salt
struct PartitionedOpenAggregationHashtable
    : public PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, is_chained, slots_mode == DIRECT, is_heterogeneous> {
    using base_t = PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, is_chained, slots_mode == DIRECT, is_heterogeneous>;
    using base_t::evict;
    using base_t::ht_mask;
    using base_t::part_buffer;
    using base_t::partition_shift;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::page_t;
    using typename base_t::part_buf_t;
    using typename base_t::slot_idx_t;

    static constexpr u16 BITS_SALT = 16;
    static constexpr u16 BITS_SLOT = (sizeof(idx_t) * 8) - BITS_SALT;

  private:
    u64 slots_mask;

  public:
    PartitionedOpenAggregationHashtable(u32 _npartitions, u32 _nslots, part_buf_t& _part_buffer)
        : base_t(_npartitions, _nslots, _part_buffer), slots_mask(_nslots - 1)
    {
        if (_nslots <= page_t::max_tuples_per_page) {
            throw InvalidOptionError{"Open-addressing hashtable needs more slots"};
        }
    }

    void aggregate(const key_t& key, const value_t& value, u64 key_hash)
    requires(slots_mode == DIRECT)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        slot_idx_t slot = slots[mod];

        // use top bits for salt
        u16 hash_prefix = key_hash >> BITS_SLOT;

        // walk sequence of slots
        while (slot != reinterpret_cast<slot_idx_t>(page_t::EMPTY_SLOT)) {
            bool condition{true};
            if constexpr (is_salted) {
                // check salt
                condition = hash_prefix == static_cast<u16>(reinterpret_cast<uintptr_t>(slot));
            }
            if (condition) {
                slot = reinterpret_cast<slot_idx_t>(reinterpret_cast<uintptr_t>(slot) >> (is_salted * BITS_SALT));
                if (part_page->get_group(slot) == key) {
                    fn_agg(part_page->get_aggregates(slot), value);
                    return;
                }
            }

            mod = (mod + 1) & slots_mask;
            slot = slots[mod | partition_mask];
        }
        if (part_page->full()) {
            // evict if full
            part_page = evict(part_no);
            part_page->clear_tuples();
            mod = key_hash & ht_mask;
            ASSERT(slots[mod] == reinterpret_cast<slot_idx_t>(page_t::EMPTY_SLOT));
        }
        auto ht_entry = reinterpret_cast<uintptr_t>(part_page->emplace_back_grp(key, value));
        if constexpr (is_salted) {
            slots[mod | partition_mask] = reinterpret_cast<slot_idx_t>((ht_entry << BITS_SALT) | hash_prefix);
        }
        else {
            slots[mod | partition_mask] = reinterpret_cast<slot_idx_t>(ht_entry);
        }
    }

    // TODO  and ((1 << ((sizeof(slot_idx_t) * 8) - (16 * is_salted))) > page_t::max_tuples_per_page))
    // require that indirect addressing can index all slots on a page (minus the bits used for salting)
    void aggregate(const key_t& key, const value_t& value, u64 key_hash)
    requires(slots_mode != DIRECT)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        slot_idx_t slot = slots[mod];

        // use top bits for salt
        u16 hash_prefix = key_hash >> BITS_SLOT;

        // walk sequence of slots
        while (slot != static_cast<slot_idx_t>(reinterpret_cast<u64>(page_t::EMPTY_SLOT))) {
            bool condition{true};
            if constexpr (is_salted) {
                // check salt
                condition = hash_prefix == static_cast<u16>(slot);
            }
            if (condition) {
                slot = slot >> (is_salted * BITS_SALT);
                if (part_page->get_group(slot) == key) {
                    fn_agg(part_page->get_aggregates(slot), value);
                    return;
                }
            }

            mod = (mod + 1) & slots_mask;
            slot = slots[mod | partition_mask];
        }
        if (part_page->full()) {
            // evict if full
            part_page = evict(part_no);
            part_page->clear_tuples();
            mod = key_hash & ht_mask;
        }
        slot_idx_t ht_entry = part_page->emplace_back_grp(key, value);
        if constexpr (is_salted) {
            slots[mod | partition_mask] = (ht_entry << BITS_SALT) | hash_prefix;
        }
        else {
            slots[mod | partition_mask] = ht_entry;
        }
    }

    void aggregate(key_t& key, value_t& value)
    {
        aggregate(key, value, hash_tuple(key));
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "open-"s + ht::get_idx_mode_str(entry_mode) + "_entry-" + ht::get_idx_mode_str(slots_mode) + "_slots-" + (is_salted ? "salted-" : "") +
               (is_chained ? "is_chained" : "");
    }
};

} // namespace ht
