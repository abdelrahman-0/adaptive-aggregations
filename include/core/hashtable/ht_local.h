// Abdelrahman Adel (2024)

#pragma once
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <optional>
#include <tbb/concurrent_queue.h>

#include "bench/bench.h"
#include "core/buffer/eviction_buffer.h"
#include "core/memory/alloc.h"
#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "ht_base.h"
#include "ht_page.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "misc/exceptions/exceptions_misc.h"

namespace ht {

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, concepts::is_mem_allocator Alloc, concepts::is_sketch sketch_t,
          double threshold_preagg, bool use_ptr, bool is_heterogeneous>
struct PartitionedAggregationHashtable : protected BaseAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, use_ptr> {
    using base_t = BaseAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, use_ptr>;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::page_t;
    using typename base_t::slot_idx_t;
    using block_alloc_t = mem::BlockAllocator<page_t, Alloc, is_heterogeneous>;
    using part_buf_t = buf::EvictionBuffer<page_t, block_alloc_t>;
    using inserter_t = buf::PartitionedAggregationInserter<key_t, value_t, entry_mode, Alloc, sketch_t, use_ptr, is_heterogeneous>;
    static_assert(std::is_same_v<page_t, typename inserter_t::page_t>);

  protected:
    // global hashtables are non-inserters
    part_buf_t& part_buffer;
    inserter_t& inserter;
    u64 group_not_found{0};
    u64 group_found{0};
    u32 npartitions{};
    u8 partition_shift{0};

    PartitionedAggregationHashtable() : base_t() {};

    PartitionedAggregationHashtable(u32 _npartitions, u32 _nslots, part_buf_t& _part_buffer, inserter_t& _inserter)
        : base_t(_npartitions * _nslots), part_buffer(_part_buffer), inserter(_inserter), npartitions(_npartitions), partition_shift(__builtin_ctz(_nslots))
    {
        ASSERT(_npartitions == next_power_2(_npartitions));
        ASSERT(_nslots == next_power_2(_nslots));
    }

    void clear_slots(u64 part_no)
    {
        // clear slots
        auto* part_begin = slots + (part_no << partition_shift);
        ::memset(part_begin, 0, (1 << partition_shift) * sizeof(slot_idx_t));
    }

  public:
    [[nodiscard]]
    bool is_useless() const
    {
        return ((group_not_found + group_found) > 0) and (group_not_found >= (npartitions * page_t::max_tuples_per_page)) and
               (((1.0 * group_not_found) / (group_not_found + group_found)) > threshold_preagg);
    }
};

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc,
          concepts::is_sketch sketch_t, double threshold_preagg, bool is_heterogeneous = false>
requires(entry_mode != NO_IDX and slots_mode != NO_IDX)
struct PartitionedChainedAggregationHashtable
    : public PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, threshold_preagg, slots_mode == DIRECT, is_heterogeneous> {
    using base_t = PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, threshold_preagg, slots_mode == DIRECT, is_heterogeneous>;
    using base_t::clear_slots;
    using base_t::group_found;
    using base_t::group_not_found;
    using base_t::ht_mask;
    using base_t::inserter;
    using base_t::part_buffer;
    using base_t::partition_shift;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::inserter_t;
    using typename base_t::page_t;
    using typename base_t::part_buf_t;
    using typename base_t::slot_idx_t;

  public:
    PartitionedChainedAggregationHashtable(u32 _npartitions, u32 _nslots, part_buf_t& _part_buffer, inserter_t& _inserter)
        : base_t(_npartitions, _nslots, _part_buffer, _inserter)
    {
    }

    void aggregate(key_t& key, value_t& value, u64 key_hash)
    requires(slots_mode == DIRECT)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        slot_idx_t& slot = slots[mod];
        auto next_offset = slot;
        auto offset = next_offset;
        while (next_offset) {
            // walk chain of slots
            if (next_offset->get_group() == key) {
                fn_agg(next_offset->get_aggregates(), value);
                group_found++;
                return;
            }
            next_offset = reinterpret_cast<slot_idx_t>(part_page->get_next(next_offset));
        }
        bool evicted;
        std::tie(slot, evicted) = inserter.insert(key, value, reinterpret_cast<idx_t>(offset), key_hash, part_no, part_page);
        if (evicted) {
            clear_slots(part_no);
        }
        group_not_found++;
    }

    void aggregate(key_t& key, value_t& value, u64 key_hash)
    requires(slots_mode != DIRECT and slots_mode == entry_mode)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        slot_idx_t& slot = slots[mod];
        auto next_offset = slot;
        auto offset = next_offset;
        while (next_offset) {
            // walk chain of slots
            if (part_page->get_group(next_offset) == key) {
                fn_agg(part_page->get_aggregates(next_offset), value);
                group_found++;
                return;
            }
            next_offset = part_page->get_next(next_offset);
        }
        bool evicted;
        std::tie(slot, evicted) = inserter.insert(key, value, offset, key_hash, part_no, part_page);
        if (evicted) {
            clear_slots(part_no);
        }
        group_not_found++;
    }

    void aggregate(key_t& key, value_t& value, u64 key_hash)
    requires(slots_mode != DIRECT and entry_mode == DIRECT)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        slot_idx_t& slot = slots[mod];
        auto next_offset = slot;
        auto offset = next_offset;
        while (next_offset) {
            // walk chain of slots
            if (part_page->get_group(next_offset) == key) {
                fn_agg(part_page->get_aggregates(next_offset), value);
                group_found++;
                return;
            }
            next_offset = reinterpret_cast<u64>(part_page->get_next(next_offset));
        }
        bool evicted;
        std::tie(slot, evicted) = inserter.insert(key, value, reinterpret_cast<idx_t>(static_cast<u64>(offset)), key_hash, part_no, part_page);
        if (evicted) {
            clear_slots(part_no);
        }
        group_not_found++;
    }

    void insert(key_t& key, value_t& value)
    {
        aggregate(key, value, hash_tuple(key));
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "chained-"s + ht::get_idx_mode_str(entry_mode) + "_entry-" + ht::get_idx_mode_str(slots_mode) + "_slots";
    }
};

template <typename key_t, typename value_t, IDX_MODE entry_mode, IDX_MODE slots_mode, void fn_agg(value_t&, const value_t&), concepts::is_mem_allocator Alloc,
          concepts::is_sketch sketch_t, double threshold_preagg, bool is_salted = true, bool is_heterogeneous = false>
requires(not is_salted or entry_mode != INDIRECT_16) // need at least 32 bits for 16-bit salt
struct PartitionedOpenAggregationHashtable
    : public PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, threshold_preagg, slots_mode == DIRECT, is_heterogeneous> {
    using base_t = PartitionedAggregationHashtable<key_t, value_t, entry_mode, slots_mode, Alloc, sketch_t, threshold_preagg, slots_mode == DIRECT, is_heterogeneous>;
    using base_t::clear_slots;
    using base_t::group_found;
    using base_t::group_not_found;
    using base_t::inserter;
    using base_t::is_chained;
    using base_t::mod_shift;
    using base_t::part_buffer;
    using base_t::partition_shift;
    using base_t::slots;
    using typename base_t::idx_t;
    using typename base_t::inserter_t;
    using typename base_t::page_t;
    using typename base_t::part_buf_t;
    using typename base_t::slot_idx_t;

    static constexpr u16 BITS_SALT = 16;
    static constexpr u16 BITS_SLOT = (sizeof(slot_idx_t) * 8) - BITS_SALT;

  private:
    u64 slots_mask;

  public:
    PartitionedOpenAggregationHashtable(u32 _npartitions, u32 _nslots, part_buf_t& _part_buffer, inserter_t& _inserter)
        : base_t(_npartitions, _nslots, _part_buffer, _inserter), slots_mask(_nslots - 1)
    {
        if (_nslots <= page_t::max_tuples_per_page) {
            throw InvalidOptionError{"Open-addressing hashtable needs more slots"};
        }
    }

    void aggregate(const key_t& key, const value_t& value, u64 key_hash)
    requires(slots_mode == DIRECT)
    {
        // extract top bits from hash
        u64 mod = key_hash >> mod_shift;
        u64 part_no = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        slot_idx_t slot = slots[mod];

        // use bottom bits for salt
        u16 hash_prefix = static_cast<u16>(key_hash);

        // walk sequence of slots
        while (slot) {
            bool condition{true};
            if constexpr (is_salted) {
                // check salt
                condition = hash_prefix == static_cast<u16>(reinterpret_cast<uintptr_t>(slot));
            }
            if (condition) {
                slot = reinterpret_cast<slot_idx_t>(reinterpret_cast<uintptr_t>(slot) >> (is_salted * BITS_SALT));
                if (slot->get_group() == key) {
                    fn_agg(slot->get_aggregates(), value);
                    group_found++;
                    return;
                }
            }

            mod = (mod + 1) & slots_mask;
            slot = slots[mod | partition_mask];
        }
        auto [ht_entry_raw, evicted] = inserter.insert(key, value, key_hash, part_no, part_page);
        if (evicted) {
            // reset mod
            mod = key_hash >> mod_shift;
            clear_slots(part_no);
        }
        auto ht_entry = reinterpret_cast<uintptr_t>(ht_entry_raw);
        if constexpr (is_salted) {
            slots[mod | partition_mask] = reinterpret_cast<slot_idx_t>((ht_entry << BITS_SALT) | hash_prefix);
        }
        else {
            slots[mod | partition_mask] = reinterpret_cast<slot_idx_t>(ht_entry);
        }
        group_not_found++;
    }

    // TODO  and ((1 << ((sizeof(slot_idx_t) * 8) - (16 * is_salted))) > page_t::max_tuples_per_page))
    // require that indirect addressing can index all slots on a page (minus the bits used for salting)
    void aggregate(const key_t& key, const value_t& value, u64 key_hash)
    requires(slots_mode != DIRECT)
    {
        static constexpr auto slot_idx_mask = (~static_cast<slot_idx_t>(0)) >> 1;
        // extract top bits from hash
        u64 mod = key_hash >> mod_shift;
        u64 part_no = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        slot_idx_t slot = slots[mod];

        // use bottom bits for salt
        u16 hash_prefix = static_cast<u16>(key_hash);

        // walk sequence of slots
        while (slot) {
            bool condition{true};
            if constexpr (is_salted) {
                // check salt
                condition = hash_prefix == static_cast<u16>(slot);
            }
            if (condition) {
                slot = (slot & slot_idx_mask) >> (is_salted * BITS_SALT);
                if (part_page->get_group(slot) == key) {
                    fn_agg(part_page->get_aggregates(slot), value);
                    group_found++;
                    return;
                }
            }

            mod = (mod + 1) & slots_mask;
            slot = slots[mod | partition_mask];
        }
        auto [ht_entry, evicted] = inserter.insert(key, value, key_hash, part_no, part_page);
        if (evicted) {
            // reset mod
            mod = key_hash >> mod_shift;
            clear_slots(part_no);
        }
        if constexpr (is_salted) {
            slots[mod | partition_mask] = (ht_entry << BITS_SALT) | hash_prefix | (~slot_idx_mask);
        }
        else {
            slots[mod | partition_mask] = ht_entry | (~slot_idx_mask);
        }
        group_not_found++;
    }

    void insert(key_t& key, value_t& value)
    {
        aggregate(key, value, hash_tuple(key));
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "open-"s + ht::get_idx_mode_str(entry_mode) + "_entry-" + ht::get_idx_mode_str(slots_mode) + "_slots" + (is_salted ? "-salted" : "") +
               (is_chained ? "-is_chained" : "");
    }
};

} // namespace ht
