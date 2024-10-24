// Abdelrahman Adel (2024)

#pragma once
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <tbb/concurrent_queue.h>

#include "bench/bench.h"
#include "core/memory/alloc.h"
#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "hashtable_page.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "misc/exceptions/exceptions_misc.h"

namespace hashtable {

template <typename Key, typename Value, concepts::is_slot Slot, bool is_entry_chained, concepts::is_mem_allocator Alloc,
          bool is_heterogeneous, bool concurrent, typename PageAgg, concepts::is_block_allocator<PageAgg> BlockAlloc,
          concepts::is_partition_buffer<PageAgg> PartBuf>
struct BasePartitionedHashtable {

  protected:
    static constexpr Slot EMPTY_SLOT = 0;
    std::conditional_t<concurrent, std::atomic<Slot>*, Slot*> ht;
    PartBuf& part_buffer;
    u64 ht_mask;
    u32 partition_shift;
    u32 npartitions;

    BasePartitionedHashtable(u32 _npartitions, u32 _nslots, PartBuf& part_buffer)
        : ht_mask((_npartitions << __builtin_ctz(_nslots)) - 1), part_buffer(part_buffer),
          partition_shift(__builtin_ctz(_nslots)), npartitions(_npartitions)
    {
        ASSERT(_npartitions == next_power_2(_npartitions));
        ASSERT(_nslots == next_power_2(_nslots));

        // alloc ht
        ht = Alloc::template alloc<Slot>(sizeof(Slot) * (ht_mask + 1));
    }

    template <bool fill = true>
    void evict(u64 part_no, PageAgg* page_to_evict, bool final_eviction = false)
    {
        part_buffer.evict(part_no, page_to_evict);
        if constexpr (fill) {
            // clear partition
            auto* part_begin = ht + (part_no << partition_shift);
            // TODO remove for MMAP allocator?
            std::fill(part_begin, part_begin + (1 << partition_shift), EMPTY_SLOT);
        }
    }
};

template <typename Key, typename Value, concepts::is_slot Slot, void fn_agg(Value&, const Value&),
          concepts::is_mem_allocator Alloc, bool is_heterogeneous, bool concurrent = false,
          typename PageAgg = PageAggregation<Slot, Key, Value, false, std::is_pointer_v<Slot>>,
          concepts::is_block_allocator<PageAgg> BlockAlloc = mem::BlockAllocator<PageAgg, Alloc, false>,
          concepts::is_partition_buffer<PageAgg> PartBuf = PartitionBuffer<PageAgg, BlockAlloc>>
struct PartitionedChainedHashtable : public BasePartitionedHashtable<Key, Value, Slot, true, Alloc, is_heterogeneous,
                                                                     concurrent, PageAgg, BlockAlloc, PartBuf> {
    using BaseHashTable = BasePartitionedHashtable<Key, Value, Slot, true, Alloc, is_heterogeneous, concurrent, PageAgg,
                                                   BlockAlloc, PartBuf>;
    using BaseHashTable::EMPTY_SLOT;
    using BaseHashTable::evict;
    using BaseHashTable::ht;
    using BaseHashTable::ht_mask;
    using BaseHashTable::part_buffer;
    using BaseHashTable::partition_shift;

  public:
    PartitionedChainedHashtable(u32 _npartitions, u32 _nslots, PartBuf& part_buffer)
        : BaseHashTable(_npartitions, _nslots, part_buffer)
    {
    }

    void aggregate(Key key, Value value, u64 key_hash)
    requires(not concurrent)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        Slot& slot = ht[mod];
        Slot next_offset = slot, offset = slot;
        while (next_offset != EMPTY_SLOT) {
            // walk chain of slots
            if (part_page->get_group(next_offset) == key) {
                fn_agg(part_page->get_aggregates(next_offset), value);
                return;
            }
            next_offset = part_page->get_next(next_offset);
        }
        if (part_page->full()) {
            // evict if full
            part_page = part_buffer.evict(part_no, part_page);
            part_page->clear_tuples();
            offset = EMPTY_SLOT;
        }
        slot = part_page->emplace_back_grp(offset, key, value);
    }

    void aggregate(Key key, Value value) { aggregate(key, value, hash_tuple(key)); }

    [[nodiscard]]
    static std::string get_type()
    {
        return "chained-"s + (std::is_pointer_v<Slot> ? "direct" : "indirect");
    }
};

template <typename Key, typename Value, concepts::is_slot Slot, bool salted, void fn_agg(Value&, const Value&),
          concepts::is_mem_allocator Alloc, bool is_heterogeneous, bool concurrent = false,
          typename PageAgg = PageAggregation<Slot, Key, Value, false, std::is_pointer_v<Slot>>,
          concepts::is_block_allocator<PageAgg> BlockAlloc = mem::BlockAllocator<PageAgg, Alloc, false>,
          concepts::is_partition_buffer<PageAgg> PartBuf = PartitionBuffer<PageAgg, BlockAlloc>>
requires(sizeof(Slot) == 8)
struct PartitionedOpenHashtable : public BasePartitionedHashtable<Key, Value, Slot, false, Alloc, is_heterogeneous,
                                                                  concurrent, PageAgg, BlockAlloc, PartBuf> {
    using BaseHashTable = BasePartitionedHashtable<Key, Value, Slot, false, Alloc, is_heterogeneous, concurrent,
                                                   PageAgg, BlockAlloc, PartBuf>;
    using BaseHashTable::EMPTY_SLOT;
    using BaseHashTable::evict;
    using BaseHashTable::ht;
    using BaseHashTable::ht_mask;
    using BaseHashTable::part_buffer;
    using BaseHashTable::partition_shift;
    using HashtablePage = PageAgg;

  private:
    u64 slots_mask;

  public:
    PartitionedOpenHashtable(u32 _npartitions, u32 _nslots, PartBuf& part_buffer)
        : BaseHashTable(_npartitions, _nslots, part_buffer), slots_mask(_nslots - 1)
    {
        if (_nslots <= PageAgg::max_tuples_per_page) {
            throw InvalidOptionError{"Open-addressing hashtable needs more slots"};
        }
    }

    void aggregate(Key key, Value value, u64 key_hash)
    requires(not concurrent and salted)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        Slot slot = ht[mod];

        // use top bits for salt
        u16 hash_prefix = key_hash >> 48;

        // walk sequence of slots
        while (slot != EMPTY_SLOT) {
            // check salt
            if (hash_prefix == static_cast<u16>(reinterpret_cast<uintptr_t>(slot))) {
                slot = reinterpret_cast<Slot>(reinterpret_cast<uintptr_t>(slot) >> 16);
                if (part_page->get_group(slot) == key) {
                    fn_agg(part_page->get_aggregates(slot), value);
                    return;
                }
            }
            mod = (mod + 1) & slots_mask;
            slot = ht[mod | partition_mask];
        }
        if (part_page->full()) {
            // evict if full
            part_page = part_buffer.evict(part_no, part_page);
            part_page->clear_tuples();
            mod = key_hash & ht_mask;
            ASSERT(ht[mod] == EMPTY_SLOT);
        }
        ht[mod | partition_mask] = reinterpret_cast<Slot>(
            (reinterpret_cast<uintptr_t>(part_page->emplace_back_grp(key, value)) << 16) | hash_prefix);
    }

    void aggregate(Key key, Value value, u64 key_hash)
    requires(not concurrent and not salted)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto* part_page = part_buffer.get_partition_page(part_no);
        Slot slot = ht[mod];

        // walk sequence of slots
        while (slot != EMPTY_SLOT) {
            if (part_page->get_group(slot) == key) {
                fn_agg(part_page->get_aggregates(slot), value);
                return;
            }
            mod = (mod + 1) & slots_mask;
            slot = ht[mod | partition_mask];
        }
        if (part_page->full()) {
            // evict if full
            part_page = part_buffer.evict(part_no, part_page);
            part_page->clear_tuples();
            mod = key_hash & ht_mask;
            ASSERT(ht[mod] == EMPTY_SLOT);
        }
        ht[mod | partition_mask] =
            reinterpret_cast<Slot>(reinterpret_cast<uintptr_t>(part_page->emplace_back_grp(key, value)));
    }

    void aggregate(Key key, Value value) { aggregate(key, value, hash_tuple(key)); }

    [[nodiscard]]
    static std::string get_type()
    {
        return "open-"s + (std::is_pointer_v<Slot> ? "direct" : "indirect") + (salted ? "-salted" : "");
    }
};

} // namespace hashtable
