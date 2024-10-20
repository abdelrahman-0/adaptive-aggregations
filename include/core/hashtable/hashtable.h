// Abdelrahman Adel (2024)

// inspired by Maximilian Kuschewski (2023)

#pragma once
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <tbb/concurrent_queue.h>

#include "core/memory/alloc.h"
#include "core/memory/block_allocator.h"
#include "defaults.h"
#include "hashtable_page.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/concepts_traits/concepts_hashtable.h"
#include "misc/exceptions/exceptions_misc.h"
#include "ubench/debug.h"

DECLARE_uint32(maxalloc);

namespace hashtable {

enum TagType : u8 { NONE, BLOOM, SALT };

template <typename Key, typename Value, void fn_agg(Value&, const Value&), bool is_entry_chained,
          concepts::is_slot Slot, concepts::is_mem_allocator Alloc, bool is_heterogeneous, bool concurrent>
struct BasePartitionedHashtable {
    using PageAgg = PageAggregation<Slot, Key, Value, is_entry_chained, std::is_pointer_v<Slot>>;
    using ConsumerFn = std::function<void(PageAgg*, bool)>;
    using BlockAlloc = mem::BlockAllocator<PageAgg, Alloc, is_heterogeneous>;

  protected:
    static constexpr Slot EMPTY_SLOT = 0;
    std::vector<std::conditional_t<concurrent, std::atomic<PageAgg*>, PageAgg*>> partitions;
    std::vector<ConsumerFn> consumer_fns;
    BlockAlloc& block_alloc;
    Slot* ht;
    u64 ht_mask;
    u32 partition_shift;
    u32 npartitions;

    BasePartitionedHashtable(u32 _npartitions, u32 _nslots, std::vector<ConsumerFn>& _consumer_fns,
                             BlockAlloc& block_alloc)
        : consumer_fns(std::move(_consumer_fns)), block_alloc(block_alloc),
          ht_mask((_npartitions << __builtin_ctz(_nslots)) - 1), partition_shift(__builtin_ctz(_nslots)),
          npartitions(_npartitions)
    {
        ASSERT(_npartitions == next_power_2(_npartitions));
        ASSERT(_nslots == next_power_2(_nslots));

        // alloc ht
        ht = Alloc::template alloc<Slot>(sizeof(Slot) * (ht_mask + 1));

        // alloc partitions
        partitions.reserve(npartitions);
        for (u32 part{0}; part < npartitions; ++part) {
            partitions.push_back(block_alloc.get_page());
            partitions.back()->clear_tuples();
        }
    }

    // TODO fill = !final_eviction
    template <bool fill = true>
    void evict(u64 part_no, PageAgg* page_to_evict, bool final_eviction = false)
    {
        consumer_fns[part_no](page_to_evict, final_eviction);
        if constexpr (fill) {
            // clear partition
            auto* part_begin = ht + (part_no << partition_shift);
            std::fill(part_begin, part_begin + (1 << partition_shift), EMPTY_SLOT);
        }
    }

  public:
    void finalize(bool last_thread = true)
    {
        for (u32 part_no{0}; part_no < npartitions; ++part_no) {
            evict<false>(part_no, partitions[part_no], last_thread);
        }
    }
};

template <typename Key, typename Value, void fn_agg(Value&, const Value&), concepts::is_slot Slot = void*,
          concepts::is_mem_allocator Alloc = mem::MMapMemoryAllocator<true>, bool is_heterogeneous = false,
          bool concurrent = false>
struct PartitionedChainedHashtable
    : public BasePartitionedHashtable<Key, Value, fn_agg, true, Slot, Alloc, is_heterogeneous, concurrent> {
    using BaseHashTable = BasePartitionedHashtable<Key, Value, fn_agg, true, Slot, Alloc, is_heterogeneous, concurrent>;
    using BaseHashTable::block_alloc;
    using BaseHashTable::EMPTY_SLOT;
    using BaseHashTable::evict;
    using BaseHashTable::ht;
    using BaseHashTable::ht_mask;
    using BaseHashTable::partition_shift;
    using BaseHashTable::partitions;
    using typename BaseHashTable ::BlockAlloc;
    using typename BaseHashTable::ConsumerFn;
    using typename BaseHashTable::PageAgg;

  public:
    PartitionedChainedHashtable(u32 _npartitions, u32 _nslots, std::vector<ConsumerFn>& _consumer_fns,
                                BlockAlloc& block_alloc)
        : BaseHashTable(_npartitions, _nslots, _consumer_fns, block_alloc)
    {
    }

    void aggregate(Key key, Value value, u64 key_hash)
    requires(not concurrent)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto*& part_page = partitions[part_no];
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
            evict(part_no, part_page);
            part_page = block_alloc.get_page();
            part_page->clear_tuples();
            offset = EMPTY_SLOT;
        }
        slot = part_page->emplace_back_grp(offset, key, value);
    }

    void aggregate(Key key, Value value) { aggregate(key, value, hash_tuple(key)); }
};

template <typename Key, typename Value, void fn_agg(Value&, const Value&), concepts::is_slot Slot = void*,
          bool salted = true, concepts::is_mem_allocator Alloc = mem::MMapMemoryAllocator<true>,
          bool is_heterogeneous = false, bool concurrent = false>
requires(sizeof(Slot) == 8)
struct PartitionedOpenHashtable
    : public BasePartitionedHashtable<Key, Value, fn_agg, false, Slot, Alloc, is_heterogeneous, concurrent> {
    using BaseHashTable =
        BasePartitionedHashtable<Key, Value, fn_agg, false, Slot, Alloc, is_heterogeneous, concurrent>;
    using BaseHashTable::block_alloc;
    using BaseHashTable::EMPTY_SLOT;
    using BaseHashTable::evict;
    using BaseHashTable::ht;
    using BaseHashTable::ht_mask;
    using BaseHashTable::partition_shift;
    using BaseHashTable::partitions;
    using typename BaseHashTable::BlockAlloc;
    using typename BaseHashTable::ConsumerFn;
    using typename BaseHashTable::PageAgg;

  private:
    u64 slots_mask;

  public:
    PartitionedOpenHashtable(u32 _npartitions, u32 _nslots, std::vector<ConsumerFn>& _consumer_fns,
                             BlockAlloc& block_alloc)
        : BaseHashTable(_npartitions, _nslots, _consumer_fns, block_alloc), slots_mask(_nslots - 1)
    {
        if (_nslots <= PageAgg::max_tuples_per_page) {
            throw InvalidOptionError{"Salted hashtable needs more salt (slots)"};
        }
    }

    void aggregate(Key key, Value value, u64 key_hash)
    requires(not concurrent and salted)
    {
        // extract lower bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        u64 partition_mask = part_no << partition_shift;
        auto*& part_page = partitions[part_no];
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
            evict(part_no, part_page);
            part_page = block_alloc.get_page();
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
        auto*& part_page = partitions[part_no];
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
            evict(part_no, part_page);
            part_page = block_alloc.get_page();
            part_page->clear_tuples();
            mod = key_hash & ht_mask;
            ASSERT(ht[mod] == EMPTY_SLOT);
        }
        ht[mod | partition_mask] =
            reinterpret_cast<Slot>(reinterpret_cast<uintptr_t>(part_page->emplace_back_grp(key, value)));
    }

    void aggregate(Key key, Value value) { aggregate(key, value, hash_tuple(key)); }
};

} // namespace hashtable
