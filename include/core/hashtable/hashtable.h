// Abdelrahman Adel (2024)

// inspired by Max

#pragma once
#include <atomic>
#include <cmath>
#include <cstdint>
#include <tbb/concurrent_queue.h>

#include "concepts_traits/concepts_alloc.h"
#include "concepts_traits/concepts_hashtable.h"
#include "core/memory/alloc.h"
#include "defaults.h"
#include "hashtable_page.h"
#include "micro_benchmarks/debug.h"

template <typename PageType, concepts::is_allocator Alloc, bool heterogeneous = false>
class BlockAllocator {
    struct BlockAllocation {
        u64 npages;
        u64 used;
        PageType* ptr; // pointer bumping
    };

  private:
    // In a heterogeneous model, network threads (and not the query thread) can push to free_pages
    std::conditional_t<heterogeneous, tbb::concurrent_queue<PageType*>, std::vector<PageType*>> free_pages;
    std::vector<BlockAllocation> allocations;
    u64 allocations_budget;
    u32 block_sz;

  public:
    explicit BlockAllocator(u32 block_sz, u64 max_allocations = 5)
        : block_sz(block_sz), allocations_budget(max_allocations)
    {
        allocations.reserve(100);
        allocate(false);
    };

    ~BlockAllocator()
    {
        // loop through partitions and deallocate them
        for (auto& allocation : allocations) {
            Alloc::dealloc(allocation.ptr, allocation.npages * sizeof(PageType));
        }
    }

    [[maybe_unused]]
    PageType* allocate(bool consume = true)
    {
        auto block = Alloc::template alloc<PageType>(block_sz * sizeof(PageType));
        allocations.push_back({block_sz, consume ? 1u : 0u, block});
        if (allocations_budget) {
            allocations_budget--;
        }
        return block;
    }

    PageType* get_page()
    requires(not heterogeneous)
    {
        PageType* page;
        auto& current_allocation = allocations.back();
        if (current_allocation.used != current_allocation.npages) {
            // try pointer bumping
            page = current_allocation.ptr + current_allocation.used++;
        }
        else if (not free_pages.empty()) {
            // check for free pages
            page = free_pages.back();
            free_pages.pop_back();
        }
        else if (allocations_budget) {
            // allocate if no free pages
            page = allocate();
        }
        else {
            // throw error (could also wait for cqe?)
            throw std::runtime_error("Exhausted allocation budget");
        }
        page->clear_tuples();
        print("allocated", page);
        return page;
    }

    PageType* get_page()
    requires(heterogeneous)
    {
        auto current_allocation = allocations.back();
        PageType* page;
        if (current_allocation.used != current_allocation.npages) {
            // try pointer bumping
            page = current_allocation.ptr + current_allocation.used++;
        }
        else if (free_pages.try_pop(page)) {
            // check for free pages
        }
        else if (allocations_budget) {
            page = allocate();
        }
        else {
            // throw error (could also wait for cqe?)
            throw std::runtime_error("Exhausted allocation budget");
        }

        // allocate new partition block
        page->clear_tuples();
        return page;
    }

    void add_page(PageType* page)
    requires(not heterogeneous)
    {
        free_pages.push_back(page);
    }

    void add_page(PageType* page)
    requires(heterogeneous)
    {
        free_pages.push(page);
    }
};

template <typename Key, typename Value, void fn_agg(Value&, const Value&), concepts::is_slot Slot = void*,
          concepts::is_allocator Alloc = memory::MMapMemoryAllocator<true>, bool heterogeneous = false,
          bool concurrent = false>
struct PartitionedChainedHashtable {
    using PageAgg = PageAggHashTable<Slot, Key, Value>;
    using ConsumerFn = std::function<void(PageAgg*, bool)>;
    static consteval auto get_empty_slot() -> Slot
    {
        if constexpr (std::is_integral_v<Slot>)
            return -1;
        return 0;
    }
    static constexpr Slot EMPTY_SLOT = get_empty_slot();

  private:
    Slot* ht;
    std::vector<std::conditional_t<concurrent, std::atomic<PageAgg*>, PageAgg*>> partitions;
    std::vector<ConsumerFn> consumer_fns;
    BlockAllocator<PageAgg, Alloc, heterogeneous> block_alloc;
    u64 ht_mask;
    u32 partition_shift;
    u32 npartitions;

  public:
    PartitionedChainedHashtable(u32 _npartitions, u32 _nslots, std::vector<ConsumerFn>& _consumer_fns)
        : npartitions(next_power_2(_npartitions)), partition_shift(__builtin_ctz(next_power_2(_nslots))),
          ht_mask((next_power_2(_npartitions) << __builtin_ctz(next_power_2(_nslots))) - 1),
          block_alloc(next_power_2(_npartitions)), consumer_fns(std::move(_consumer_fns))
    {
        // alloc ht
        ht = Alloc::template alloc<Slot>(sizeof(Slot) * (ht_mask + 1));

        DEBUGGING(print("max tuples per HashTablePreAgg page:", PageAgg::max_tuples_per_page);)
        // alloc partitions
        for (u32 part{0}; part < npartitions; ++part) {
            partitions.push_back(block_alloc.get_page());
        }
    }

    ~PartitionedChainedHashtable()
    {
        //        for (auto p : partitions) {
        //            p->retire();
        //            p->print_page();
        //            for(auto j{0u}; j<p->num_tuples; ++j){
        //                print(p->template get_tuple<0>(j));
        //            }
        //        }
    }

    void aggregate(Key key, Value value) { aggregate(key, value, hash_tuple(key)); }

    void aggregate(Key key, Value value, u64 key_hash)
    requires(not concurrent)
    {
        // extract bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto*& part_page = partitions[part_no];
        Slot& slot = ht[mod];
        Slot next_offset = slot, offset = slot;
        if (part_no == 0) {
            int x = 2;
        }
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
            offset = EMPTY_SLOT;
        }
        slot = part_page->emplace_back_grp(offset, key, value);
    }

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

    void finalize()
    {
        for (u32 part_no{0}; part_no < npartitions; ++part_no) {
            evict<false>(part_no, partitions[part_no], true);
        }
    }

    void aggregate(Key key, Value value, u64 key_hash)
    requires(concurrent)
    {
        // TODO concurrent
        // extract bits from hash
        u64 mod = key_hash & ht_mask;
        u64 part_no = mod >> partition_shift;
        auto* part_page = partitions[part_no];
        Slot& slot = ht[mod];
        Slot next_offset = slot, offset;
    }
};
