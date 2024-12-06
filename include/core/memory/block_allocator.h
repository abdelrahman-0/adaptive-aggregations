#pragma once

#include <gflags/gflags.h>
#include <queue>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>

#include "alloc.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/exceptions/exceptions_alloc.h"

namespace mem {

template <typename object_t, concepts::is_mem_allocator Alloc = mem::JEMALLOCator<true>, bool is_concurrent = false>
class BlockAllocator {
    struct BlockAllocation {
        u64 nobjects;
        std::conditional_t<is_concurrent, std::atomic<u64>, u64> used;
        object_t* ptr; // pointer bumping

        BlockAllocation(u64 _nobjects, u64 _used, object_t* _ptr) : nobjects(_nobjects), used(_used), ptr(_ptr)
        {
        }

        BlockAllocation(const BlockAllocation& other)
        {
            nobjects = other.nobjects;
            if constexpr (is_concurrent) {
                used = other.used.load();
            }
            else {
                used = other.used;
            }
            ptr = other.ptr;
        }
    };

  protected:
    // In a heterogeneous model, network threads (and not the query thread) can push to free_objects
    std::conditional_t<is_concurrent, tbb::concurrent_queue<object_t*>, std::queue<object_t*>> free_objects;
    std::conditional_t<is_concurrent, tbb::concurrent_vector<BlockAllocation>, std::vector<BlockAllocation>> allocations;
    std::conditional_t<is_concurrent, std::atomic<u64>, u64> allocation_budget;
    u32 block_sz;

    object_t* try_bump_pointer()
    {
        auto& current_allocation = allocations.back();
        u64 offset{0};
        if ((offset = current_allocation.used++) < current_allocation.nobjects) {
            return current_allocation.ptr + offset;
        }
        return nullptr;
    }

  public:
    explicit BlockAllocator(u32 block_sz, u64 max_allocations) : block_sz(block_sz), allocation_budget(max_allocations)
    {
        allocations.reserve(100);
        allocate(false);
    };

    ~BlockAllocator()
    {
        // loop through partitions and deallocate them
        // TODO commented out so that we can sum up counts in micro-benchmarks
        //        for (auto& allocation : allocations) {
        //            Alloc::dealloc(allocation.ptr, allocation.nobjects * sizeof(object_t));
        //        }
    }

    [[maybe_unused]]
    object_t* allocate(bool consume = true)
    {
        auto* block = Alloc::template alloc<object_t>(block_sz * sizeof(object_t));
        auto muster = BlockAllocation{block_sz, (consume ? 1u : 0u), block};
        allocations.push_back(muster);
        --allocation_budget;
        return block;
    }

    void return_object(object_t* obj)
    {
        free_objects.push(obj);
    }
};

template <typename object_t, concepts::is_mem_allocator Alloc>
class BlockAllocatorNonConcurrent : public BlockAllocator<object_t, Alloc, false> {
    using base_t = BlockAllocator<object_t, Alloc, false>;
    using base_t::allocate;
    using base_t::allocation_budget;
    using base_t::free_objects;
    using base_t::try_bump_pointer;

  public:
    BlockAllocatorNonConcurrent(u32 block_sz, u64 max_allocations) : base_t(block_sz, max_allocations)
    {
    }

    object_t* get_object()
    {
        object_t* obj;
        if ((obj = try_bump_pointer())) {
            return obj;
        }
        else if (not free_objects.empty()) {
            // check for free objects
            obj = free_objects.front();
            free_objects.pop();
            return obj;
        }
        else if (allocation_budget) {
            // allocate new block
            return allocate();
        }
        throw BlockAllocError{"Exhausted allocation budget"};
    }
};

template <typename object_t, concepts::is_mem_allocator Alloc = MMapAllocator<true>>
class BlockAllocatorConcurrent : public BlockAllocator<object_t, Alloc, true> {
    using base_t = BlockAllocator<object_t, Alloc, true>;
    using base_t::allocate;
    using base_t::allocation_budget;
    using base_t::free_objects;
    using base_t::try_bump_pointer;

    // need version to avoid ABA-problem
    std::atomic<u64> version{0};
    std::atomic<bool> allocation_lock{false};

  public:
    BlockAllocatorConcurrent(u32 block_sz, u64 max_allocations) : base_t(block_sz, max_allocations)
    {
    }

    object_t* get_object()
    {
    retry:;
        const u64 old_version = version.load();
        object_t* obj;
        if (((obj = try_bump_pointer())) or free_objects.try_pop(obj)) {
            return obj;
        }
        if (allocation_budget) {
            // allocate new block
            if (bool expected{false}; allocation_lock.compare_exchange_strong(expected, true)) {
                // verify version
                if (old_version != version.load()) {
                    allocation_lock = false;
                    allocation_lock.notify_all();
                    goto retry;
                }
                ++version;
                auto* result    = allocate();
                allocation_lock = false;
                allocation_lock.notify_all();
                return result;
            }
            allocation_lock.wait(true);
            goto retry;
        }
        throw BlockAllocError{"Exhausted allocation budget"};
    }
};

} // namespace mem
