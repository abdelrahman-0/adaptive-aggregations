#pragma once

#include <gflags/gflags.h>
#include <queue>
#include <tbb/concurrent_queue.h>

#include "alloc.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_alloc.h"
#include "misc/exceptions/exceptions_alloc.h"

namespace mem {

template <typename PageType, concepts::is_mem_allocator Alloc = mem::MMapMemoryAllocator<true>,
          bool has_concurrent_free_pages = false>
class BlockAllocator {
    struct BlockAllocation {
        u64 npages;
        u64 used;
        PageType* ptr; // pointer bumping
    };

  private:
    // In a heterogeneous model, network threads (and not the query thread) can push to free_pages
    std::conditional_t<has_concurrent_free_pages, tbb::concurrent_queue<PageType*>, std::queue<PageType*>> free_pages;
    std::vector<BlockAllocation> allocations;
    u64 allocations_budget;
    u32 block_sz;

    PageType* try_bump_pointer()
    {
        auto& current_allocation = allocations.back();
        if (current_allocation.used != current_allocation.npages) {
            // try pointer bumping
            return current_allocation.ptr + current_allocation.used++;
        }
        return nullptr;
    }

  public:
    BlockAllocator() = delete;

    explicit BlockAllocator(u32 block_sz, u64 max_allocations) : block_sz(block_sz), allocations_budget(max_allocations)
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
        allocations_budget--;
        return block;
    }

    PageType* get_page()
    requires(not has_concurrent_free_pages)
    {
        PageType* page;
        if ((page = try_bump_pointer())) {
            return page;
        }
        else if (not free_pages.empty()) {
            // check for free pages
            page = free_pages.front();
            free_pages.pop();
            return page;
        }
        else if (allocations_budget) {
            // allocate new block
            return allocate();
        }
        throw BlockAllocError{"Exhausted allocation budget"};
    }

    PageType* get_page()
    requires(has_concurrent_free_pages)
    {
        PageType* page;
        if ((page = try_bump_pointer()) or free_pages.try_pop(page)) {
            return page;
        }
        else if (allocations_budget) {
            // allocate new block
            return allocate();
        }
        // throw error (could also wait for cqe?)
        throw BlockAllocError{"Exhausted allocation budget"};
    }

    void return_page(PageType* page) { free_pages.push(page); }
};

} // namespace mem
