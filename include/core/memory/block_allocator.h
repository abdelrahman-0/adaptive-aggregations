#pragma once

namespace memory {

template <typename PageType, concepts::is_allocator Alloc, bool is_heterogeneous = false>
class BlockAllocator {
    struct BlockAllocation {
        u64 npages;
        u64 used;
        PageType* ptr; // pointer bumping
    };

  private:
    // In a heterogeneous model, network threads (and not the query thread) can push to free_pages
    std::conditional_t<is_heterogeneous, tbb::concurrent_queue<PageType*>, std::vector<PageType*>> free_pages;
    std::vector<BlockAllocation> allocations;
    u64 allocations_budget;
    u32 block_sz;

  public:
    explicit BlockAllocator(u32 block_sz, u64 max_allocations = 10000)
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
        allocations_budget--;
        return block;
    }

    PageType* get_page()
    requires(not is_heterogeneous)
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
            // allocate new block
            page = allocate();
        }
        else {
            // throw error (could also wait for cqe?)
            throw std::runtime_error("Exhausted allocation budget");
        }
        return page;
    }

    PageType* get_page()
    requires(is_heterogeneous)
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
            // allocate new block
            page = allocate();
        }
        else {
            // throw error (could also wait for cqe?)
            throw std::runtime_error("Exhausted allocation budget");
        }
        return page;
    }

    void return_page(PageType* page)
    requires(not is_heterogeneous)
    {
        free_pages.push_back(page);
    }

    void return_page(PageType* page)
    requires(is_heterogeneous)
    {
        free_pages.push(page);
    }
};

} // namespace memory