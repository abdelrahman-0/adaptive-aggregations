#pragma once

#include <cstdint>
#include <memory>
#include <tbb/scalable_allocator.h>

#include "allocators/rpmalloc/rpmalloc.h"
#include "common/page.h"

static constexpr std::size_t chunk_size = defaults::num_pages_on_chunk;

template <custom_concepts::is_page PageOnChunk>
class PageChunk {
  private:
    std::array<PageOnChunk, chunk_size> page_array{};

  public:
    PageChunk() = default;
    ~PageChunk() = default;

    PageOnChunk* get_page(std::size_t index) { return page_array.data() + index; }

    bool page_full(std::size_t index) { return page_array[index].full(); }
};

template <custom_concepts::is_page PageOnChunk>
struct PageChunkedList {
    using Chunk = PageChunk<PageOnChunk>;
//        tbb::scalable_allocator<Chunk> chunk_allocator{};
//    std::allocator<Chunk> chunk_allocator{};
    std::vector<Chunk*> chunk_ptrs;
    std::vector<std::size_t> pages_per_chunk{};
    std::size_t current_chunk{0};

    PageChunkedList() {
        add_new_chunk();
        get_new_page();
    }

    PageChunkedList(PageChunkedList& other) {
        chunk_ptrs = std::move(other.chunk_ptrs);
        pages_per_chunk = std::move(other.pages_per_chunk);
        current_chunk = other.current_chunk;
    }

    void add_new_chunk() {
//        auto* ptr = chunk_allocator.allocate(sizeof(Chunk));
        auto* ptr = reinterpret_cast<Chunk*>(rpmalloc(sizeof(Chunk)));
        assert(ptr);
        ptr->get_page(0)->clear_tuples();
        chunk_ptrs.push_back(ptr);
        //        chunk_ptrs.push_back(std::unique_ptr<Chunk>(chunk_allocator.allocate(sizeof(Chunk))));
        pages_per_chunk.push_back(0);
    }

    ~PageChunkedList() {
        for (auto* chunk_ptr : chunk_ptrs) {
            rpfree(chunk_ptr);
//            chunk_allocator.deallocate(chunk_ptr, sizeof(Chunk));
        }
    };

    PageOnChunk* get_current_page() { return chunk_ptrs[current_chunk]->get_page(pages_per_chunk[current_chunk] - 1); }

    PageOnChunk* get_new_page() {
        if (current_chunk_full()) {
            add_new_chunk();
            current_chunk++;
        }
        auto* new_page = chunk_ptrs[current_chunk]->get_page(pages_per_chunk[current_chunk]++);
        new_page->clear_tuples();
        return new_page;
    }

    [[nodiscard]] bool current_page_full() const {
        return chunk_ptrs[current_chunk]->page_full(pages_per_chunk[current_chunk] - 1);
    }

    [[nodiscard]] bool current_chunk_full() const { return pages_per_chunk[current_chunk] == chunk_size; }
};
