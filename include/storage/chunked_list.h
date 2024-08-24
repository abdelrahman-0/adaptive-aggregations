#pragma once

#include <cstdint>
#include <memory>
#include <tbb/scalable_allocator.h>

#include "common/page.h"

static constexpr std::size_t chunk_size = defaults::num_pages_on_chunk;

template <custom_concepts::is_page PageOnChunk>
class PageChunk {
  private:
    std::array<PageOnChunk, chunk_size> page_array{};

  public:
    PageChunk() = default;
    ~PageChunk() = default;

    PageOnChunk* get_page(std::size_t index) { return page_array.begin() + index; }

    bool page_full(std::size_t index) { return page_array[index].full(); }
};

template <custom_concepts::is_page PageOnChunk>
struct PageChunkedList {
    using Chunk = PageChunk<PageOnChunk>;
//    tbb::scalable_allocator<Chunk> chunk_allocator;
        std::allocator<Chunk> chunk_allocator{};
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
        auto* ptr = chunk_allocator.allocate(sizeof(Chunk));
        chunk_ptrs.push_back((ptr));
        pages_per_chunk.push_back(0);
    }

    ~PageChunkedList() = default;

    PageOnChunk* get_current_page() { return chunk_ptrs[current_chunk]->get_page(pages_per_chunk[current_chunk] - 1); }

    PageOnChunk* get_new_page() {
        if (current_chunk_full()) {
            add_new_chunk();
            current_chunk++;
        }
        return chunk_ptrs[current_chunk]->get_page(pages_per_chunk[current_chunk]++);
    }

    [[nodiscard]] bool current_page_full() const {
        return chunk_ptrs[current_chunk]->page_full(pages_per_chunk[current_chunk] - 1);
    }

    [[nodiscard]] bool current_chunk_full() const { return pages_per_chunk[current_chunk] == chunk_size; }
};
