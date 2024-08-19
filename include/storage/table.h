#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <numeric>
#include <string>
#include <vector>

#include "cache.h"
#include "common/page.h"
#include "defaults.h"
#include "file.h"
#include "io_manager.h"
#include "utils/utils.h"

static std::atomic<int> global_segment_id = 0;

class Table {
  private:
    std::vector<Swip> swips;
    File file;

    void prepare_swips() {
        swips.resize((file.get_size() + defaults::local_page_size - 1) / defaults::local_page_size);
        auto first_page = file.get_offset_begin() / defaults::local_page_size;
        std::iota(swips.begin(), swips.end(), first_page);
    }

  public:
    int segment_id;

    explicit Table(File&& file) : file(std::move(file)) { segment_id = global_segment_id.fetch_add(1); }

    ~Table() = default;

    auto& get_swips() {
        prepare_swips();
        return swips;
    }

    template <custom_concepts::is_page CachePage>
    void populate_cache(Cache<CachePage>& cache, IO_Manager& io, std::size_t num_pages_cache, bool randomize) {
        auto swip_indexes = std::vector<std::size_t>(swips.size());
        std::iota(swip_indexes.begin(), swip_indexes.end(), 0u);
        if (randomize) {
            std::shuffle(swip_indexes.begin(), swip_indexes.end(), rng);
        }
        for (auto i = 0u; i < num_pages_cache; ++i) {
            assert(swips[swip_indexes[i]].is_page_idx());
            auto page_offset = swips[swip_indexes[i]].get_page_index() * defaults::local_page_size;
            io.sync_io<READ>(file.get_file_descriptor(), page_offset, cache.get_page(i));
            // swizzle pointer
            swips[swip_indexes[i]].set_pointer(&cache.get_page(i));
        }
    }

    template <typename... Attributes>
    void read_async(IO_Manager& io, std::size_t page_idx, PageLocal<Attributes...>* block) {
        io.async_io<READ>(segment_id, page_idx * sizeof(PageLocal<Attributes...>), as_bytes(block), true);
    }

    template <typename... Attributes>
    void read_pages_async(IO_Manager& io, uint32_t swips_begin, uint32_t swips_end,
                          std::vector<PageLocal<Attributes...>>& batch_blocks) {
        io.batch_async_io<READ>(segment_id, std::span{swips.begin() + swips_begin, swips.begin() + swips_end},
                                batch_blocks);
    }

    auto& get_file() { return file; }
};
