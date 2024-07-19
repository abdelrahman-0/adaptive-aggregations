#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <numeric>
#include <string>
#include <vector>

#include "cache.h"
#include "defaults.h"
#include "file.h"
#include "io_manager.h"
#include "page.h"
#include "utils/utils.h"

static std::atomic<uint64_t> global_segment_id = 0;

class Table {
  private:
    std::vector<Swip> swips;
    File file;

  public:
    uint64_t segment_id;

    explicit Table(File&& file) : file(std::move(file)) {
        segment_id = global_segment_id.fetch_add(1);
        swips.resize((file.get_size_in_bytes() + defaults::local_page_size - 1) / defaults::local_page_size);
        std::iota(swips.begin(), swips.end(), 0ull);
    }

    ~Table() = default;

    auto& get_swips() { return swips; }

    void populate_cache(Cache& cache, IO_Manager& io, std::size_t num_pages_cache, bool randomize) {
        auto swip_indexes = std::vector<std::size_t>(swips.size());
        std::iota(swip_indexes.begin(), swip_indexes.end(), 0u);
        if (randomize) {
            std::shuffle(swip_indexes.begin(), swip_indexes.end(), rng);
        }
        for (auto i = 0u; i < num_pages_cache; ++i) {
            assert(!swips[swip_indexes[i]].is_pointer());
            auto page_offset = swips[swip_indexes[i]].get_page_index() * defaults::local_page_size;
            // synchronous read (async submission followed immediately by waiting)
            io.block_io(file.get_file_descriptor(), page_offset, cache.get_page_ptr(i));
            while (io.has_inflight_requests()) {
                if (io.peek()) {
                    io.seen();
                }
            }
            // swizzle
            swips[swip_indexes[i]].set_pointer(cache.get_page_ptr(i));
        }
    }

    void read_page(IO_Manager& io, std::size_t page_idx, std::byte* block) {
        io.block_io(file.get_file_descriptor(), page_idx * defaults::local_page_size, block);
    }
};
