#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <gflags/gflags.h>
#include <numeric>
#include <string>
#include <vector>

#include "cache.h"
#include "core/page.h"
#include "defaults.h"
#include "file.h"
#include "io_manager.h"
#include "utils/utils.h"

DECLARE_bool(random);
DECLARE_uint32(nodes);
DECLARE_string(path);

static std::atomic<int> global_segment_id = 0;

class Table {
    std::vector<Swip> swips;
    File file;

  public:
    int segment_id;

    explicit Table(u32 npages)
    {
        segment_id = global_segment_id.fetch_add(1);
        if (FLAGS_random) {
            prepare_random_swips(npages);
        }
        else {
            // prepare local IO at node offset (adjusted for page boundaries)
            // TODO improve this -> separate into data generator and table
            // file              = File{FLAGS_path, FileMode::READ};
            // auto offset_begin = (((file.get_total_size() / FLAGS_nodes) * node_id) / defaults::local_page_size) * defaults::local_page_size;
            // auto offset_end   = (((file.get_total_size() / FLAGS_nodes) * (node_id + 1)) / defaults::local_page_size) * defaults::local_page_size;
            // if (node_id == FLAGS_nodes - 1) {
            //     offset_end = file.get_total_size();
            // }
            // file.set_offset(offset_begin, offset_end);
            // prepare_file_swips();
            // DEBUGGING(print("reading bytes:", offset_begin, "→", offset_end, (offset_end - offset_begin) / defaults::local_page_size, "pages"));
        }
    };

    ~Table() = default;

    void prepare_file_swips()
    {
        swips.resize((file.get_size() + defaults::local_page_size - 1) / defaults::local_page_size);
        auto first_page = file.get_offset_begin() / defaults::local_page_size;
        std::iota(swips.begin(), swips.end(), first_page);
    }

    void prepare_random_swips(u32 npages)
    {
        swips.resize(npages);
    }

    decltype(auto) get_swips()
    {
        return (swips);
    }

    template <concepts::is_page CachePage>
    void populate_cache(Cache<CachePage>& cache, u32 num_pages_cache, bool sequential_io = true)
    {
        std::vector<std::jthread> threads;
        if (FLAGS_random) {
            // populate cache using 1 thread
            for (u64 idx{0}; idx < num_pages_cache; ++idx) {
                auto& page      = cache.get_page(idx);
                page.num_tuples = CachePage::max_tuples_per_page;
                page.fill_random();
                swips[idx].set_pointer(&page);
            }
        }
        else {
            auto swip_indexes = std::vector<std::size_t>(swips.size());
            std::iota(swip_indexes.begin(), swip_indexes.end(), 0u);
            if (not sequential_io) {
                std::shuffle(swip_indexes.begin(), swip_indexes.end(), librand::rng);
            }

            // populate cache using all available threads
            std::atomic<u32> current_swip;
            for (auto thread{0u}; thread < std::thread::hardware_concurrency(); ++thread) {
                threads.emplace_back([&]() {
                    IO_Manager io{256, false};
                    u32 local_swip, end_swip, batch_sz{100};
                    while ((local_swip = current_swip.fetch_add(batch_sz)) < num_pages_cache) {
                        end_swip = std::min(num_pages_cache, local_swip + batch_sz);
                        for (; local_swip < end_swip; ++local_swip) {
                            assert(swips[swip_indexes[local_swip]].is_page_idx());
                            auto page_offset = swips[swip_indexes[local_swip]].get_page_index() * defaults::local_page_size;
                            io.sync_io<READ>(file.get_file_descriptor(), page_offset, cache.get_page(local_swip));
                            // swizzle pointer
                            swips[swip_indexes[local_swip]].set_pointer(&cache.get_page(local_swip));
                        }
                    }
                });
            }
        }
    }

    template <typename Attribute, typename... Attributes>
    void read_async(IO_Manager& io, std::size_t page_idx, PageLocal<Attribute, Attributes...>* block)
    {
        io.async_io<READ>(segment_id, page_idx * sizeof(PageLocal<Attribute, Attributes...>), as_bytes(block), true);
    }

    template <typename Attribute, typename... Attributes>
    void read_pages_async(IO_Manager& io, uint32_t swips_begin, uint32_t swips_end, std::vector<PageLocal<Attribute, Attributes...>>& batch_blocks)
    {
        io.batch_async_io<READ>(segment_id, std::span{swips.begin() + swips_begin, swips.begin() + swips_end}, batch_blocks);
    }

    auto& get_file()
    {
        return file;
    }
};
