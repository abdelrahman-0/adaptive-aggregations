#pragma once

#include <array>
#include <atomic>

#include "page.h"
#include "utils/hash.h"

struct PageFrame {
    std::atomic<uint64_t> page_id;
    std::byte* page_ptr;
    std::atomic<uint16_t> readers;
    std::atomic<uint16_t> io_latch;
};

static constexpr std::size_t cache_size = 1 << 10;
static constexpr std::size_t cache_size_mask = cache_size - 1;

struct Cache {
    std::array<PageFrame, cache_size> frames{};
    std::array<std::byte[page_size], cache_size> pages{};

    Cache() = default;
    ~Cache() = default;

    uint64_t get_page(uint64_t page_id, std::byte*& page_ptr) {
        auto bucket_idx = murmur_hash(page_id) & cache_size_mask;
        while(true) {
            auto current_page_id = frames[bucket_idx].page_id.load();
            if (current_page_id == page_id){ // early exit checks
                // found potential frame
                frames[bucket_idx].readers.fetch_add(1);
                if (frames[bucket_idx].page_id.load() == page_id){
                    // can use page
                    page_ptr = frames[bucket_idx].page_ptr;
                    break;
                }else{
                    // spin for a bit
                    frames[bucket_idx].readers.fetch_add(-1);
                }
            } else if (frames[bucket_idx].readers.load() == 0) { // early exit checks
                uint16_t expected_readers = 0;
                uint16_t expected_io_latch = 0;
                if (frames[bucket_idx].readers.compare_exchange_strong(expected_readers, 1)){
                    if(frames[bucket_idx].io_latch.compare_exchange_strong(expected_io_latch, 1)){
                        if(frames[bucket_idx].page_id.compare_exchange_strong(current_page_id, page_id)){

                        }
                    }
                }else{

                }
            }
        }
        return bucket_idx;
    }

    void done_page(uint64_t bucket_idx) {
        frames[bucket_idx].readers.fetch_add(-1);
    }

};
