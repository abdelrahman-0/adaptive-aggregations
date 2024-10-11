#pragma once

#include <atomic>
#include <vector>

#include "core/page.h"
#include "defaults.h"
#include "utils/spinlock.h"

template <uint64_t initial_ring_size>
struct PointerRing {
    uint64_t ring_size = next_power_2(initial_ring_size);
    uint64_t ring_mask = ring_size - 1;
    uint64_t head{0};
    std::atomic<uint64_t> tail{0};
    std::vector<std::atomic<uint64_t>> thread_pages{};
    std::vector<uintptr_t> ring{};

    explicit PointerRing(std::size_t num_threads) : thread_pages(num_threads), ring(ring_size) {
        for (auto& i : thread_pages) {
            i = 0;
        }
    }

    void insert(uintptr_t page) { ring[tail.fetch_add(1) & ring_mask] = page; }

    auto get_next() {
        while (head == tail)
            ;
        return ring[head++ & ring_mask];
    }
};
