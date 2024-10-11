#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <vector>

#include "utils/utils.h"

// inspired by: https://moodycamel.com/blog/2014/a-fast-general-purpose-lock-free-queue-for-c++.htm

// Concurrent Multiple-Producer-Single-Consumer queue implemented as a collection of SPSC queues
template <concepts::is_pointer T>
class ConcurrentMPSCQueue {
    struct MiniQueue {
        std::vector<T> buffer;
        std::atomic<uint64_t> head{0};
        std::atomic<uint64_t> tail{0};

        explicit MiniQueue(std::size_t size) : buffer(size) {}

        MiniQueue(const MiniQueue& other) noexcept {
            buffer = std::move(other.buffer);
            head = other.head.load();
            tail = other.tail.load();
        }
    };

  private:
    std::vector<MiniQueue> queues;
    uint64_t size{0};
    uint64_t mask{0};
    uint16_t producers{0};
    uint16_t last_checked{0};

  public:
    explicit ConcurrentMPSCQueue(std::size_t producers, std::size_t size)
        : queues(producers, MiniQueue{next_power_2(size)}), size(next_power_2(size)),
          mask(next_power_2(size) - 1), producers(producers) {}

    void insert(std::size_t idx, T val) {
        auto& miniqueue = queues[idx];
        while (miniqueue.tail - miniqueue.head >= size)
            ;
        miniqueue.buffer[miniqueue.tail++ & mask] = val;
        assert((miniqueue.tail & mask) >= 0 and (miniqueue.tail & mask) < size);
    }

    T get() {
        for (;; last_checked = (last_checked + 1) % producers) {
            auto& miniqueue = queues[last_checked];
            if (miniqueue.head != miniqueue.tail) {
                return miniqueue.buffer[miniqueue.head++ & mask];
            }
            // cheap sleep?
        }
    }
};
