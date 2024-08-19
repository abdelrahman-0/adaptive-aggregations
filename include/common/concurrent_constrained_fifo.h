#pragma once

#include <atomic>
#include <vector>

#include "page.h"
#include "utils/custom_concepts.h"

// FIFO queue implemented as circular buffer. Allows multiple writers and one reader
class alignas(64) ConcurrentFIFO {

  private:
    // TODO: handle false sharing (grouped inserts?)
    std::vector<void*> buffer{};
    std::atomic<uint64_t> tail{0};
    uint64_t head{0};
    uint64_t fifo_size{0};
    uint64_t mask{0};

  public:
    explicit ConcurrentFIFO(uint64_t size = 10)
        : fifo_size(next_power_of_2(size)), mask(fifo_size - 1), buffer(next_power_of_2(size), nullptr) {}

    ~ConcurrentFIFO() = default;

    ConcurrentFIFO(ConcurrentFIFO&& other) noexcept {
        buffer = std::move(other.buffer);
        tail = other.tail.load();
        head = other.head;
        fifo_size = other.fifo_size;
        mask = other.mask;
    }

    void insert(custom_concepts::is_pointer_type auto ptr) {
        uint64_t prev_tail;
        do {
            while ((prev_tail = tail) >= head + fifo_size)
                ;
            // try compare_exchange_weak as well
        } while (!tail.compare_exchange_strong(prev_tail, prev_tail + 1));
        buffer[prev_tail & mask] = ptr;
    }

    template <typename T>
    T* get(std::atomic<bool>& exit_cond) {
        while (!buffer[head & mask])
            ;
        void* result = nullptr;
        std::swap(buffer[head++ & mask], result);
        return reinterpret_cast<T*>(result);
    }
};
