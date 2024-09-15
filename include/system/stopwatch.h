#pragma once

#include <chrono>

#include "utils/logger.h"
#include "utils/utils.h"

struct Stopwatch {
    std::chrono::time_point<std::chrono::high_resolution_clock> begin;
    uint64_t time_ms{0};

    Stopwatch() = default;

    ~Stopwatch() = default;

    void start() { begin = std::chrono::high_resolution_clock::now(); }
    void stop() {
        time_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - begin)
                .count();
        time_ms = std::max(time_ms, 1ul);
    }
};