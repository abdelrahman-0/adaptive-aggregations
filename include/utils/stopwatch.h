#pragma once

#include <chrono>
#include "utils.h"

struct Stopwatch {
    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    explicit Stopwatch() {
        start = std::chrono::high_resolution_clock::now();
    }

    ~Stopwatch() {
        auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start);
        println("time taken:", diff.count(), "ms");
    }
};