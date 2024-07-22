#pragma once

#include <chrono>

#include "logger.h"
#include "utils.h"

struct Stopwatch {
    Logger& logger;
    std::chrono::time_point<std::chrono::high_resolution_clock> start;

    explicit Stopwatch(Logger& logger) : logger(logger) { start = std::chrono::high_resolution_clock::now(); }

    ~Stopwatch() {
        auto diff =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start);
        logger.log("time (ms)", diff.count());
    }
};