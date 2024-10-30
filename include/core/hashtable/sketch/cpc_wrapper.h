#pragma once

#include <DataSketches/cpc_sketch.hpp>
#include <DataSketches/cpc_union.hpp>
#include <mutex>

#include "defaults.h"

namespace ht {

// Wrapper class for Apache's high-performance CPC-sketch implementation
static constexpr u16 log_k = 10;

struct CPCSketch {
    datasketches::cpc_sketch sketch{log_k};

    CPCSketch() = default;

    void update(u64 hash)
    {
        sketch.update(hash);
    }

    [[nodiscard]]
    u64 get_estimate() const
    {
        return sketch.get_estimate();
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "Compressed Probability Counting";
    }
};

struct CPCUnion {
    datasketches::cpc_union union_state{log_k};
    std::mutex merge_mtx{};

    [[nodiscard]]
    u64 get_estimate() const
    {
        return union_state.get_result().get_estimate();
    }

    void merge(const CPCSketch& other)
    {
        union_state.update(other.sketch);
    }

    void merge_concurrent(const CPCSketch& other)
    {
        std::unique_lock _{merge_mtx};
        merge(other);
    }

    [[nodiscard]]
    static std::string get_type()
    {
        return "Compressed Probability Counting";
    }
};

} // namespace ht
