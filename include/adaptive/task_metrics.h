#pragma once

#include <atomic>
#include <chrono>
#include <cmath>

#include "core/sketch/hll_custom.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_hashtable.h"

namespace adapt {

using sketch_t = ht::HLLSketch<true>;

struct TaskMetrics {
    u64 tuples_total{};
    std::atomic<u64> tuples_produced{0};
    std::chrono::time_point<std::chrono::system_clock> start_time_ns{};
    std::vector<sketch_t> sketches;
    const u16 node_id;
    const u16 output_tup_sz;

    explicit TaskMetrics(node_t _node_id, u16 _output_tup_sz, u32 _ngroups) : sketches(_ngroups), node_id(_node_id), output_tup_sz(_output_tup_sz)
    {
    }

    void reset()
    {
        for (auto& sketch : sketches) {
            sketch.clear();
        }
        tuples_produced = 0;
        start_time_ns   = std::chrono::high_resolution_clock::now();
    }

    [[nodiscard]]
    auto get_elapsed_time_ms() const
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(high_resolution_clock::now() - start_time_ns).count();
    }

    [[nodiscard]]
    double estimate_unique_groups(u16 nworkers, double remainder_factor) const
    {
        sketch_t combined_sketch;
        auto [sketches_per_worker, extra_sketches] = std::ldiv(sketches.size(), nworkers);
        bool has_extra_sketch                      = node_id < extra_sketches;
        u16 lower_limit                            = node_id * sketches_per_worker + (has_extra_sketch ? node_id : extra_sketches);
        u16 upper_limit                            = lower_limit + sketches_per_worker + has_extra_sketch;
        for (u16 i{lower_limit}; i < upper_limit; ++i) {
            combined_sketch.merge_concurrent(sketches[i]);
        }
        // TODO exp weighted? check second and first order grad? f' & f''
        return combined_sketch.get_estimate() * remainder_factor;
    }

    [[nodiscard]]
    u64 get_output_size_B() const
    {
        return output_tup_sz * tuples_produced;
    }

    void merge_sketch(u16 part_grp_id, const sketch_t& sketch)
    {
        sketches[part_grp_id].merge_concurrent(sketch);
    }
};

} // namespace adapt
