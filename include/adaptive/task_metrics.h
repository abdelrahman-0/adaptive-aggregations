#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <oneapi/tbb/concurrent_vector.h>

#include "core/sketch/hll_custom.h"
#include "defaults.h"
#include "misc/concepts_traits/concepts_hashtable.h"

namespace adapt {

using sketch_t = ht::HLLSketch<true>;

struct GroupCardinalityHistory {
    static constexpr u64 history_depth             = 16;
    static constexpr double numerical_error_thresh = 1e-5;
    sketch_t combined_sketch{};
    tbb::concurrent_vector<u64> group_history;
    tbb::concurrent_vector<u64> page_num_history;
    u64 total_pages;
    std::atomic<u64> tail{0};

    explicit GroupCardinalityHistory(u64 _total_page) : total_pages(_total_page)
    {
        group_history.resize(history_depth);
        page_num_history.resize(history_depth);
    }

    void update_sketch(const sketch_t& sketch)
    {
        combined_sketch.merge_concurrent(sketch);
    }

    void checkpoint(u64 page_num)
    {
        group_history[tail & (history_depth - 1)]    = combined_sketch.get_estimate();
        page_num_history[tail & (history_depth - 1)] = page_num;
        tail++;
    }

    [[nodiscard]]
    auto estimate_linear_regression_coefficients() const
    {
        double mean_groups = 0.0, mean_pages = 0.0;
        u16 total_seen = std::min(history_depth, tail.load());
        for (u16 i{0}; i < total_seen; ++i) {
            mean_groups += group_history[i];
            mean_pages  += std::log(page_num_history[i]);
        }
        mean_groups      /= total_seen;
        mean_pages       /= total_seen;
        double numerator = 0.0, denominator = 0.0;
        for (u16 i{0}; i < total_seen; ++i) {
            double deviation  = std::log(page_num_history[i]) - mean_pages;
            numerator        += deviation * (group_history[i] - mean_groups);
            denominator      += deviation * deviation;
        }
        if (denominator < numerical_error_thresh) {
            // guard against numerical precision errors
            return std::make_tuple(mean_groups, 0.0);
        }
        double beta_0, beta_1;
        beta_1 = numerator / denominator;
        beta_0 = mean_groups - beta_1 * mean_pages;
        return std::make_tuple(beta_0, beta_1);
    }

    [[nodiscard]]
    auto estimate_quadratic_regression_coefficients() const
    {
        // this function is not used for now
        // https://math.stackexchange.com/questions/267865/equations-for-quadratic-regression
        double sum_x1         = 0.0;
        double sum_x2         = 0.0;
        double sum_x1_x2      = 0.0;
        double sum_squared_x2 = 0.0;
        double sum_y          = 0.0;
        double sum_y_x1       = 0.0;
        double sum_y_x2       = 0.0;
        for (u16 i{0}; i < history_depth; ++i) {
            const double x  = page_num_history[i];
            const double y  = group_history[i];
            sum_x1         += x;
            sum_x2         += x * x;
            sum_x1_x2      += x * x * x;
            sum_squared_x2 += x * x * x * x;
            sum_y          += y;
            sum_y_x1       += y * x;
            sum_y_x2       += y * x * x;
        }
        double s11    = sum_x2 - (sum_x1 * sum_x1 / history_depth);
        double s12    = sum_x1_x2 - (sum_x1 * sum_x2 / history_depth);
        double s22    = sum_squared_x2 - (sum_x2 * sum_x2 / history_depth);
        double sy1    = sum_y_x1 - (sum_y * sum_x1 / history_depth);
        double sy2    = sum_y_x2 - (sum_y * sum_x2 / history_depth);
        double x1_bar = sum_x1 / history_depth;
        double x2_bar = sum_x2 / history_depth;
        double y_bar  = sum_y / history_depth;
        double beta_2 = (sy1 * s22 - sy2 * s12) / (s22 * s11 - s12 * s12);
        double beta_3 = (sy2 * s11 - sy1 * s12) / (s22 * s11 - s12 * s12);
        double beta_1 = y_bar - beta_2 * x1_bar - beta_3 * x2_bar;
        return std::make_tuple(beta_1, beta_2, beta_3);
    }

    [[nodiscard]]
    u64 estimate_total_groups() const
    {
        auto [beta_0, beta_1] = estimate_linear_regression_coefficients();
        return beta_0 + beta_1 * std::log(total_pages);
    }
};

struct TaskMetrics {
    u64 tuples_total{};
    std::atomic<u64> tuples_produced{0};
    std::chrono::time_point<std::chrono::system_clock> start_time_ns{};
    const u16 node_id;
    const u16 output_tup_sz;
    GroupCardinalityHistory history;

    explicit TaskMetrics(node_t _node_id, u16 _output_tup_sz, u32 _total_pages) : node_id(_node_id), output_tup_sz(_output_tup_sz), history(_total_pages)
    {
    }

    void reset()
    {
        start_time_ns = std::chrono::high_resolution_clock::now();
    }

    [[nodiscard]]
    auto get_elapsed_time_ms() const
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(high_resolution_clock::now() - start_time_ns).count();
    }

    [[nodiscard]]
    u64 get_output_size_B() const
    {
        return output_tup_sz * tuples_produced;
    }

    void merge_sketch(const sketch_t& sketch)
    {
        history.update_sketch(sketch);
    }

    void checkpoint_unique_groups(u64 page_num)
    {
        history.checkpoint(page_num);
    }

    auto estimate_total_groups() const
    {
        return history.estimate_total_groups();
    }
};

} // namespace adapt
